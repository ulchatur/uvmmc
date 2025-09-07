from typing import Any, Dict, Tuple
import pandas as pd
from config.etl_config import dict_of_etl
from utils.etl.utils import create_mapping, filter_columns


def flag_non_recurring(
    input_df: pd.DataFrame,
    non_recurring_df: pd.DataFrame,
    product_subgroup_code_col: str = "Product SubGroup Code",
    duration_code_col: str = "Duration Code",
    nr_subgroups_code_col: str = "NR Sub-Groups: Code",
    non_renewing_value: str = "non-renewing policy",
) -> pd.DataFrame:
    """
    Adds a 'non_recurring_flag' column to the input dataframe.
    The flag is True if the Product SubGroup Code is in the non-recurring list
    or the Duration code matches the non-renewing policy.

    :param input_df: DataFrame containing the product subgroup and duration information.
    :param non_recurring_df: DataFrame containing non-recurring product subgroups.
    :param product_subgroup_code_col: Column name for Product SubGroup Code in input_df.
    :param duration_code_col: Column name for Duration code in input_df.
    :param nr_subgroups_code_col: Column name for NR Sub-Groups in non_recurring_df.
    :param non_renewing_value: The value in the Duration code column that indicates a non-renewing policy.
    :return: A copy of the DataFrame with the new 'non_recurring_flag' column.
    """

    # Extract the set of non-recurring codes
    non_recurring_codes_set = set(non_recurring_df[nr_subgroups_code_col])

    # Create the 'non_recurring_flag' using vectorized operations
    input_df["non_recurring_flag"] = input_df[product_subgroup_code_col].isin(non_recurring_codes_set) | (
        input_df[duration_code_col] == non_renewing_value
    )

    return input_df


def add_prefix_to_column_values(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Adds a prefix 'D_' to all the values in the specified column of the dataframe.

    Args:
        dataframe (pd.DataFrame): The input dataframe.
        column_name (str): The name of the column to modify.

    Returns:
        pd.DataFrame: The modified dataframe with the prefix added to the specified column values.
    """

    # Check if the column exists in the dataframe
    if column_name not in dataframe.columns:
        raise ValueError(f"Column '{column_name}' does not exist in the dataframe.")

    # Add prefix 'D_' to the column values
    dataframe[column_name] = "D_" + dataframe[column_name].astype(str)

    return dataframe


def get_unique_zones_mapping(zones_df: pd.DataFrame, key_column: str, value_column: str) -> dict:
    """
    Get a unique zones mapping dictionary from the given zones DataFrame.

    Args:
        zones_df (pd.DataFrame): Pandas DataFrame containing the zones data.
        key_column (str): Name of the column to be used as the key in the mapping dictionary.
        value_column (str): Name of the column to be used as the value in the mapping dictionary.

    Returns:
        dict: A dictionary that maps each unique zone name to its corresponding level.

    """
    columns_to_filter = [key_column, value_column]
    # Keep the filter_columns and create a mapping dictionary based on the key_column and value_column
    zones_mapping = zones_df.pipe(filter_columns, columns_to_filter).pipe(create_mapping, key_column, value_column)
    return zones_mapping


def combine_zones_mapping(spoke_df: pd.DataFrame, extra_zones_df: pd.DataFrame, zones_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Combines zones mapping from different dataframes into a single dictionary.

    Args:
        spoke_df (pd.DataFrame): The dataframe containing spoke zones data.
        extra_zones_df (pd.DataFrame): The dataframe containing extra zones data.
        zones_df (pd.DataFrame): The dataframe containing zones data.

    Returns:
        Dict[str, Any]: The combined zones mapping dictionary.
    """

    # Get spoke zones mapping
    spoke_zones_mapping = get_unique_zones_mapping(spoke_df, *dict_of_etl["zones_etl_params"]["spoke_df"])

    # Get extra zones mapping
    extra_zones_mapping = get_unique_zones_mapping(extra_zones_df, *dict_of_etl["zones_etl_params"]["extra_zones_df"])

    # Get zones mapping
    zones_mapping = get_unique_zones_mapping(zones_df, *dict_of_etl["zones_etl_params"]["zones_df"])

    # Combine the mappings
    combined_dict = zones_mapping.copy()
    combined_dict.update(spoke_zones_mapping)
    combined_dict.update(extra_zones_mapping)

    return combined_dict


def filter_cmap_df(cmap_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the cmap_df DataFrame based on the Reporting Country Code and selects specific columns.

    Args:
        cmap_df (pd.DataFrame): The input DataFrame to be filtered.

    Returns:
        pd.DataFrame: The filtered DataFrame with selected columns.
    """

    filtered_df = (
        cmap_df
        # Filter by Reporting Country Code "US" and valid Client Company Number
        .pipe(lambda df: df.loc[(df["Reporting Country Code"] == "US") & (df["Client Company Number"].notna())])[
            ["Client Company Number", "Client Industry", "Client Segment"]
        ]
        # Assign "unknown" to Client Industry and Client Segment where Client Company Number is "CN00000000"
        .assign(
            Client_Industry=lambda df: df["Client Industry"].mask(df["Client Company Number"] == "CN00000000", "unknown"),
            Client_Segment=lambda df: df["Client Segment"].mask(df["Client Company Number"] == "CN00000000", "unknown"),
        )
    )

    return filtered_df


def create_cmap_mapping_tables(filtered_df: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Creates industry and segment mapping tables from the filtered DataFrame.

    Args:
        filtered_df (pd.DataFrame): The filtered DataFrame containing the necessary columns.

    Returns:
        Tuple[Dict[str, str], Dict[str, str]]: A tuple of two dictionaries representing the mapping tables.
    """
    filtered_df = filtered_df.dropna(subset=["Client Company Number", "Client Industry", "Client Segment"])
    cn_industry_mapping = dict(zip(filtered_df["Client Company Number"], filtered_df["Client Industry"]))
    cn_market_mapping = dict(zip(filtered_df["Client Company Number"], filtered_df["Client Segment"]))
    return cn_industry_mapping, cn_market_mapping


def test_if_bermuda_exists(df: pd.DataFrame) -> None:
    """
    Test if a row with the tag 'BM Bermuda' exists in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to be tested.

    Raises:
        AssertionError: If a row with the tag 'BM Bermuda' is found in the DataFrame.
    """
    # Check if a row with the tag "BM Bermuda" exists
    if any(df["mi_lookup_level4"] == "BM Bermuda"):
        raise AssertionError("Row with tag 'BM Bermuda' found in the DataFrame")


def pivot_revenue_by_product(df):
    """
    Transform renewal revenue data from long format to wide format by product.
    Only pivots renewal_revenue columns, preserves other numerical columns as-is.

    Parameters:
    df: DataFrame with columns: client, product, renewal_revenue_year_X_QY, and other columns

    Returns:
    DataFrame with product-specific renewal revenue columns and preserved other columns
    """

    # Identify ONLY renewal revenue columns for pivoting
    renewal_revenue_cols = [col for col in df.columns if 'csr_revenue_year_' in col]

    # Identify other numerical columns that should NOT be pivoted
    other_numerical_cols = [col for col in df.columns 
                           if col not in ['client_number', 'product_line_nm'] + renewal_revenue_cols
                           and df[col].dtype in ['int64', 'float64', 'int32', 'float32']]

    # Get unique products
    products = df['product_line_nm'].unique()

    # Start with client as the base and include other numerical columns
    # For other numerical columns, we'll aggregate them by client (sum by default)
    base_cols = ['client_number'] + other_numerical_cols
    if other_numerical_cols:
        result_df = df[base_cols].groupby('client_number').sum().reset_index()
    else:
        result_df = df[['client_number']].drop_duplicates().reset_index(drop=True)

    # For each product, create product-specific renewal revenue columns
    for product in products:
        # Filter data for this product
        product_data = df[df['product_line_nm'] == product][['client_number'] + renewal_revenue_cols]

        # Rename ONLY renewal revenue columns to include product prefix
        rename_dict = {}
        for col in renewal_revenue_cols:
            new_col_name = f"{product}_{col}"
            rename_dict[col] = new_col_name

        product_data = product_data.rename(columns=rename_dict)

        # Merge with result dataframe
        result_df = result_df.merge(product_data, on='client_number', how='left')

    return result_df
 