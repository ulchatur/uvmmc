# prepare_data.py
from typing import Dict, List
import pandas as pd
from utils.etl.num_feature_definitions import FeatureDefinition


def prepare_revenue_data(
    df: pd.DataFrame, feature_definitions: List[FeatureDefinition], base_year: int = None
) -> Dict[str, pd.DataFrame]:
    """
    Processes revenue data and returns a dictionary of DataFrames keyed by aggregation level.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.
        feature_definitions (List[FeatureDefinition]): List of feature definitions.
        base_year (int, optional): The base year for dynamic column naming (e.g., year_0).
            If None, uses the maximum year in the data.

    Returns:
        Dict[str, pd.DataFrame]: A dictionary where keys are aggregation level names and
            values are DataFrames with aggregated features in a wide format.
    """
    # Copy DataFrame to avoid modifying the original
    df = df.copy()

    # Initial preprocessing
    # Convert 'year' and 'month' to a datetime 'date' column
    df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    # Extract 'quarter' and 'year' from the 'date' column
    df["quarter"] = df["date"].dt.quarter
    df["year"] = df["date"].dt.year

    # Determine the base year for dynamic naming
    if base_year is None:
        base_year = df["year"].max()

    # Calculate 'year_offset' so that current year is 'year_0', prior year is 'year_1', etc.
    df["year_offset"] = base_year - df["year"]

    # Organize features by their aggregation levels
    features_by_level = {}
    for feature in feature_definitions:
        for level in feature.aggregation_levels:
            level_key = tuple(level)
            features_by_level.setdefault(level_key, []).append(feature)

    # Process features for each aggregation level
    result_dfs = {}
    for level_key, features in features_by_level.items():
        # Process features at this level using the helper function
        pivot_df = process_features_at_level(df, features, list(level_key))
        # Create a name for the aggregation level
        level_name = "_".join(level_key) if level_key else "overall"
        result_dfs[level_name] = pivot_df

    return result_dfs


def process_features_at_level(df: pd.DataFrame, features: List[FeatureDefinition], level: List[str]) -> pd.DataFrame:
    """
    Processes features at a specific aggregation level.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        features (List[FeatureDefinition]): List of features to process.
        level (List[str]): List of columns to group by.

    Returns:
        pd.DataFrame: Aggregated and pivoted DataFrame for the specified level.
    """
    groupby_cols = level + ["year_offset", "quarter"] if level else ["year_offset", "quarter"]
    df_level = df.copy()

    # Apply feature computation functions relevant to this level
    for feature in features:
        df_level = feature.compute_function(df_level)

    # Aggregate features
    agg_dict = {feature.name: feature.aggfunc for feature in features}
    agg_df = df_level.groupby(groupby_cols).agg(agg_dict).reset_index()

    # Pivot the DataFrame to wide format
    pivot_df = agg_df.pivot_table(
        index=level if level else None, columns=["year_offset", "quarter"], values=list(agg_dict.keys()), fill_value=0
    )

    # If there's an index, reset it
    if pivot_df.index.names != [None]:
        pivot_df.reset_index(inplace=True)

    # Flatten MultiIndex columns
    pivot_df.columns = flatten_columns(pivot_df.columns, level if level else [])

    return pivot_df


def flatten_columns(columns: pd.Index, pivot_index: List[str]) -> List[str]:
    """
    Flattens MultiIndex columns after pivoting, constructing dynamic column names.

    Parameters:
        columns (pd.Index): MultiIndex columns from the pivoted DataFrame.
        pivot_index (List[str]): List of columns that are part of the index.

    Returns:
        List[str]: A list of flattened column names.
    """
    flattened_cols = []
    for col in columns:
        if isinstance(col, tuple):
            if col[0] in pivot_index:
                # Index columns remain as is
                flattened_cols.append(col[0])
            else:
                # Unpack the MultiIndex levels
                feature: str = col[0]
                year_offset: int = col[1]
                quarter: int = col[2]
                # Construct the dynamic column name
                col_name = f"{feature}_year_{year_offset}_Q{quarter}"
                flattened_cols.append(col_name)
        else:
            flattened_cols.append(col)
    return flattened_cols
