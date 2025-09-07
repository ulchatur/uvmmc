import pandas as pd

def create_mapping(data_frame, key_column, value_column):
    """
    Given a Pandas DataFrame, create_mapping() creates a dictionary mapping values from the key_column
    to their corresponding values in the value_column.

    Args:
    data_frame: Pandas DataFrame that contains the key_column and value_column.
    key_column: a string representing the name of the column that contains the keys.
    value_column: a string representing the name of the column that contains the values.

    Returns:
    A dictionary that maps each value in key_column to its corresponding value in value_column.

    """
    # Uses the zip function to zip together two Pandas Series, key_column & value_column
    # And turns them into a dictionary.
    # The resulting dictionary maps key values to corresponding values.
    return dict(zip(data_frame[key_column], data_frame[value_column]))


def filter_columns(data_frame: pd.DataFrame, columns_to_keep: list) -> pd.DataFrame:
    """
    This function takes a pandas dataframe and a list of column names
    as input parameters. It returns the input dataframe, but with only
    the specified column names included. The other columns are dropped
    from the dataframe in-place.

    Args:
        data_frame: A pandas dataframe object.
        columns_to_keep: A list of column names to keep.

    Returns:
        Returns the input dataframe with only the columns specified in
        `columns_to_keep`.
    """
    # Create a new list containing the columns to drop from the dataframe
    # The list comprehension iterates over each column in the dataframe
    # and checks if its name is not in the list of columns to keep.
    columns_to_drop = [col for col in data_frame.columns if col not in columns_to_keep]

    # Use the `drop()` method of pandas dataframe to remove the required columns.
    # The `inplace=True` argument indicates that the dataframe itself should be modified.
    data_frame.drop(columns=columns_to_drop, inplace=True)
    return data_frame


def exclude_rows_from_mask(df, mask):
    """
    Exclude rows from a DataFrame based on a given mask.

    Args:
        df (pandas.DataFrame): The DataFrame to modify.
        mask (pandas.Series or array-like): The mask to apply for row exclusion.

    Returns:
        pandas.DataFrame: The modified DataFrame with rows excluded.

    """
    num_dropped_rows = df[mask].shape[0]
    df = df[~mask]
    df = df.reset_index(drop=True)
    print("Number of dropped rows:", num_dropped_rows)
    return df


def ignore_rows_based_on_list_values(df: pd.DataFrame, column: str, value_list: list) -> pd.DataFrame:
    """
    Ignore rows where the values in the specified column match any value in the provided list.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column (str): The column to check against the list of values.
        value_list (list): The list of values to be ignored.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    return df.loc[~df[column].isin(value_list)]


def keep_rows_based_on_list_values(df: pd.DataFrame, column: str, value_list: list) -> pd.DataFrame:
    """
    Keep rows where the values in the specified column match any value in the provided list.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column (str): The column to check against the list of values.
        value_list (list): The list of values to be kept.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    return df.loc[df[column].isin(value_list)]  # Keep rows where the column value IS in the list
