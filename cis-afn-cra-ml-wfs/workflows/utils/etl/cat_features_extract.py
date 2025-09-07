from typing import List
import pandas as pd

def extract_categorical_variables(df: pd.DataFrame, categorical_vars: List[str], grouping_columns: List[str]) -> pd.DataFrame:
    """
    Extracts the most frequent value for each categorical variable per group.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        categorical_vars (List[str]): List of categorical variables to extract.
        grouping_columns (List[str]): Columns to group by.

    Returns:
        pd.DataFrame: DataFrame with grouping columns and extracted categorical variables.
    """
    # Initialize DataFrame with grouping columns
    result_df = df[grouping_columns].drop_duplicates().reset_index(drop=True)

    for var in categorical_vars:
        # Compute the mode of the categorical variable per group
        mode_df = (
            df.groupby(grouping_columns)[var].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None).reset_index()
        )
        # Merge the mode_df into result_df
        result_df = result_df.merge(mode_df, on=grouping_columns, how="left")

    return result_df

def relabel_except(df: pd.DataFrame, column: str, categories_to_keep: list, new_label: str) -> pd.DataFrame:
    """
    Relabel all categories except for the specified ones under a broader category (e.g., 'Other').
    Parameters:
        df (pandas.DataFrame): The input DataFrame containing the column to be relabeled.
        column (str): The column name that contains the categories to be relabeled.
        categories_to_keep (list): A list of categories that should be kept unchanged.
        new_label (str): The label that will replace all categories except for those specified.
    Returns:
        pd.DataFrame: The DataFrame with the categories relabeled except for the specified ones.
    """
    # Replace categories not in the list to keep with the new label
    df[column] = df[column].apply(lambda x: x if x in categories_to_keep else new_label)
    
    return df
