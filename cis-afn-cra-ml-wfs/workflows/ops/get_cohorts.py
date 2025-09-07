import pandas as pd
import numpy as np

def get_cohorts(df: pd.DataFrame):
    try:
        (cohorts, df)= create_cohorts(df)
        return (cohorts, df)
    except Exception as e:
        raise e

def create_cohorts(df:pd.DataFrame):
    """
    This function takes in our modeling df created from the ETL steps prior, and generates a list 
    of the zone-product cohorts to be trained.

    Args:
        df (pd.DataFrame): modeling df

    Returns:
        list: list of all of the zone-product cohorts to train
    """
    try:
        df.loc[df['final_mip_desc'] == 'Missing','final_mip_desc'] = np.nan
        df.loc[df.final_mip_desc.isnull(), 'final_mip_desc'] = "Others"
        df['product_line_nm_original'] = df['product_line_nm']
        df['mi_lookup_level4_original'] = df['mi_lookup_level4']
        x = df[['product_line_nm','mi_lookup_level4']].value_counts()
        runs = x[x < 200].reset_index(name = 'count')
        mask = df[['product_line_nm', 'mi_lookup_level4']].apply(tuple, axis=1).isin(runs[['product_line_nm', 'mi_lookup_level4']].apply(tuple, axis=1))
        df.loc[mask, ['product_line_nm']] = ['Other']
        
        cohorts = df[['product_line_nm','mi_lookup_level4']].value_counts().index.tolist()
        return cohorts, df
    except Exception as e:
        raise e