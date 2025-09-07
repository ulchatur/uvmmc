import logging
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_preceding_quarters(year, quarter, num_quarters):
    """
    Generates a list of preceding year_quarters based on a year and quarter.
    Args:
        year (int): The current year.
        quarter (int): The current quarter (1-4).
        num_quarters (int): Number of preceding quarters to generate.

    Returns:
        list: A list of preceding quarters based on the input parameters.
    """
    preceding_quarters = []
    
    for _ in range(num_quarters):
        # Decrement the quarter
        if quarter == 1:
            quarter = 4
            year += 1  # Move to the next year
        else:
            quarter -= 1
        
        # Append the formatted string to the list
        preceding_quarters.append(f'year_{year}_Q{quarter}')
    
    return preceding_quarters


def prep_trn_df(df, latest_quarter, inference):
    trn_cols = df.columns
    if inference == False:
        preceding_quarters = get_preceding_quarters(2, latest_quarter + 1, 8)
        logger.info(f'Tuning Training Columns {preceding_quarters}')
    else:
        preceding_quarters = get_preceding_quarters(1, latest_quarter + 1, 8)
        logger.info(f'Inference Training Columns {preceding_quarters}')
    trn_cols = [col for col in trn_cols if any(quarter in col for quarter in preceding_quarters)]

    trn_df = df[trn_cols]
    #print("trn_df shape", trn_df.shape)
    
    x = trn_df.dtypes
    df_n = trn_df[x[x != 'object'].index]
    
    correlated_groups = find_correlated_groups(df_n, threshold=0.7)
    
    # Print the results
    corr_cols = []
    #print("Correlated Groups:")
    for group_name, columns in correlated_groups.items():
        #print(f"{group_name}: {columns}")
        corr_cols.extend(columns)
    
    #print(len(np.unique(corr_cols)))
    
    agg_cols_trn = {}
    
    for group_name, columns in correlated_groups.items():
        agg_cols_trn[group_name] = trn_df[list(correlated_groups[group_name])].mean(axis=1)
    
    agg_cols_trn_df = pd.DataFrame(agg_cols_trn)
    agg_cols_trn_df.columns = [x.replace(" ", "_") for x in agg_cols_trn_df.columns]
    
    #agg_cols_trn_df.head()
    
    try:
        trn_df.drop(np.unique(corr_cols), axis=1, inplace=True)
    except:
        print("Drop Keys Not Found in DF")
    
    trn_df = pd.concat([trn_df, agg_cols_trn_df], axis=1)
    trn_df.reset_index(drop=True, inplace=True)
    
    #print(trn_df.shape, len(correlated_groups))
    feature_dict_trn = {}
    for i, val in enumerate(trn_df.columns):
        feature_dict_trn["feature_" + str(i)] = val
    
    trn_df.columns = list(feature_dict_trn.keys())

    return(trn_df, feature_dict_trn, correlated_groups)

def find_correlated_groups(df, threshold=0.8):
    # Calculate the correlation matrix
    corr_matrix = df.corr().abs()
    
    # Create a boolean mask for the upper triangle of the correlation matrix
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)
    
    # Get the pairs of correlated columns
    correlated_pairs = corr_matrix.where(mask).stack().reset_index()
    
    # Filter pairs based on the threshold
    correlated_pairs = correlated_pairs[correlated_pairs[0]> threshold]
    
    # Create a dictionary to hold groups of correlated columns
    groups = {}
    
    for index, row in correlated_pairs.iterrows():
        col1, col2 = row['level_0'], row['level_1']
        
        # Find or create a group for col1
        group_found = False
        for key in groups.keys():
            if col1 in groups[key] or col2 in groups[key]:
                groups[key].add(col1)
                groups[key].add(col2)
                group_found = True
                break
        
        # If no group was found, create a new one
        if not group_found:
            groups[f'Group {len(groups) + 1}'] = {col1, col2}
    
    return groups