import logging
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def prep_trn_df_cls(df, client_size, product, inference):

    # filter data according to client size (less than 10k ,between 10k and 250k, greater than 250k)
    if client_size == 'large':
        df = df[(df['large_client_train_condition']==True)]
    elif client_size == 'medium':
        df = df[(df['large_client_train_condition']==False) & (df['mid_client_train_condition']==True)]
    else:
        df = df[(df['mid_client_train_condition']==False)]
    

    # Get target cols, which is the CSR for the current product in 2023
    trn_cols = df.columns


    if inference == False:

        target_cols_trn = list(df.filter(like = f'{product}_csr_revenue_year_2').columns)
        target_df_trn = (df[target_cols_trn].sum(axis=1).round(0) > 0).astype(int)
        trn_cols = [col for col in trn_cols if "year_4" in col or "year_3" in col]

    else:

        target_cols_trn = list(df.filter(like = f'{product}_csr_revenue_year_1').columns)
        target_df_trn = (df[target_cols_trn].sum(axis=1).round(0) > 0).astype(int)
        trn_cols = [col for col in trn_cols if "year_2" in col or "year_3" in col]


   
    # Filter training features to be only those from 2021 and 2022
    trn_df = df[trn_cols]
    
    # Find the features in our train df that are correlated and group them together
    x = trn_df.dtypes
    df_n = trn_df[x[x != 'object'].index]
    correlated_groups = find_correlated_groups(df_n, threshold=0.7)
    
    # Print the results
    corr_cols = []
    #print("Correlated Groups:")
    for group_name, columns in correlated_groups.items():
        #print(f"{group_name}: {columns}")
        corr_cols.extend(columns)
    
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

    return(trn_df, feature_dict_trn, correlated_groups, target_df_trn)


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