import pandas as pd
import numpy as np
def prep_test_df_cls(df, correlated_groups, client_size, product, inference):

    # filter data according to client size (less than 10k ,between 10k and 250k, greater than 250k)
    if client_size == 'large':
        df = df[(df['large_client_test_condition']==True)]
    elif client_size == 'medium':
        df = df[(df['large_client_test_condition']==False) & (df['mid_client_test_condition']==True)]
    else:
        df = df[(df['mid_client_test_condition']==False)]


    tst_clients= df.index.tolist()
    tst_cols = df.columns
    
    # Get target and feature cols
    if inference == False:
        
        target_cols_tst = list(df.filter(like = f'{product}_csr_revenue_year_1').columns)
        tst_cols = [col for col in tst_cols if "year_2" in col or "year_3" in col]

    else:
        target_cols_tst = list(df.filter(like = f'{product}_csr_revenue_year_1').columns)
        tst_cols = [col for col in tst_cols if "year_1" in col or "year_2" in col]


    tst_df = df[tst_cols]
    target_df_tst = (df[target_cols_tst].sum(axis=1).round(0) > 0).astype(int)
    
    print(df.shape)
        

    ## Need to update the feature names of our test set by updating the year values to match the train set ##
    # Step 1: Extract unique year values
    tst_correlated_groups = {}
    unique_years = set()
    for columns in correlated_groups.values():
        for col in columns:
            # Extract the year part from the column name
            year_part = col.split('_Q')[0][-1]  # split on _Q and grab the value right to the left, which is the year value
            unique_years.add(year_part)

    # Step 2: Sort the unique year values
    sorted_years = sorted(int(year) for year in unique_years)

    # Step 3: Replace year values in the columns
    for group_name, columns in correlated_groups.items():
        temp = list(columns)
        for year in sorted_years:
            # Replace 'year_{year}' with 'year_{year - 1}'
            temp = [x.replace(f"year_{year}", f"year_{year - 1}") for x in temp]
    
        tst_correlated_groups[group_name] = {}
        for itm in temp:
            if not tst_correlated_groups[group_name]:
                tst_correlated_groups[group_name] = {itm}
            else:
                tst_correlated_groups[group_name].add(itm)
    
        #print(tst_correlated_groups[group_name])

    corr_cols = []
    #print("Correlated Groups:")
    for group_name, columns in tst_correlated_groups.items():
        #print(f"{group_name}: {columns}")
        corr_cols.extend(columns)
    
    agg_cols_tst = {}
    
    for group_name, columns in tst_correlated_groups.items():
        agg_cols_tst[group_name] = tst_df[list(tst_correlated_groups[group_name])].mean(axis=1)
    
    agg_cols_tst_df = pd.DataFrame(agg_cols_tst)
    agg_cols_tst_df.columns = [x.replace(" ", "_") for x in agg_cols_tst_df.columns]

    #agg_cols_tst_df.head()
    
    try:
        tst_df.drop(np.unique(corr_cols), axis=1, inplace=True)
    except:
        print("Drop Keys Not Found in DF")
    
    tst_df = pd.concat([tst_df, agg_cols_tst_df], axis=1)
    tst_df.reset_index(drop=True, inplace=True)
    
    feature_dict_tst = {}
    for i, val in enumerate(tst_df.columns):
        feature_dict_tst["feature_" + str(i)] = val
    
    tst_df.columns = list(feature_dict_tst.keys())         
    
    return(tst_df, feature_dict_tst, target_df_tst, tst_clients)