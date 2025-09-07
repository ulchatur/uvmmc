import logging
import pandas as pd
import numpy as np

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


def prep_test_df(df, correlated_groups, latest_quarter, inference):
    tst_cols = df.columns
    if inference == False:
        preceding_quarters = get_preceding_quarters(1, latest_quarter + 1, 8)
        logger.info(f'Tuning Testing Cols: {preceding_quarters}')
    else:
        preceding_quarters = get_preceding_quarters(0, latest_quarter + 1, 8)
        logger.info(f'Inference Testing Cols: {preceding_quarters}')
    tst_cols = [col for col in tst_cols if any(quarter in col for quarter in preceding_quarters)]

    tst_df = df[tst_cols]
    
    ## Update column names by adjusting year values ##
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

    return(tst_df, feature_dict_tst)

