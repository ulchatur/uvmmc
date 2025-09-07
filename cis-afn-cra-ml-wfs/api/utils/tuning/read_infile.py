import logging
import pandas as pd
import json
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#  Find most recent year (should always be 0) and quarter (0-3) within data
def get_last_year_quarter(df):
    
    # Initialize variables to track the most recent year and quarter
    most_recent_year = 10
    most_recent_quarter = -1

    # Iterate through the column names

    pattern = '.*_year_([0-9]\d*)_Q([1-4])$'
    # Filter the DataFrame to include only the matching columns
    no_adjust = [col for col in df.columns if 'adjustments_year_' not in col and col.count('year_') < 2]
    df_no_adjust = df[no_adjust]
    year_cols = df_no_adjust.filter(regex=pattern).columns
    for column in list(year_cols):
        # Split the column name to extract year and quarter
        parts = column.split('year_')
        year_quarter = parts[1]
        year = int(year_quarter[0])  # Extract the year part
        quarter = int(year_quarter[-1])  # Extract the quarter part (remove 'Q' and convert to int)

        # Check if this year is more recent
        if year < most_recent_year:
            most_recent_year = year
            most_recent_quarter = quarter  # Reset quarter to the current one
        elif year == most_recent_year:
            # If the year is the same, check for the greatest quarter
            if quarter > most_recent_quarter:
                most_recent_quarter = quarter

    # Output the most recent year and quarter
    return (most_recent_year,most_recent_quarter)

  
def get_preceding_quarters(year, quarter, num_quarters):
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


def read_infile(df, zone, product, qtr, inference):

    
    df.loc[df['final_mip_desc'] == 'Missing','final_mip_desc'] = np.nan
    df.loc[df.final_mip_desc.isnull(), 'final_mip_desc'] = "Others"

    df_keys = df.loc[:, ['client_number', 'product_line_nm', 'product_line_nm_original']]
    
    if zone == 'None' and product == 'None':
        df_cat = df.loc[:, ['final_mip_desc', 'market_segment', 'mi_lookup_level4', 'product_line_nm']]
    elif zone != 'None' and product == 'None':
        df_cat = df.loc[:, ['final_mip_desc', 'market_segment', 'product_line_nm']]
    else:
        df_cat = df.loc[:, ['final_mip_desc', 'market_segment']]

    #Remove columns with 0 variance and their correspoinding year/qtr columns
    x = df.dtypes
    x = x[x != 'object'].index

    temp = df[x]
    y = temp.apply(lambda x: len(np.unique(x)) == 1)

    y = y[y == True].index

    y = [y for y in y if "year_0" not in y]

    y = ["_".join(y.split("_")[:-2]) for y in y]

    latest_year, latest_quarter = get_last_year_quarter(df)

    # find the 7 quarters prior to our latest quarter
    preceding_quarters = get_preceding_quarters(latest_year, latest_quarter, 7)
    logger.info(f'Preceeding Quarters: {preceding_quarters}')
    logger.info(f'Latest Quarter: {latest_quarter}')
    # define target columns for training
    if inference == False:
        target_cols_trn = [f'renewal_revenue_' + year_quarter for year_quarter in preceding_quarters[-4:]][::-1]
        logger.info(f'Tuning Target Cols Train: {target_cols_trn}')
    else:
        target_cols_trn = [f'renewal_revenue_' + year_quarter for year_quarter in preceding_quarters[:3]][::-1] +  [f'renewal_revenue_year_{latest_year}_Q{latest_quarter}'] 
        logger.info(f'Inference Target Cols Train: {target_cols_trn}')

    # define target columns for testing
    target_cols_tst = [f'renewal_revenue_' + year_quarter for year_quarter in preceding_quarters[:3]][::-1] +  [f'renewal_revenue_year_{latest_year}_Q{latest_quarter}'] 
    logger.info(f'Target Cols Test: {target_cols_tst}')
    
    cols_ignore = target_cols_trn + target_cols_tst
    c_cols = []
    for col in x:
        if col in cols_ignore:
            c_cols.append(col)
        elif sum([itm in col for itm in y]) > 0:
            continue
        else:
            c_cols.append(col)


    df = df.loc[:, c_cols]
    df.reset_index(drop=True, inplace=True)

    ##########################################
    ## Get the target and Create Target Vars
    ##########################################

    target_df_trn = df[target_cols_trn[qtr]]
    target_df_tst = df[target_cols_tst[qtr]]

    return(df, df_keys, df_cat, target_df_trn, target_df_tst, latest_quarter)
        