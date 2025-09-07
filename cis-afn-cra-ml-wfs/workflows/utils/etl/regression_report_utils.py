import pandas as pd
import numpy as np

def add_year_column(regression_results: pd.DataFrame, latest_year, latest_quarter):
    print(f'Latest Quarter: {latest_quarter}')
    print(f'Latest Year: {latest_year}')
    latest_year=int(latest_year)
    latest_quarter=int(latest_quarter)

    # if latest quarter is 4, prediction year will always be latest_year + 1
    regression_results['Year'] = latest_year + 1  # default to latest_year + 1

    if latest_quarter == 3:
        regression_results.loc[regression_results['quarter'] == 'Q4', 'Year'] = latest_year
    elif latest_quarter == 2:
        regression_results.loc[regression_results['quarter'].isin(['Q3', 'Q4']), 'Year'] = latest_year
    elif latest_quarter == 1:
        regression_results.loc[regression_results['quarter'].isin(['Q2', 'Q3', 'Q4']), 'Year'] = latest_year

    return regression_results
            
    
def rename_quarter_cols(regression_results: pd.DataFrame, latest_quarter):
    print(f'Latest Quarter: {latest_quarter}')
    latest_quarter = int(latest_quarter)
    if latest_quarter == 1:
        quarters = ['Q2','Q3','Q4','Q1']
    elif latest_quarter == 2:
        quarters=['Q3','Q4','Q1','Q2']
    elif latest_quarter == 3:
        quarters=['Q4','Q1','Q2','Q3']
    else:
        quarters=['Q1','Q2','Q3','Q4']
        
    regression_results['quarter'] = regression_results['quarter'].replace({0:quarters[0],
                                                                1:quarters[1],
                                                                2:quarters[2],
                                                                3:quarters[3]})
    return regression_results

def add_model_column(regression_results: pd.DataFrame):

    # Assuming your DataFrame is named df
    regression_results['model_type'] = np.where(
        (regression_results['zone'] != 'None') & (regression_results['product_model'] != 'None'),
       'Product Zone Prediction',
        np.where(
            (regression_results['zone'] != 'None') & (regression_results['product_model'] == 'None'),
            'Zone Prediction',
            'US Prediction'
        )
    )
    return regression_results

def append_regression_results(regression_results: pd.DataFrame, historical_results: pd.DataFrame):
   

    # Pivot the regression results dataFrame to be same format as historical report
    regression_pivot_df = regression_results.pivot_table(
        index=['zone', 'product', 'quarter', 'Year'],  # keys to keep
        columns='model_type',       # values to pivot into columns
        values='prediction'         # values to fill in
    ).reset_index()
    
    print(regression_pivot_df.head())

    # add iteration columns
    latest_iteration = historical_results['iteration'].astype(int).max()
    regression_pivot_df['iteration'] = int(latest_iteration + 1)
    
    # reorder columns
    regression_pivot_df = regression_pivot_df[['iteration', 'zone', 'product', 'quarter', 'Year', 'Product Zone Prediction', 'Zone Prediction', 'US Prediction']]
    
    # concatenate historical results with new results
    new_report = pd.concat([historical_results, regression_pivot_df])
    new_report.reset_index(drop=True, inplace=True)

    return new_report