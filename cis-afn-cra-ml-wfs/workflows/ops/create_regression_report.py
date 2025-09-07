import os
import pandas as pd
from utils.etl.regression_report_utils import *
from utils.azure.get_df import get_df
from utils.azure.post_df import post_df

deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()

def create_regression_report(all_regression_results: pd.DataFrame, latest_quarter, latest_year):
    
    # add model column to results
    model_reg_results = add_model_column(regression_results=all_regression_results)
    
    # drop the 'product_model' column
    model_reg_results.drop(columns=['product_model'], inplace=True)
    
    # filter out individual models into own dataframes
    us_results = model_reg_results[model_reg_results['model_type'] == 'US Prediction']
    zone_results = model_reg_results[model_reg_results['model_type'] == 'Zone Prediction']
    zone_product_results = model_reg_results[model_reg_results['model_type'] == 'Product Zone Prediction']

    # group model results so they are not at the client level and at necessary respective level
    us_results_grouped = us_results.groupby('quarter')['prediction'].sum().reset_index()
    zone_results_grouped = zone_results.groupby(['zone', 'quarter'])['prediction'].sum().reset_index()
    zone_product_results_grouped = zone_product_results.groupby(['zone','quarter', 'product_line_nm', 'product_line_nm_original'])[['prediction']].sum().reset_index()
    
    # Add zone/product columns with default values for non zone product models
    zone_results_grouped['product'] = 'All Lines'
    us_results_grouped['zone'] = 'ACS'
    us_results_grouped['product'] = 'All Lines'

    # drop the product column (has 'others') and rename product_line_nm_original to product
    zone_product_results_grouped.drop(columns=['product_line_nm'], inplace =True)
    zone_product_results_grouped.rename(columns={'product_line_nm_original': 'product'}, inplace=True)
    
    # make sure all columns are in same order and concatenate
    us_results_grouped = us_results_grouped[['zone', 'product', 'quarter', 'prediction']]
    us_results_grouped['model_type'] = 'US Prediction'
    
    zone_results_grouped = zone_results_grouped[['zone', 'product', 'quarter', 'prediction']]
    zone_results_grouped['model_type'] = 'Zone Prediction'
    
    zone_product_results_grouped = zone_product_results_grouped[['zone', 'product', 'quarter', 'prediction']]
    zone_product_results_grouped['model_type'] = 'Product Zone Prediction'
    
    us_results_grouped.reset_index(drop=True, inplace=True)
    zone_results_grouped.reset_index(drop=True, inplace=True)
    zone_product_results_grouped.reset_index(drop=True, inplace=True)
    
    all_results_grouped = pd.concat([us_results_grouped, zone_results_grouped, zone_product_results_grouped])

    # rename the quarter columns
    updated_quarters = rename_quarter_cols(regression_results=all_results_grouped, latest_quarter=latest_quarter)
    
    # add prediction year columns
    prediction_years = add_year_column(regression_results=updated_quarters, latest_year=latest_year, latest_quarter=latest_quarter)
    
    # pull historical results report from azure
    historical_results = get_df(
        container_name=f'AFN_ML/{deployment_mode}/cra/regression_reports/latest',
        blob_name='Historical Regression Results Report',
        file_extension='csv'
    )
    
    # update the historical report with new results
    updated_report = append_regression_results(regression_results=prediction_years, historical_results=historical_results)
    
    # post report to azure
    # post_df(container_name=f'data/AFN_ML/{deployment_mode}/cra/regression_reports/latest',
    #         blob_name='Historical Regression Results Report',
    #         file_extension_azure='csv',
    #         dataframe_orient='tight',
    #         dataframe=updated_report)

    return updated_report
    