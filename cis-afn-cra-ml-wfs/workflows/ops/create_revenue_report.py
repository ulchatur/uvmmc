import os
import pandas as pd
from utils.azure.post_df import post_df

deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()

def create_revenue_report(source_df: pd.DataFrame, latest_year, latest_quarter, model_name: str, classification_report: pd.DataFrame = pd.DataFrame()):
    try:
        if model_name =='Regression':
            revenue_report = regression_revenue_report(source_df=source_df, latest_year=latest_year, latest_quarter=latest_quarter)
        else:
            revenue_report = classification_revenue_report(source_df=source_df, classification_report=classification_report, latest_year=latest_year, latest_quarter=latest_quarter)
        
        return revenue_report
    except Exception as e:
        dagster_logger.info(e)
        raise e


def regression_revenue_report(source_df, latest_year, latest_quarter):
     
    # define quarter column
    source_df['quarter'] = ((source_df['month'] - 1) // 3) + 1
    source_df['quarter'] = source_df['quarter'].astype(int)
    
    # Group df by zone, product, year, quarter
    grouped = source_df.groupby(['mi_lookup_level4', 'product_line_nm', 'year', 'quarter'])

    # Calculate csr_revenue as sum of net_revenue for renewal, new business, and expanded services records
    csr_revenue = grouped.apply(lambda x: x.loc[x['production_code'].isin(['Renewal', 'New business', 'Expanded services']), 'net_revenue'].sum())

    # Calculate renewal_revenue as sum of net_revenue where production_code == 'Renewal'
    renewal_revenue = grouped.apply(lambda x: x.loc[x['production_code'] == 'Renewal', 'net_revenue'].sum())

    # Combine into a new DataFrame
    result = pd.DataFrame({
        'zone': csr_revenue.index.get_level_values('mi_lookup_level4'),
        'product': csr_revenue.index.get_level_values('product_line_nm'),
        'year': csr_revenue.index.get_level_values('year'),
        'quarter': csr_revenue.index.get_level_values('quarter'),
        'csr_revenue': csr_revenue.values,
        'renewal_revenue': renewal_revenue.values
    })

    # filter df to not have any quarters later than the latest full quarter/year
    result = result[~((result['year']==int(latest_year)) & (result['quarter'] > int(latest_quarter)))]

    # Reset index
    result.reset_index(drop=True, inplace=True)
    return result
    
def classification_revenue_report(source_df, classification_report, latest_year, latest_quarter):
    # add quarter column
    source_df['quarter'] = ((source_df['month'] - 1) // 3) + 1
    source_df['quarter'] = source_df['quarter'].astype(int)

    # drop any rows with future months
    source_df = source_df[~((source_df['year']==int(latest_year)) & (source_df['quarter'] > int(latest_quarter)))]

    # fill in missing client numbers
    source_df['client_number'].fillna('missing_client', inplace=True)

    # define product line == Others
    source_df.loc[~source_df['product_line_nm'].isin(['FINPRO', 'Surety', 'Casualty', 'Property']),'product_line_nm'] = 'Others'

    # Create recurring column
    source_df['recurring'] = (~source_df['non_recurring_flag'])

    # Group df by zone, product, recurring, and year
    grouped = source_df.groupby(['client_number', 'product_line_nm', 'recurring', 'year'])

    # Calculate csr_revenue as sum of net_revenue for renewal, new business, and expanded services records
    csr_revenue = grouped.apply(lambda x: x.loc[x['production_code'].isin(['Renewal', 'New business', 'Expanded services']), 'net_revenue'].sum())

    # Calculate renewal_revenue as sum of net_revenue where production_code == 'Renewal'
    renewal_revenue = grouped.apply(lambda x: x.loc[x['production_code'] == 'Renewal', 'net_revenue'].sum())

    # Combine into a new DataFrame
    result = pd.DataFrame({
        'client_number': csr_revenue.index.get_level_values('client_number'),
        'product_line_nm': csr_revenue.index.get_level_values('product_line_nm'),
        'recurring': csr_revenue.index.get_level_values('recurring'),
        'revenue_year': csr_revenue.index.get_level_values('year'),
        'csr_revenue': csr_revenue.values,
        'renewal_revenue': renewal_revenue.values
    })

    # Reset index
    result.reset_index(drop=True, inplace=True)

    ## Make sure that all years are present for all combos ##

    # Step 1: Get all unique combinations of client_number, product_line_nm, recurring, and year
    unique_combinations = source_df[['client_number', 'product_line_nm', 'recurring', 'year']].drop_duplicates()

    # Step 2: Get all unique years in the source data
    all_years = source_df['year'].drop_duplicates()

    # Step 3: Create a Cartesian product of unique combinations with all years
    # First, create a DataFrame of all years repeated for each combination
    combinations_expanded = unique_combinations.drop(columns='year').drop_duplicates()
    years_expanded = pd.DataFrame({'year': all_years})

    # Cross join to get all combinations of client, product, recurring, and year
    full_combinations = combinations_expanded.assign(key=1).merge(years_expanded.assign(key=1), on='key').drop('key', axis=1)

    # Step 4: Merge the full combinations with your aggregated results
    # Rename 'revenue_year' in your result to 'year' for consistency
    result.rename(columns={'revenue_year': 'year'}, inplace=True)

    # Merge to ensure all combinations are present
    final_df = pd.merge(full_combinations, result, on=['client_number', 'product_line_nm', 'recurring', 'year'], how='left')

    # Optional: Fill NaNs with zeros if desired
    final_df['csr_revenue'] = final_df['csr_revenue'].fillna(0)
    final_df['renewal_revenue'] = final_df['renewal_revenue'].fillna(0)

    # Now, final_df contains a row for every client-product-recurring-year combination
    final_df.reset_index(drop=True, inplace=True)
    
    # add client sizes
    classification_report['client_number'].replace({'Missing':'missing_client'}, inplace=True)
    classification_report = classification_report[['client_number','recurring', 'client_size']]
    classification_report.drop_duplicates(inplace=True)
    
    report_w_client_size = pd.merge(final_df, classification_report, on=['client_number', 'recurring'], how='left')
    return report_w_client_size