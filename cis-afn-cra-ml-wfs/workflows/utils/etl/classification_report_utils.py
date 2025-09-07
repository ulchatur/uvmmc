import pandas as pd
import numpy as np

def extract_reports(collected_results: list):
    prediction_dfs = [res['prediction_df'] for res in collected_results]
    feature_imp_dfs = [res['feature_imp_df'] for res in collected_results]
    feature_percentile_dfs = [res['feature_percentile_df'] for res in collected_results]

    prediction_df = pd.concat(prediction_dfs, ignore_index=True)
    print.info(f'Prediction DF Shape: {prediction_df.shape}')
    feature_imp_df = pd.concat(feature_imp_dfs, ignore_index=True)
    print.info(f'Feature Importance DF Shape: {feature_imp_df.shape}')
    feature_percentile_df = pd.concat(feature_percentile_dfs, ignore_index=True)
    
    return prediction_df, feature_imp_df, feature_percentile_df


def add_client_names(prediction_df: pd.DataFrame, source_df: pd.DataFrame):
    
    clients= source_df[['client_number', 'client_nm']]
    clients = clients.drop_duplicates()
    clients.drop_duplicates(subset=['client_number'],inplace=True)
    prediction_w_clients = pd.merge(prediction_df, clients, on=['client_number'],how='left')
    return prediction_w_clients

def add_py_renewal_month(prediction_df: pd.DataFrame, source_df: pd.DataFrame, latest_year):

    # prediction_year = int(latest_year) + 1
    # latest_year = 2024

    source_df = source_df[source_df['production_code']!= 'Unknown']
    source_df.loc[~source_df['product_line_nm'].isin(['FINPRO', 'Surety', 'Casualty', 'Property']),'product_line_nm'] = 'Others'
    df = source_df[['client_number','product_line_nm','non_recurring_flag','year','month','net_revenue']]

    df_py_year = df[df['year'] == int(latest_year)]
    grouped = df_py_year.groupby(['client_number', 'product_line_nm', 'non_recurring_flag', 'year', 'month'])['net_revenue'].sum().reset_index()

    # Step 2: Filter out months where total net_revenue is zero or negative
    positive_revenue_months = grouped[grouped['net_revenue'] > 0]

    # Step 3: For each group, find the earliest month with positive revenue
    first_transactions_py = (
        positive_revenue_months
        .groupby(['client_number', 'product_line_nm', 'non_recurring_flag'])
        ['month']
        .min()
        .reset_index()
    )
    first_transactions_py.rename({'month':'py_renewal_month', 'non_recurring_flag':'recurring'}, axis=1, inplace=True)
    first_transactions_py['recurring'] = ~first_transactions_py['recurring']


    # Step 1: Group by all relevant columns and sum net_revenue
    source_df_cy = source_df[source_df['production_code']== 'Renewal']
    df = source_df_cy[['client_number','product_line_nm','non_recurring_flag','year','month','net_revenue']]
    df_pred_year = df[df['year'] ==int(latest_year) + 1]
    grouped = df_pred_year.groupby(['client_number', 'product_line_nm', 'non_recurring_flag', 'year', 'month'])['net_revenue'].sum().reset_index()

    # Step 2: Filter out months where total net_revenue is zero or negative
    positive_revenue_months = grouped[grouped['net_revenue'] > 0]

    # Step 3: For each group, find the earliest month with positive revenue
    first_transactions_pred_year = (
        positive_revenue_months
        .groupby(['client_number', 'product_line_nm', 'non_recurring_flag'])
        ['month']
        .min()
        .reset_index()
    )

    # Step 4: Rename columns as needed
    first_transactions_pred_year.rename({'month':'cy_renewal_month', 'non_recurring_flag':'recurring'}, axis=1, inplace=True)
    first_transactions_pred_year['recurring'] = ~first_transactions_pred_year['recurring']

    # prediction_df.rename(columns={'Recurring': 'recurring'}, inplace=True)
    report_w_months = prediction_df.merge(first_transactions_py,on=['client_number', 'product_line_nm', 'recurring'], how = 'left').merge(first_transactions_pred_year, on=['client_number', 'product_line_nm', 'recurring'],how = 'left')

    #updated cy month column
    updated_report = update_months(source_df=source_df, initial_report=report_w_months, year=int(latest_year) + 1)
    updated_report_final = update_months(source_df=source_df, initial_report=updated_report, year=int(latest_year))

    return updated_report_final

def update_cy_renewal_months(source_df: pd.DataFrame, prediction_df: pd.DataFrame, prediction_year: int):
    # drop the cy_renewal_column from prediction report as we are going to rebuild it
    prediction_df.drop(columns=['cy_renewal_month'], inplace=True)

    source_df.loc[~source_df['product_line_nm'].isin(['FINPRO', 'Surety', 'Casualty', 'Property']),'product_line_nm'] = 'Others'
    source_df_cy = source_df[source_df['production_code']== 'Renewal']
    df = source_df_cy[['client_number','product_line_nm','non_recurring_flag','year','month','net_revenue']]
    df_pred_year = df[df['year'] == int(prediction_year)]
    grouped = df_pred_year.groupby(['client_number', 'product_line_nm', 'non_recurring_flag', 'year', 'month'])['net_revenue'].sum().reset_index()

    # Step 2: Filter out months where total net_revenue is zero or negative
    positive_revenue_months = grouped[grouped['net_revenue'] > 0]

    # Step 3: For each group, find the earliest month with positive revenue
    first_transactions_pred_year = (
        positive_revenue_months
        .groupby(['client_number', 'product_line_nm', 'non_recurring_flag'])
        ['month']
        .min()
        .reset_index()
    )

    # Step 4: Rename columns as needed
    first_transactions_pred_year.rename({'month':'cy_renewal_month'}, axis=1, inplace=True)
    first_transactions_pred_year['recurring'] = ~first_transactions_pred_year['non_recurring_flag']
    first_transactions_pred_year.drop(columns=['non_recurring_flag'], inplace=True)

    prediction_df['recurring'] = prediction_df['recurring'].astype(bool)
    first_transactions_pred_year['recurring'] = first_transactions_pred_year['recurring'].astype(bool)
    updated_report = pd.merge(prediction_df,first_transactions_pred_year, on=['client_number', 'product_line_nm', 'recurring'],how = 'left')
    return updated_report

def update_months(source_df: pd.DataFrame, initial_report: pd.DataFrame, year: int, latest_year):
    print(f'Updating Months for Year: {year}')
    print(f'Latest year: {latest_year}')
    print(f' Updating column {'cy_renewal_month' if year==(latest_year + 1) else 'py_renewal_month'}')

    if year == latest_year + 1:
        source_df = source_df[source_df['production_code']=='Renewal']

    # Step 1: Calculate total revenue for year
    total_revenue = source_df[source_df['year'] == year].groupby(['client_number', 'product_line_nm', 'non_recurring_flag'])['net_revenue'].sum().reset_index()

    # Step 2: Identify pairs with total revenue <= 0
    zero_or_negative_revenue_pairs = total_revenue[total_revenue['net_revenue'] <= 0][['client_number', 'product_line_nm', 'non_recurring_flag']]
    zero_or_negative_revenue_pairs['recurring'] = ~zero_or_negative_revenue_pairs['non_recurring_flag']

    # Step 3: Merge into report
    report_w_months_updated = initial_report.merge(
        zero_or_negative_revenue_pairs.assign(zero_or_neg=1),
        on=['client_number', 'product_line_nm', 'recurring'],
        how='left'
    )

    # Step 4: Override renewal month for these pairs
    if year == latest_year + 1:
        report_w_months_updated.loc[report_w_months_updated['zero_or_neg'] == 1, 'cy_renewal_month'] = np.nan
    else:
        report_w_months_updated.loc[report_w_months_updated['zero_or_neg'] == 1, 'py_renewal_month'] = np.nan

    # Optional: Drop helper column
    report_w_months_updated.drop(columns=['zero_or_neg', 'non_recurring_flag'], inplace=True)

    return report_w_months_updated

def add_csr_numbers(source_data: pd.DataFrame, results_df: pd.DataFrame):
    
    renewal_columns = source_data.filter(like ='csr_revenue_year').columns.tolist()
    renewal_columns.append('client_number')
    renewal_columns.append('Recurring')
    print(f'Renewal Columns: {renewal_columns}')

    df_cat = source_data[renewal_columns]

    revenue_cols = [col for col in df_cat.columns if 'csr_revenue_year' in col]
    other_cols = [col for col in df_cat.columns if col not in revenue_cols]

    df_melted = df_cat.melt(id_vars = other_cols, value_vars = revenue_cols, var_name = 'revenue_column', value_name = 'revenue_value')

    df_melted['year_num'] = df_melted['revenue_column'].str.extract(r'year_(\d+)').astype(int)

    df_melted['product_line_nm'] = df_melted['revenue_column'].str.extract(r'^(.*?)_csr_revenue_year_\d+_Q\d+')

    df_melted['year'] = 2025 - df_melted['year_num']
    df_melted.drop(['revenue_column','year_num'], axis = 1, inplace = True)

    df_pivoted = df_melted.pivot_table(index = ['client_number','product_line_nm','Recurring'],
                                    columns = 'year',
                                    values = 'revenue_value',
                                    aggfunc='sum').reset_index()
    df_pivoted.columns.name = None
    df_pivoted.columns = [f'revenue_{col}' if col not in ['client_number', 'product_line_nm','Recurring'] else col for col in df_pivoted.columns]
    print(df_pivoted.head())
    csr_numbers = pd.merge(results_df, df_pivoted, how='left', on=['client_number', 'product_line_nm','Recurring'])
    print(csr_numbers.head())
    return csr_numbers


def get_revenue_numbers(source_df: pd.DataFrame, latest_year, latest_quarter):

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
    return final_df
