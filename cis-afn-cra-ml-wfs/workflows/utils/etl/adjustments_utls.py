import pandas as pd
import numpy as np

    
def map_production_code(value):
    if 'Expanded' in value:
        return 'Expanded services'
    elif 'New Client' in value:
        return 'New business'
    elif 'Renewal' in value:
        return 'Renewal'
    else:
        return 'Unkown'


def map_recurring(value):
    if 'Non Recurring' in value:
        return 'Non Recurring'
    elif 'Recurring' in value:
        return 'Recurring'
    else:
        return None

def recurring_flag(status):
    if "Recurring" in status and "Non" not in status:
        return "Recurring"
    elif "Non Recurring" in status:
        return "Non Recurring"
    else:
        return None

def get_quarter(month):
    return (month - 1) // 3 + 1


def determine_year(row, max_year):
    account_contains_py = 'PY' in row['Accounts L4: CSR Accounts']
    
    if row['L4. Planning Product: Planning Year'] == f'FY{str(max_year)[2:]}' and not account_contains_py:
        return max_year
    elif account_contains_py:
        return 2000 + int(row['L4. Planning Product: Planning Year'][2:]) - 1
    else:
        return None

    
def get_last_quarter(df,max_year):
    available_months = df[df['year'] == max_year]['month'].tolist()
    # print(available_months.unique())
    quarters = {
        1: [1, 2, 3],
        2: [4, 5, 6],
        3: [7, 8, 9],
        4: [10, 11, 12]
    }

    latest_full_quarter = 0

    for quarter, months in quarters.items():
        if all(month in available_months for month in months):
            latest_full_quarter = quarter

    return latest_full_quarter

def fill_specific_missing_values(df, columns):
    for column in columns:
        if column in df.columns:  
            df[column] = df[column].fillna('Missing')
    return df

def create_missing_columns(df, columns_to_create):
    for column in columns_to_create:
        df[column] = 'Missing'
    return df

def filter_columns(df, columns):
    return df[columns]

def merge_with_product_line(df, prod_line_nm):
    return df.merge(prod_line_nm, on='product_line_cd', how='left')

renames = {'L4. Planning Product: Planning Year':'year',
'Month':'month',
'L4. Planning Product: Client Code':'client_numberO',
'L4. Planning Product: Department Code':'fcs_department_nr',
'L4. Planning Product: Product Code':'product_line_cd',
'production_code_spoke':'production_code'}

columns = ['year','month','client_numberO','fcs_department_nr','product_line_cd','production_code',
    'revenue_id', 'duration_cd', 'company_number', 'product_subgroup_nm',
    'billing_type', 'entry_mode', 'basis_code', 'accrual_type_us',
    'invoice_date', 'bill_effective_dt', 'cv_effective_dt', 
    'cv_expiration_dt', 'non_recurring_flag','net_revenue','Manual Adjustments','Restatements']

columns_to_fill = [
    'entry_mode', 
    'basis_code', 
    'accrual_type_us', 
    'invoice_date', 
    'bill_effective_dt', 
    'cv_effective_dt', 
    'cv_expiration_dt', 
    'mi_lookup_level4', 
    'final_mip_desc', 
    'market_segment'
]

columns_to_create = [
    'revenue_id', 'duration_cd', 'company_number', 'product_subgroup_nm',
    'billing_type', 'entry_mode', 'basis_code', 'accrual_type_us',
    'invoice_date', 'bill_effective_dt', 'cv_effective_dt', 
    'cv_expiration_dt', 'non_recurring_flag'
]