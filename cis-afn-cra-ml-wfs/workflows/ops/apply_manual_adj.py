import pandas as pd
from utils.etl.adjustments_utls import *

def apply_manual_adjustments(df_ml: pd.DataFrame, df_spoke: pd.DataFrame):
    """
    Applies manual adjustments to the dataframes df_ml and df_spoke.

    Args:
        df_ml (pd.DataFrame): The main dataframe containing the hub-based ML data.
        df_spoke (pd.DataFrame): The spoke dataframe.

    Returns:
        grouped_df_ml (pd.DataFrame): DataFrame to be used for building features and eventual regression training.
        max_hub_year (int): Maximum year found in Hub df
        latest_full_quarter (int): Maximum full quarter found in Hub df
    """
    try:
        csr_24 = df_ml[(df_ml['year']==2024) & (df_ml['production_code'].isin(['Renewal', 'New business', 'Expanded services']))]['net_revenue'].sum()
        csr_25 = df_ml[(df_ml['year']==2024) & (df_ml['month'].isin([1,2,3,4,5,6])) & (df_ml['production_code'].isin(['Renewal', 'New business', 'Expanded services']))]['net_revenue'].sum()
        print(f'CSR 24 DF ML: {csr_24}')
        print(f'CSR 25 DF ML: {csr_25}')
        
        max_hub_year = df_ml['year'].max()
        latest_full_quarter = get_last_quarter(df=df_ml,max_year=max_hub_year)
        print(f'Latest Year: {max_hub_year}, Latest Full Quarter: Q{latest_full_quarter}')
       
        df_ml['Recurring_Type'] = df_ml['non_recurring_flag'].apply(lambda x: 'Non Recurring' if x else 'Recurring')
        df_ml['quarter'] = df_ml['month'].apply(get_quarter)

        #### FILL MISSING VALUES ########
        df_ml['client_number']=df_ml['client_number'].fillna('missing_client')
        df_ml['entry_mode'] = df_ml['entry_mode'].fillna('Missing')
        df_ml['basis_code'] = df_ml['basis_code'].fillna('Missing')
        df_ml['accrual_type_us'] = df_ml['accrual_type_us'].fillna('Missing')
        df_ml['invoice_date'] = df_ml['invoice_date'].fillna('Missing')
        df_ml['bill_effective_dt'] = df_ml['bill_effective_dt'].fillna('Missing')
        df_ml['cv_effective_dt'] = df_ml['cv_effective_dt'].fillna('Missing')
        df_ml['cv_expiration_dt'] = df_ml['cv_expiration_dt'].fillna('Missing')
        df_ml['final_mip_desc'] = df_ml['final_mip_desc'].fillna('Missing')
        df_ml['market_segment'] = df_ml['market_segment'].fillna('Missing')
        
        grouped_df_ml = df_ml.groupby(['year','month','client_number','product_line_cd',
                   'product_line_nm','production_code','fcs_department_nr',
                   'revenue_id','duration_cd','company_number','product_subgroup_nm','billing_type','entry_mode','basis_code','accrual_type_us',
                   'invoice_date','bill_effective_dt','cv_effective_dt','cv_expiration_dt','non_recurring_flag','mi_lookup_level4','final_mip_desc',
                   'market_segment'])[['net_revenue']].sum().reset_index()
        
        grouped_df_ml['Manual Adjustments'] = 0
        grouped_df_ml['Restatements'] = 0
        
        grouped_df_ml.reset_index(drop=True, inplace=True)
        csr_24 = grouped_df_ml[(grouped_df_ml['year']==2024) & (grouped_df_ml['production_code'].isin(['Renewal', 'New business', 'Expanded services']))]['net_revenue'].sum()
        csr_25 = grouped_df_ml[(grouped_df_ml['year']==2024) & (grouped_df_ml['month'].isin([1,2,3,4,5,6])) & (grouped_df_ml['production_code'].isin(['Renewal', 'New business', 'Expanded services']))]['net_revenue'].sum()

        print(f'CSR 24 DF ML: {csr_24}')
        print(f'CSR 25 DF ML: {csr_25}')
        
        return (grouped_df_ml, max_hub_year, latest_full_quarter)
    except Exception as e:
        raise e