import re
import pandas as pd
import numpy as np
from utils.etl.cat_features_extract import extract_categorical_variables, relabel_except
from utils.etl.etl_functions import pivot_revenue_by_product
from utils.etl.num_feature_data_prep import prepare_revenue_data
from utils.etl.num_feature_definitions import feature_definitions
from utils.azure.post_df import post_df
import warnings
warnings.filterwarnings("ignore")

def build_ml_data_cls(df_original: pd.DataFrame, latest_year, latest_quarter):
    """
    Builds machine learning features.
        
    Args:
        df_original (pd.DataFrame): The dataframe used to build features.
        latest_year (int): The latest year found in df_original.
        latest_quarter (int): The latest full quarter found in df_original.

    Returns:
        pd.DataFrame: machine learning data with features.
    """
    try:
        csr_24 = df_original[(df_original['year']==2024) & (df_original['production_code'].isin(['Renewal','New business','Expanded services']))]['net_revenue'].sum()
        print(f'CSR 24: {csr_24}')
        csr_25_q1 = df_original[(df_original['year']==2025) & (df_original['production_code'].isin(['Renewal','New business','Expanded services'])) & (df_original['month'].isin([1,2,3]))]['net_revenue'].sum()
        print(f'CSR 25 Q1: {csr_25_q1}')

        quarters = [[1,2,3],[4,5,6],[7,8,9],[10,11,12]]
        available_years = df_original['year'].unique().tolist()
        last_year = int(latest_year)

        years = [val for val in available_years if val <= last_year]
        print(f'Found years: {years}')


        pivot_df_list = []
        all_quarters= [[1,2,3],[4,5,6],[7,8,9],[10,11,12]]
        
        for recurring in [True, False]:
            dfs = []
            df_modeling = df_original.copy()
            df_modeling.loc[~df_modeling['product_line_nm'].isin(['Casualty','FINPRO','Property','Surety']),'product_line_nm'] = 'Others'
            df_modeling_filt = df_modeling[df_modeling['non_recurring_flag'] == recurring]
            for year in years:
                if year == int(latest_year):
                    quarters=all_quarters[:int(latest_quarter)]
                else:
                    quarters=all_quarters
                for quarter in quarters:

                    print(f"Year: {year}, Quarter:{quarter}")

                    df = df_modeling_filt[(df_modeling_filt['year'] == year) & (df_modeling_filt['production_code'].isin(['Renewal','New business','Expanded services'])) & (df_modeling_filt['month'].isin(quarter))]

                    df['client_number'].fillna('missing_client', inplace=True)

                    df[['bill_effective_dt', 'invoice_date', 'cv_effective_dt', 'cv_expiration_dt','non_recurring_flag']] = df[['bill_effective_dt', 'invoice_date', 'cv_effective_dt', 'cv_expiration_dt','non_recurring_flag']].fillna('Missing') 

                    df['quarter'] = ((df['month'] - 1) // 3) + 1
                    
                    q= df['quarter'].iloc[0]

                    base_year = None  # or set to a specific year like base_year = 2023
        
                    # Prepare the revenue data

                    numerical_df = prepare_revenue_data(
                        df=df,
                        feature_definitions=feature_definitions,
                        base_year=base_year
                    )

                    # Access DataFrames for different aggregation levels

                    # For example, product-level DataFrame
                    product_level_df = numerical_df.get('product_line_nm', pd.DataFrame())

                    # Client-Level DataFrame
                    client_level_df = numerical_df.get('client_number', pd.DataFrame())
        

                    # Client-Product-Level DataFrame
                    client_product_level_df = numerical_df.get('client_number_product_line_nm', pd.DataFrame())
        
                    # Client-Product-Level DataFrame
                    company_number_level_df = numerical_df.get('company_number', pd.DataFrame())
        

                    # Merge DataFrames as needed

                    # merging client-level and client-product-level DataFrames
                    if not client_product_level_df.empty and not client_level_df.empty:

                        merged_df = pd.merge(
                            client_product_level_df,
                            client_level_df,
                            on='client_number',
                            how='left',
                            suffixes=('_client_product', '_client')
                        )
        

                    # merge product-level features
                    if not merged_df.empty and not product_level_df.empty:

                        final_num_df = pd.merge(
                            merged_df,
                            product_level_df,
                            on='product_line_nm',
                            how='left',
                            suffixes=('', '_product')
                        )
        
                    # merge company nummber level DataFrames

                    # first, need to merge company number level df with our original df to get client number. drop client number duplicates in order to do merge
                    if not final_num_df.empty and not company_number_level_df.empty:

                        filtered_df = df[['company_number', 'client_number']].drop_duplicates(subset='client_number')
                        company_client_df = pd.merge(
                            company_number_level_df,
                            filtered_df,
                            on='company_number',
                            how='left'
                        )

                        final_df = pd.merge(
                            final_num_df,
                            company_client_df,
                            on='client_number',
                            how='left',
                            suffixes=('', '_company')
                        )
        
                    if year == int(latest_year):
                        y = 0
                
                    else:
                        y = int(latest_year) - year
        
                    final_df.rename(columns=lambda x: x.replace('year_0', f'year_{y}'), inplace=True)
        
                    dfs.append(final_df)


            base_df = pd.concat(dfs)
            base_df['company_number'] = 'Missing'
            
            result_df = base_df.groupby(['product_line_nm', 'client_number'], 
                                as_index=False).agg('first')
            
            result_df.fillna(0, inplace = True)
            result_df.reset_index(drop=True, inplace=True)
            # create CSR columns
            year_quarters = set()
            pattern = r'renewal_revenue_year_(\d+)_Q([1-4])'
            rev_columns = list(result_df.filter(like='renewal_revenue_year').columns)
            for col in rev_columns:
                match = re.match(pattern, col)
                if match:
                    year = int(match.group(1))
                    quarter = match.group(2)
                    year_quarters.add((year, quarter))

            # Convert set to sorted list if needed
            for y,q in year_quarters:
                result_df[f'csr_revenue_year_{y}_Q{q}'] = result_df[f'renewal_revenue_year_{y}_Q{q}'] + result_df[f'expanded_services_revenue_year_{y}_Q{q}'] + result_df[f'new_business_revenue_year_{y}_Q{q}']


            # pivot classification date
            pivot_df = pivot_revenue_by_product(result_df)
            if recurring:
                pivot_df['Recurring'] = False
            else:
                pivot_df['Recurring'] = True
    
            pivot_df_list.append(pivot_df)

        full_cls_df = pd.concat(pivot_df_list, ignore_index=True)
        full_cls_df.reset_index(drop=True, inplace=True)

        return full_cls_df

    except Exception as e:
        dagster_logger.info(e)
        raise e
    
    