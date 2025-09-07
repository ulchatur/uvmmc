import pandas as pd
import numpy as np
from utils.etl.cat_features_extract import extract_categorical_variables, relabel_except
from utils.etl.num_feature_data_prep import prepare_revenue_data
from utils.etl.num_feature_definitions import feature_definitions
import warnings
warnings.filterwarnings("ignore")

def build_ml_data(df_original: pd.DataFrame, latest_year, latest_quarter):
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
        zones = df_original['mi_lookup_level4'].dropna().unique()
        quarters = [[1,2,3],[4,5,6],[7,8,9],[10,11,12]]
        available_years = df_original['year'].unique().tolist()
        last_year = int(latest_year)

        years = [val for val in available_years if val <= last_year]
        print(f'Found years: {years}')

        dfs = []
        all_quarters= [[1,2,3],[4,5,6],[7,8,9],[10,11,12]]

        for zone in zones:
        
            for year in years:
                if year == int(latest_year):
                    quarters=all_quarters[:int(latest_quarter)]
                else:
                    quarters=all_quarters
                for quarter in quarters:

                    print(f"Zone:{zone}, Year: {year}, Quarter:{quarter}")

                    df = df_original[(df_original['year'] == year) & (df_original['production_code'].isin(['Renewal','New business','Expanded services'])) & (df_original['mi_lookup_level4'] == zone) & (df_original['month'].isin(quarter))]
                    # df['client_number'] = df['client_numberO'].str.replace('-US', '', regex=False)

                    df['client_number'].fillna('missing_client', inplace=True)

                    df[['bill_effective_dt', 'invoice_date', 'cv_effective_dt', 'cv_expiration_dt','non_recurring_flag']] = df[['bill_effective_dt', 'invoice_date', 'cv_effective_dt', 'cv_expiration_dt','non_recurring_flag']].fillna('Missing')

                    categorical_vars = ['mi_lookup_level4', 'final_mip_desc', 'market_segment']

                    grouping_columns = ['client_number']     

                    df[categorical_vars] = df[categorical_vars].fillna('Missing')            

                    categorical_df = df.pipe(extract_categorical_variables, categorical_vars, grouping_columns).pipe(relabel_except, 'market_segment', ["Corporate", "Risk Management"], 'Other')

                    df["non_recurring_flag"] = df["non_recurring_flag"].replace({'True': True, 'False': False})

                    df["non_recurring_flag"] = df["non_recurring_flag"].astype(bool)  # Convert to boolean type

                    df['quarter'] = ((df['month'] - 1) // 3) + 1

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

                        final_num_df_company = pd.merge(
                            final_num_df,
                            company_client_df,
                            on='client_number',
                            how='left',
                            suffixes=('', '_company')
                        )
        
                    # merge client categorical features
                    if not final_num_df_company.empty and not categorical_df.empty:

                        final_df = pd.merge(
                            final_num_df_company,
                            categorical_df,
                            on='client_number',
                            how='left',
                            suffixes=('', '_client')
                        )
        
                    if year == int(latest_year):
                        y = 0
                
                    else:
                        y = int(latest_year) - year
        
                    final_df.rename(columns=lambda x: x.replace('year_0', f'year_{y}'), inplace=True)
        
                    dfs.append(final_df)


        base_df = pd.concat(dfs)
        base_df['company_number'] = 'Missing'
        
        result_df = base_df.groupby(['company_number', 'product_line_nm', 'final_mip_desc',
                            'client_number', 'market_segment', 'mi_lookup_level4'], 
                            as_index=False).agg('first')
        
        result_df.fillna(0, inplace = True)


        return result_df

    except Exception as e:
        dagster_logger.info(e)
        raise e