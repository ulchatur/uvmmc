import re
import pandas as pd
from utils.etl.adjustments_utls import *

def apply_overrides_adjustments(df_ml: pd.DataFrame, df_spoke: pd.DataFrame, df: pd.DataFrame, latest_year, latest_quarter):
    """
    Applies overrides and adjustments to the given DataFrames based on the latest year and quarter.

    Args:
        df_ml (pd.DataFrame): DataFrame containing machine learning data.
        df_spoke (pd.DataFrame): DataFrame containing spoke data.
        df (pd.DataFrame): DataFrame where overrides and adjustments will be applied.
        latest_year: Latest year for adjustments.
        latest_quarter: Latest quarter for adjustments.

    Returns:
        pd.DataFrame: DataFrame for ML containing adjustments features.
    """
    try:
        
        df_spoke['L4. Planning Product: Planning Year'] = df_spoke.apply(lambda row: determine_year(row, max_year=int(latest_year)), axis=1)
        print(df_spoke.shape)
        print(f'Found years: {df_spoke[df_spoke["L4. Planning Product: Planning Year"].isna()]}')
        df_spoke['production_code_spoke'] = df_spoke['Accounts L4: CSR Accounts'].apply(map_production_code)
        df_spoke['recurring_flag'] = df_spoke['Accounts L4: CSR Accounts'].apply(recurring_flag)
        df_spoke['Time'] = pd.to_datetime(df_spoke['Time'], format='%b %y')
        df_spoke['Month'] = df_spoke['Time'].dt.month
        df_spoke['client_number'] = df_spoke['L4. Planning Product: Client Code'].str.replace('-US', '', regex=False)
        
        prod_line_nm = df_ml[['product_line_nm','product_line_cd']].drop_duplicates()
        df_spoke = df_spoke.merge(prod_line_nm, left_on = 'L4. Planning Product: Product Code', right_on = 'product_line_cd', how = 'left')
        
        df_spoke['product_line_nm'].fillna('Product Line Not Available', inplace = True)
        
        df_spoke = df_spoke[['L4. Planning Product: Planning Year','Quarter','client_number','product_line_nm','NR Override']]
        
        latest_year = df_spoke['L4. Planning Product: Planning Year'].max()
        print(f' Lastest year: {latest_year}')
        print(df_spoke['L4. Planning Product: Planning Year'].unique())

        df_spoke['year_index'] = latest_year - df_spoke['L4. Planning Product: Planning Year']
        print(df_spoke[['year_index','Quarter']].drop_duplicates())
        print(df_spoke['Quarter'].unique())
        print(df_spoke['year_index'].unique())

        df_spoke['NR Override'] = df_spoke['NR Override'].astype(float)
        pivot_df = df_spoke.pivot_table(
            index=['client_number', 'product_line_nm'],
            columns=['year_index', 'Quarter'],
            values='NR Override',
            aggfunc='sum'
        )
        print(pivot_df.columns)

        pivot_df.columns = [f'adjustments_year_{int(year)}_Q{int(quarter)}' for year, quarter in pivot_df.columns]

        pivot_df.reset_index(inplace=True)

        df = df.merge(pivot_df, on = ['client_number','product_line_nm'], how = 'left')
        
        # Find all unique year quarter combinations where we have an adjustments column
        pattern = r'adjustments_year_(\d+)_Q(\d)'

        # Set to store unique combinations
        unique_combinations = set()

        # Extract year and quarter
        for item in pivot_df.columns:
            match = re.search(pattern, item)
            if match:
                year = int(match.group(1))
                quarter = int(match.group(2))
                unique_combinations.add((year, quarter))

        # Convert set to a sorted list
        year_quarters = sorted(unique_combinations)
        year_quarters = [val for val in year_quarters if not ((val[0]== 0) and (val[1] > int(latest_quarter) -1))]
       
        for y, q in year_quarters:
            df[f'non_recurring_revenue_year_{y}_Q{q}'] = df[f'non_recurring_revenue_year_{y}_Q{q}'] + df[f'adjustments_year_{y}_Q{q}']

        return df
    except Exception as e:
        raise e
    