import pandas as pd

def revenue_dq_check(source_df: pd.DataFrame, modeling_df: pd.DataFrame, model_name: str, latest_year):
    try:
        check_results = []
        source_df_revenue = source_df.groupby('year')['net_revenue'].sum().reset_index()
        source_df_revenue.rename(columns={'net_revenue':'revenue'},inplace=True)
    
        # get renewal revenue columns
        modeling_df_revenue_cols = modeling_df.filter(like='renewal_revenue_year').columns.tolist()
        pattern = r'renewal_revenue_year_(\d+)_Q([1-4])'

        # find all unique renewal revenue years in modeling df
        years = set()
        for col in modeling_df_revenue_cols:
            match = re.match(pattern, col)
            if match:
                year = int(match.group(1))
                years.add(year)
        modeling_df_revenue_dict = {}
        for y in years:
            year_num = latest_year - y
            year_cols = modeling_df.filter(like=f'renewal_revenue_year_{y}').columns.tolist()
            year_revenue = modeling_df[year_cols].sum(axis=1).sum()
            modeling_df_revenue_dict[year_num] = year_revenue

        modeling_df_revenue = pd.DataFrame(modeling_df_revenue_dict)
        modeling_df_revenue.columns = ['year', 'revenue']
    
        # compare dataframes
        merged_df = pd.merge(source_df_revenue, modeling_df_revenue, on='year', suffixes=('_source_df', '_modeling_df'))

        mismatches = merged_df[merged_df['revenue_source_df'] != merged_df['revenue_modeling_df']]

        if mismatches.empty:
            print("Renewal revenues match for all years.")
            check_results.append(True)
        else:
            print("Renewal revenues differ for the following years:")
            print(mismatches)
            check_results.append(False)

        # Need to check pivotted csr numbers for classification
        if model_name == 'Classification':
            modeling_df_revenue_cols = modeling_df.filter(like='csr_revenue_year').columns.tolist()
            pattern = r'csr_revenue_year_(\d+)_Q([1-4])'

            # find all unique renewal revenue years in modeling df
            years = set()
            for col in modeling_df_revenue_cols:
                match = re.match(pattern, col)
                if match:
                    year = int(match.group(1))
                    years.add(year)
            modeling_df_revenue_dict = {}
            for y in years:
                year_num = latest_year - y
                year_cols = modeling_df.filter(like=f'csr_revenue_year_{y}').columns.tolist()
                year_revenue = modeling_df[year_cols].sum(axis=1).sum()
                modeling_df_revenue_dict[year_num] = year_revenue

            modeling_df_revenue = pd.DataFrame(modeling_df_revenue_dict)
            modeling_df_revenue.columns = ['year', 'revenue']
        
            # compare dataframes
            merged_df = pd.merge(source_df_revenue, modeling_df_revenue, on='year', suffixes=('_source_df', '_modeling_df'))

            mismatches = merged_df[merged_df['revenue_source_df'] != merged_df['revenue_modeling_df']]

            if mismatches.empty:
                print("CSR revenues match for all years.")
                check_results.append(True)
            else:
                print("CSR revenues differ for the following years:")
                print(mismatches)
                check_results.append(False)

        if all(check_results):
            return True
        else:
            return False

    except Exception as e:
        print(e)
        raise e
