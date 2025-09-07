import os
import pandas as pd
from utils.azure.get_df import get_df
from utils.etl.utils import keep_rows_based_on_list_values
from config.etl_config import dict_of_etl
from utils.etl.etl_functions import filter_cmap_df, create_cmap_mapping_tables, flag_non_recurring, add_prefix_to_column_values
from utils.general.get_config import get_config
from dotenv import load_dotenv

azure_config = get_config('azure_config.json')
load_dotenv(dotenv_path ='/Workspace/Shared/.env')
deployment_mode =os.environ.get('DEPLOYMENT_MODE').lower()

def initial_etl(df: pd.DataFrame):

    """
    Performs an initial ETL (Extract, Transform, Load) process on the hub data.
    Args:
        df (pd.DataFrame): The input DataFrame on which the ETL process will be performed.

    Returns:
        pd.DataFrame: The post-ETL hub data
    """
    try:
    
        #Get non recurring:
        nr_df = get_df(container_name=azure_config['mappings']['container'].replace('ENV_PLACEHOLDER', deployment_mode),
                        blob_name=azure_config['mappings']['non-recurring']['blob_name'],
                        file_extension=azure_config['mappings']['non-recurring']['file_extension'])
        print('Got NR DF')

        # Read CMAP
        cmap_df = get_df(container_name=azure_config['mappings']['container'].replace('ENV_PLACEHOLDER', deployment_mode),
                            blob_name=azure_config['mappings']['client_mapping']['blob_name'],
                            file_extension=azure_config['mappings']['client_mapping']['file_extension'])
        print('Got cmap df')

        # New Zones table
        zones_full = get_df(container_name=azure_config['mappings']['container'].replace('ENV_PLACEHOLDER', deployment_mode),
                            blob_name=azure_config['mappings']['zones']['blob_name'],
                            file_extension=azure_config['mappings']['zones']['file_extension'])
        print('got zones full')
        
        # Process Zone Data
        zones_full['ZONES'] = zones_full["ZONES"].str.replace('OneMMC_', '')
        zones_dictionary = zones_full.set_index('RC_DEPT')['ZONES'].to_dict()
        
        # Extract industry and market mapping table
        cn_industry_mapping, cn_market_mapping = cmap_df.pipe(filter_cmap_df) \
                                                    .pipe(create_cmap_mapping_tables)
                                                    
        # Main Pipeline
        df_cleaned = df.pipe(flag_non_recurring, nr_df)\
        .pipe(add_prefix_to_column_values, 'Local Revenue Department')\
        .assign(mi_lookup_level4=lambda x: x["RC_DEPT"].map(zones_dictionary))\
        .pipe(keep_rows_based_on_list_values, 'mi_lookup_level4', dict_of_etl['zonesana'])\
        .assign(final_mip_desc=lambda x: x["Company Number"].map(cn_industry_mapping))\
        .assign(market_segment=lambda x: x["Company Number"].map(cn_market_mapping))\
        .pipe(lambda x: x.rename(columns=str.lower))\
        .pipe(lambda x: x.rename(columns=dict_of_etl["col_rename_dict"]))
        print(f'Found years: {df_cleaned["year"].unique()}')

        return df_cleaned
    except Exception as e:
        raise e

