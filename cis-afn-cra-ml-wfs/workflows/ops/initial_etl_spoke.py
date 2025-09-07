import os
import pandas as pd
from utils.azure.get_df import get_df
from utils.etl.utils import keep_rows_based_on_list_values
from config.etl_config import dict_of_etl
from utils.general.get_config import get_config
from utils.anaplan.download_anaplan_data import download_anaplan_data
from config.anaplan_column_lists import spoke_col_list
from utils.anaplan.anaplan_connection import get_conn

azure_config = get_config('azure_config.json')
deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()

spoke_workspace_id = os.environ['SPOKE_WORKSPACE_ID']
spoke_model_id = os.environ['SPOKE_MODEL_ID']

def etl_spoke():
    """
    Extracts the spoke data files from Azure and creates a Quarter field and Zone field.

    Returns:
        pd.DataFrame: DataFrame containing all years of spoke data concatenated.
    """
    try:
        # generate connection to anaplan
        print("making connection to anaplan...")
        conn = get_conn(workspace_id=spoke_workspace_id, model_id=spoke_model_id)
        print('made connection to anaplan')

        spoke_year_3 = get_df(container_name=azure_config['raw_data']['spoke']['container'].replace("ENV_PLACEHOLDER", deployment_mode),
                            blob_name=azure_config['raw_data']['spoke']['year_3']['blob_name'],
                            file_extension=azure_config['raw_data']['spoke']['year_3']['file_extension'],
                            delimiter=azure_config['raw_data']['spoke']['year_3']['delimiter'])
        print('Got spoke_year_3 df')
        spoke_year_2 = get_df(container_name=azure_config['raw_data']['spoke']['container'].replace("ENV_PLACEHOLDER", deployment_mode),
                            blob_name=azure_config['raw_data']['spoke']['year_2']['blob_name'],
                            file_extension=azure_config['raw_data']['spoke']['year_2']['file_extension'],
                            delimiter=azure_config['raw_data']['spoke']['year_2']['delimiter'])
        print('Got spoke_year_2 df')
        spoke_year_1 = get_df(container_name=azure_config['raw_data']['spoke']['container'].replace("ENV_PLACEHOLDER", deployment_mode),
                            blob_name=azure_config['raw_data']['spoke']['year_1']['blob_name'],
                            file_extension=azure_config['raw_data']['spoke']['year_1']['file_extension'],
                            delimiter=azure_config['raw_data']['spoke']['year_1']['delimiter'])
        print('Got spoke_year_1 df')
        # spoke_year_0 = get_df(container_name=azure_config['raw_data']['spoke']['container'].replace("ENV_PLACEHOLDER", deployment_mode),
        #                     blob_name=azure_config['raw_data']['spoke']['year_0']['blob_name'],
        #                     file_extension=azure_config['raw_data']['spoke']['year_0']['file_extension'],
        #                     delimiter=azure_config['raw_data']['spoke']['year_0']['delimiter'])
        spoke_year_0 = download_anaplan_data(
                    conn=conn,
                    workspace_id=spoke_workspace_id,
                    model_id=spoke_model_id,
                    action_id=azure_config['raw_data']['spoke']['year_0']['action_id'],
                    retry_count=int(azure_config['raw_data']['spoke']['year_0']['anaplan_retry_count']),
                    skip_char=int(azure_config['raw_data']['spoke']['year_0']['anaplan_skip_char']),
                    file_delimiter_anaplan=azure_config['raw_data']['spoke']['year_0']['anaplan_delimiter'],
                    use_cols=spoke_col_list,
                    engine=azure_config['raw_data']['spoke']['year_0']['anaplan_engine'],
                )
        print('Got spoke_year_0 df')

        spoke_quarter_map = {
        'Jan': 1,
        'Feb':1,
        'Mar':1,
        'Apr':2,
        'May':2,
        'Jun':2,
        'Jul':3,
        'Aug':3,
        'Sep':3,
        'Oct':4,
        'Nov':4,
        'Dec':4
        }

        spoke_year_0['Quarter'] = spoke_year_0['Time'].str[0:3].map(spoke_quarter_map)
        spoke_year_1['Quarter'] = spoke_year_1['Time'].str[0:3].map(spoke_quarter_map)
        spoke_year_2['Quarter'] = spoke_year_2['Time'].str[0:3].map(spoke_quarter_map)
        spoke_year_3['Quarter'] = spoke_year_3['Time'].str[0:3].map(spoke_quarter_map)
        
        # pull zone-rc data and prepare for mapping
        zones_full = get_df(container_name='AFN_ML/dev/cra/mappings',
                        blob_name='Zones',
                        file_extension='csv')

        zones_full['ZONES'] = zones_full["ZONES"].str.replace('OneMMC_', '')
        zones_dictionary = zones_full.set_index('RC_DEPT')['ZONES'].to_dict()
        
        # main pipeline
        def etl_spoke_df(df):  
            # Map Zones to Spoke data and filter
            df = df.assign(zone=lambda x: x["L4. Planning Product: Department Code"].map(zones_dictionary))\
                .pipe(keep_rows_based_on_list_values, 'zone', dict_of_etl['zonesana'])
                
            return df

        # apply ETL to all dfs
        spoke_year_0 = etl_spoke_df(spoke_year_0)
        spoke_year_1 = etl_spoke_df(spoke_year_1)
        spoke_year_2 = etl_spoke_df(spoke_year_2)
        spoke_year_3 = etl_spoke_df(spoke_year_3)

        # Concatenate all data files together
        spoke_data = pd.concat([spoke_year_0, spoke_year_1, spoke_year_2, spoke_year_3])

        return spoke_data
    except Exception as e:
        raise e    