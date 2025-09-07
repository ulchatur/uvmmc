import os
import pandas as pd
from utils.azure.get_df import get_df
from utils.azure.post_df import post_df
from utils.general.get_config import get_config
from utils.anaplan.download_anaplan_data import download_anaplan_data
from config.anaplan_column_lists import hub_col_list
from utils.anaplan.cert_pull import startup
from utils.anaplan.anaplan_connection import get_conn

azure_config = get_config('azure_config.json')
deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()
hub_container = azure_config['raw_data']['hub']['container'].replace("ENV_PLACEHOLDER", deployment_mode)

hub_workspace_id = os.environ['HUB_WORKSPACE_ID']
hub_model_id = os.environ['HUB_MODEL_ID']

def get_raw_data_files():
    """
    Function to retrieve raw data from a parquet file.

    Returns:
        pd.Dataframe: Dataframe with all years of data concatenated
"""
    try:
        startup()

        # generate connection to anaplan
        print("making connection to anaplan...")
        conn = get_conn(workspace_id=hub_workspace_id, model_id=hub_model_id)
        print('made connection to anaplan')
    
        hub_config = azure_config['raw_data']['hub']
        config_keys = list(hub_config.keys())
        non_year_keys = [val for val in config_keys if 'year' not in val]

        for key in non_year_keys:
            hub_config.pop(key)
    
        yearly_dfs = []
        for key, value in hub_config.items():
            file_source = value['file_source']
            if file_source == 'azure':
                yearly_df = get_raw_data(blob_details=value)
            if file_source == 'anaplan':
                yearly_df = download_anaplan_data(
                    conn=conn,
                    workspace_id=hub_workspace_id,
                    model_id=hub_model_id,
                    action_id=value['action_id'],
                    retry_count=3,
                    chunk_size=100000,
                    skip_char=0,
                    file_delimiter_anaplan=",",
                    use_cols=hub_col_list,
                    engine='python',
                )
                print(f'Got {key} df')
                if key == 'year_0':
                    post_df(
                        container_name=hub_container,
                        blob_name='year_0',
                        file_extension_azure='parquet',
                        dataframe=yearly_df
                    )
            yearly_dfs.append(yearly_df)

        full_df = pd.concat(yearly_dfs, ignore_index=False)
        return full_df
    except Exception as e:
        raise e  

def get_raw_data(blob_details: dict):
    """
    Pull single blob from azure.
    """
    try:
        yearly_hub_df = get_df(container_name=hub_container,
                        blob_name= blob_details['blob_name'],
                        file_extension=blob_details['file_extension'])

        print(f'Got {blob_details["blob_name"]} df')

        return yearly_hub_df
    except Exception as e:
        raise e