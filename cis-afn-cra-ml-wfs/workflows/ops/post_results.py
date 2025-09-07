import os
import json
import pandas as pd
from utils.azure.post_df import post_df
from utils.general.time_functions import get_day_month_yr
from utils.general.get_config import get_config

deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()

azure_config = get_config('azure_config.json')
results_container = azure_config['results']['container'].replace('ENV_PLACEHOLDER', deployment_mode)

def post_results(container_name, blob_name, file_extension, df: pd.DataFrame):
    """
    Function to post regression training results to Azure storage container.

    Args:
        df (pd.DataFrame): The DataFrame containing the training results to be posted.

    Returns:
        bool: True if the training results are successfully posted.
    """
    try:
        post_df(
            container_name=container_name,
            blob_name=blob_name,
            file_extension_azure=file_extension,
            dataframe=df,
        )
        print(f"SUCCESS: Post training results")
        return True
    except Exception as e:
        print(f"FAILURE:{e}")
        raise e
