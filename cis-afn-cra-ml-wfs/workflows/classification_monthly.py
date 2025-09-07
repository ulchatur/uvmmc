import os
from datetime import datetime
from dotenv import load_dotenv
from utils.azure.get_df import get_df
from utils.azure.post_df import post_df
from utils.anaplan.download_anaplan_data import download_anaplan_data
from utils.general.get_config import get_config
from utils.etl.classification_report_utils import update_cy_renewal_months, update_months
from config.anaplan_column_lists import hub_col_list
from utils.anaplan.cert_pull import startup
from utils.anaplan.anaplan_connection import get_conn
from ops.initial_etl import initial_etl

azure_config = get_config('azure_config.json')
deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()
hub_workspace_id = os.environ['HUB_WORKSPACE_ID']
hub_model_id = os.environ['HUB_MODEL_ID']

load_dotenv(dotenv_path ='/Workspace/Shared/.env')

def classification_monthly():
    startup()
    # pull classification report (should we pull the latest month or original?)
    classification_report = get_df(
        container_name=f'AFN_ML/{deployment_mode}/cra/classification_reports/latest',
        blob_name='Classification Prediction Report',
        file_extension='csv'
    )
    prediction_year = int(classification_report['prediction_year'].values[0])
    print(f'Found prediction year {prediction_year}')

    # pull latest hub data from anaplan and apply initial ETL to it
    conn = get_conn(workspace_id=hub_workspace_id, model_id=hub_model_id)
    latest_hub_data = download_anaplan_data(
        conn=conn,
        workspace_id=hub_workspace_id,
        model_id=hub_model_id,
        action_id=azure_config['raw_data']['hub']['year_0']['action_id'],
        retry_count=3,
        skip_char=0,
        file_delimiter_anaplan=",",
        use_cols=hub_col_list,
        engine='python',
    )
    cleaned_hub_data = initial_etl(latest_hub_data)
    

    # update cy renewal months based on new data
    updated_report = update_cy_renewal_months(
        source_df = cleaned_hub_data,
        prediction_df = classification_report,
        prediction_year=prediction_year,
    )

    current_month = datetime.now().month
    
    # ensure that renewal revenue does not sum to 0 for the year
    updated_report_final = update_months(
        source_df = cleaned_hub_data,
        initial_report = updated_report,
        year=prediction_year,
        latest_year=int(prediction_year - 1)
    )

    # post back to azure
    current_month = datetime.now().month
    current_year = datetime.now().year
    post_df(container_name=f'data/AFN_ML/{deployment_mode}/cra/classification_reports/monthly',
            blob_name=f'Classification Prediction Report - {current_month}/{current_year}',
            file_extension_azure='csv',
            dataframe=updated_report_final)
    
    return True


def run_classification_monthly():
    result = classification_monthly()
    if result:
        print("Job executed successfully")
    else:
        print("Job failed")

if __name__ == "__main__":
    run_classification_monthly()

   