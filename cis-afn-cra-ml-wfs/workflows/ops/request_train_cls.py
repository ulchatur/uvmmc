import requests
import os
import time
import re
import math
import tempfile
import pandas as pd
import concurrent.futures
from io import StringIO
from joblib import Parallel, delayed
from requests.exceptions import Timeout, RequestException
from api.main import train_model_cls

deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()
is_test = bool(int(os.environ['IS_TEST']))

if is_test:
    environment_type = "test"
else:
    environment_type = deployment_mode

def train_all_classification(df: pd.DataFrame):
    try:
        classification_results = []

        # Helper function to run train_regression asynchronously
        def run_training(recurring, product, client_size, cohort_df):
            return train_classification(recurring, product, client_size, cohort_df)

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for recurring in [True ,False]:
                cohort_df = df[df['Recurring'] == recurring]
                for product in ['Casualty', 'FINPRO', 'Property', 'Surety', 'Others']:
                    for client_size in ['medium','large','small']:
                        futures.append(executor.submit(run_training, recurring, product, client_size, cohort_df))
            
            # Collect results
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                classification_results.append(result)

        # Concatenate all results
        prediction_dfs = []
        percentile_dfs = []
        feat_imp_dfs = []

        # Loop through each dictionary
        for d in classification_results:
            prediction_dfs.append(d['prediction_results'])
            percentile_dfs.append(d['feat_percentile_results'])
            feat_imp_dfs.append(d['feat_imp_results'])

        # Concatenate all dataframes in each list
        prediction_df = pd.concat(prediction_dfs, ignore_index=True)
        feat_percentile_df = pd.concat(percentile_dfs, ignore_index=True)
        feat_imp_df = pd.concat(feat_imp_dfs, ignore_index=True)

        return prediction_df, feat_percentile_df, feat_imp_df
    
    except Exception as e:
        print(e)
        raise e



def train_classification(recurring, product, client_size, cohort_df: pd.DataFrame):
    """
    Train a single classification model.
    """

    try:
        print(f'Training classification model {recurring}, {product}, {client_size}')
        cls_train_result = train_model_cls(recurring=recurring, product=product, client_size=client_size, df=cohort_df)
        return cls_train_result

    except Exception as e:
        print(e)
        raise e

