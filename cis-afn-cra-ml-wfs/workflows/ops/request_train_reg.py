import requests
import os
import time
import re
import tempfile
import pandas as pd
from io import StringIO
import concurrent.futures
from joblib import Parallel, delayed
from requests.exceptions import Timeout, RequestException
from api.main import train_model_regression
deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()
is_test = bool(int(os.environ['IS_TEST']))

if is_test:
    environment_type = "test"
else:
    environment_type = deployment_mode


def train_all_regression(cohorts: list, df: pd.DataFrame):
    try:
        regression_results = []

        # Helper function to run train_regression asynchronously
        def run_training(zone, product, quarter, cohort_df):
            return train_regression(zone, product, quarter, cohort_df)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []

            # Loop through quarters 1 to 4
            for quarter in range(0, 4):
                # Model 1: All zones/products
                cohort_df = df
                futures.append(executor.submit(run_training, 'None', 'None', quarter, cohort_df))
                
                # Model 2: Specific zone-product pairs
                for zone_product in cohorts:
                    product, zone = zone_product
                    cohort_df = df[(df['mi_lookup_level4'] == zone) & (df['product_line_nm'] == product)]
                    futures.append(executor.submit(run_training, zone, product, quarter, cohort_df))
                    
                # Model 3: Zones only
                zones = [cohort[1] for cohort in cohorts]
                unique_zones = list(set(zones))
                for zone in unique_zones:
                    cohort_df = df[(df['mi_lookup_level4'] == zone)]
                    futures.append(executor.submit(run_training, zone, 'None', quarter, cohort_df))
                
            # Collect results
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                print(f'Got result: {type(result)}')
                regression_results.append(result)

        # Concatenate all results
        all_regression_results = pd.concat(regression_results, ignore_index=True)
        return all_regression_results

    except Exception as e:
        print(e)
        raise e


def train_regression(zone: str, product:str, quarter, cohort_df):
    """
    Trigger a single API request. Each dynamic op calls the API service.
    """

    try:
        print(f'Training regression model: {zone}, {product}, Q{quarter}')
        regression_result = train_model_regression(zone, product, quarter, cohort_df)
        return regression_result
    except Exception as e:
        print(e)
        raise e

    
     
       