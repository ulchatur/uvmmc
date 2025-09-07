import json
import os
import math
import logging
import zlib
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from api.utils.tuning.read_infile import read_infile
from api.utils.tuning.trn_df import prep_trn_df
from api.utils.tuning.test_df import prep_test_df
from api.utils.tuning.feature_engineering import feature_engg
from api.utils.tuning.reg_model_optimizer import reg_model_optimizer
from api.utils.tuning.tune_classification import tune_classification
from api.utils.tuning.read_infile_cls import read_infile_cls
from api.utils.tuning.test_df_cls import prep_test_df_cls
from api.utils.tuning.trn_df_cls import prep_trn_df_cls
from api.utils.tuning.predict_classification import predict_classification
from api.utils.tuning.predict_regression import predict_regression
from api.utils.tuning.feature_importance_utils import *

deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()
is_test = bool(int(os.environ['IS_TEST']))


if is_test:
    environment_type = "test"
else:
    environment_type = deployment_mode


executor = ThreadPoolExecutor(max_workers=4)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
results_store = {}


def train_model_regression(zone: str, product: str, quarter: int, source_df: pd.DataFrame):
    logger = logging.getLogger(__name__)

    ## TRAINING
    df, df_keys, df_cat, target_df_trn, target_df_tst, latest_quarter = read_infile(df=source_df, zone=zone, product=product, qtr=quarter, inference=False)
    trn_df, feature_dict_trn, correlated_groups = prep_trn_df(df, latest_quarter, inference=False)
    tst_df, feature_dict_tst = prep_test_df(df, correlated_groups, latest_quarter, inference=False)
    trn_df_xfm, tst_df_xfm, cat_cols = feature_engg(trn_df, tst_df, df_cat)
    best_params = reg_model_optimizer(trn_df_xfm, tst_df_xfm, target_df_trn, target_df_tst, cat_cols)

    ## INFERENCE
    df, df_keys, df_cat, target_df_trn, target_df_tst, latest_quarter = read_infile(df=source_df, zone=zone, product=product, qtr=quarter, inference=True)
    trn_df, feature_dict_trn, correlated_groups = prep_trn_df(df, latest_quarter, inference=True)
    tst_df, feature_dict_tst = prep_test_df(df, correlated_groups, latest_quarter, inference=True)
    trn_df_xfm, tst_df_xfm, cat_cols = feature_engg(trn_df, tst_df, df_cat)
    inference_prediction = predict_regression(trn_df_xfm, target_df_trn, tst_df_xfm, cat_cols, best_params)

    # Prepare output data
    df_keys['product_model'] = product
    df_keys['quarter'] = quarter
    df_keys['zone'] = zone
    df_keys['prediction'] = inference_prediction

    # df = df_keys.to_dict(orient='records')
    return df_keys


def train_model_cls(recurring, client_size, product, df):
    out_data = {'client_number':[],
            'product_line_nm': [],
            'Client Size':[],
            'Recurring': [],
            'prediction':[],
            'pred_renewal_prob':[],
            'optimal_threshold': [],

    }
    
    logger = logging.getLogger(__name__)
    
    source_df = df
    
    # Perform your model training steps here
    df, df_keys = read_infile_cls(source_df=source_df, product=product, inference=False)
    trn_df, feature_dict_trn, correlated_groups, target_df_trn = prep_trn_df_cls(df, client_size=client_size, product=product, inference = False)
    tst_df, feature_dict_tst, target_df_tst, tst_clients = prep_test_df_cls(df, correlated_groups, client_size=client_size, product=product, inference = False)
    trn_df_xfm, tst_df_xfm, cat_cols = feature_engg(trn_df, tst_df)
    best_params,optimal_threshold, feature_importance, trn_df_xfm = tune_classification(trn_df_xfm, tst_df_xfm, target_df_trn, target_df_tst, cat_cols)

    ## FEATURE IMPORTANCE?
    full_feat_imp_df, features_source_df, top_3_feature_names = get_feat_imp_percentile_df(feature_importance, trn_df_xfm, correlated_groups, feature_dict_trn, recurring, product, client_size, source_df)
    
    ## INFERENCE
    df, df_keys = read_infile_cls(source_df=source_df, product=product, inference=True)
    inference_trn_df, inference_feature_dict_trn, inference_correlated_groups, inference_target_df_trn = prep_trn_df_cls(df, client_size=client_size, product=product, inference = True)
    inference_tst_df, inference_feature_dict_tst, inference_target_df_tst, inference_tst_clients = prep_test_df_cls(df, inference_correlated_groups, client_size=client_size, product=product, inference = True)
    inference_trn_df_xfm, inference_tst_df_xfm, inference_cat_cols = feature_engg(inference_trn_df, inference_tst_df)
    inference_prediction_values, inference_y_pred_proba = predict_classification(inference_trn_df_xfm, inference_tst_df_xfm, inference_target_df_trn, inference_cat_cols, optimal_threshold, best_params)

    df_keys = df_keys[df_keys.index.isin(inference_tst_clients)]
    for i, client_number in enumerate(df_keys['client_number']):
        out_data['client_number'].append(client_number)
        out_data['product_line_nm'].append(product)
        out_data['Client Size'].append(client_size)
        out_data['Recurring'].append(recurring)
        out_data['prediction'].append(float(inference_prediction_values[i]))
        out_data['pred_renewal_prob'].append(float(inference_y_pred_proba[i]))
        out_data['optimal_threshold'].append(float(optimal_threshold))

    # logger.info({k: len(v) for k, v in out_data.items()})
    df = pd.DataFrame(out_data)


    return {
        "prediction_results": df,
        "feat_percentile_results": features_source_df,
        "feat_imp_results": full_feat_imp_df
    }

