import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier


def predict_classification(trn_df_xfm, tst_df_xfm, target_df_trn, cat_cols, optimal_threshold, params):

    trn_df_xfm = pd.get_dummies(trn_df_xfm, columns=[x for x in cat_cols], drop_first=True)
    tst_df_xfm = pd.get_dummies(tst_df_xfm, columns=[x for x in cat_cols], drop_first=True)

    common_columns = trn_df_xfm.columns.intersection(tst_df_xfm.columns)
    trn_df_xfm = trn_df_xfm[common_columns]
    tst_df_xfm = tst_df_xfm[common_columns]

    params['n_estimators'] = int(params['n_estimators'])
    params['min_samples_split'] = int(params['min_samples_split'])
    params['min_samples_leaf'] = int(params['min_samples_leaf'])

    rf_model = RandomForestClassifier(**params)

    rf_model.fit(trn_df_xfm, target_df_trn)

    y_pred_proba = rf_model.predict_proba(tst_df_xfm)[:, 1] 

    y_pred = (y_pred_proba >= optimal_threshold).astype(int)

    prediction_values = y_pred

    return (prediction_values, y_pred_proba)
