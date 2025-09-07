import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import confusion_matrix

def tune_classification(trn_df_xfm, tst_df_xfm, target_df_trn, target_df_tst, cat_cols):
    trn_df_xfm = pd.get_dummies(trn_df_xfm, columns=[x for x in cat_cols], drop_first=True)
    tst_df_xfm = pd.get_dummies(tst_df_xfm, columns=[x for x in cat_cols], drop_first=True)
    
    # make sure train and test dataframes have same columns
    common_columns = trn_df_xfm.columns.intersection(tst_df_xfm.columns)
    trn_df_xfm = trn_df_xfm[common_columns]
    tst_df_xfm = tst_df_xfm[common_columns]
    
    rf_model = RandomForestClassifier(random_state=42)
    
    # param_grid = {
    #     'n_estimators': np.arange(100, 500, 50),
    #     'max_depth': [None, 10, 20, 30],
    #     'min_samples_split': [2, 5, 10],
    #     'min_samples_leaf': [1, 2, 4],
    #     'bootstrap': [True, False],
    #     'criterion': ['gini', 'entropy', 'log_loss']
    # }

    # smaller param grid for faster runs
    param_grid = {
        'n_estimators': np.arange(100, 200, 50),
        'max_depth': [None, 10, 20],
        'min_samples_split': [2, 5,],
        'min_samples_leaf': [1, 2],
        'bootstrap': [True, False],
        'criterion': ['entropy', 'log_loss']
    }

    # Hyperparameter tuning
    grid_search = GridSearchCV(estimator=rf_model, param_grid=param_grid,
                                cv=2, n_jobs=-1)

    # Fit training set on the best parameters
    grid_search.fit(trn_df_xfm, target_df_trn)

    best_params = grid_search.best_params_
    
    feature_importance = grid_search.best_estimator_.feature_importances_

    y_pred_proba = grid_search.predict_proba(tst_df_xfm)[:, 1]

    best_weights, optimal_threshold = find_best_weights(target_df_tst, y_pred_proba)

    return best_params, optimal_threshold, feature_importance, trn_df_xfm

def find_best_weights(y_true, y_proba, weight_range=np.linspace(0.1, 10, 10)):
    best_score = -np.inf
    best_weights = (1, 1)
    best_threshold = 0.5
    for weight_tn in weight_range:
        for weight_tp in weight_range:
            threshold = find_optimal_threshold(y_true, y_proba, weight_tn=weight_tn, weight_tp=weight_tp)
            y_pred = (y_proba >= threshold).astype(int)
            tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
            score = (tn / (tn + fp) if (tn + fp) > 0 else 0) * weight_tn + (tp / (tp + fn) if (tp + fn) > 0 else 0) * weight_tp

            if score > best_score:
                best_score = score
                best_weights = (weight_tn, weight_tp)
                best_threshold = threshold

    return best_weights, best_threshold  



def find_optimal_threshold(y_true, y_proba, weight_tn, weight_tp):
 
    thresholds = np.linspace(0.01, 0.95, 2000)
    best_threshold = 0.5
    best_score = 0
    for threshold in thresholds:
        y_pred_thresh = (y_proba >= threshold).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred_thresh).ravel()
        tp_rate = tp / (tp + fn) if (tp + fn) > 0 else 0
        tn_rate = tn / (tn + fp) if (tn + fp) > 0 else 0
        score = (tn_rate * weight_tn) + (tp_rate * weight_tp)
        if score > best_score:
            best_score = score
            best_threshold = threshold
    return best_threshold