import xgboost as xgb
import pandas as pd

def predict_regression(X_train, y_train, X_test, cat_cols, best_params):
    
    X_train = pd.get_dummies(X_train, columns=[x for x in cat_cols], drop_first=True)
    X_test = pd.get_dummies(X_test, columns=[x for x in cat_cols], drop_first=True)

    common_columns = X_train.columns.intersection(X_test.columns)
    X_train = X_train[common_columns]
    X_test = X_test[common_columns]
    
    best_params['max_depth'] = int(best_params['max_depth'])
    best_params['n_estimators'] = int(best_params['n_estimators'])

    xgb_model = xgb.XGBRegressor(**best_params)
    xgb_model.fit(X_train, y_train)
    predictions = xgb_model.predict(X_test)
    return predictions