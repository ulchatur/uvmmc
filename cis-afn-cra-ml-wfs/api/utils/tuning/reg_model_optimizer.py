import logging
import numpy as np
import pandas as pd
import xgboost as xgb
import hyperopt
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from hyperopt.pyll.base import scope
hyperopt.pyll.stochastic.tqdm = lambda *args, **kwargs: None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Define the hyperparameter space
def reg_model_optimizer(trn_df_xfm, tst_df_xfm, target_df_trn, target_df_tst, cat_cols):
    logging.getLogger('hyperopt').setLevel(logging.WARNING)

    space = {
        'max_depth': scope.int(hp.quniform('max_depth', 2, 5, 1)),
        'n_estimators': scope.int(hp.quniform('n_estimators', 100, 2000, 200)),
        'learning_rate': hp.loguniform('learning_rate', -4, -1),
        'subsample': hp.uniform('subsample', 0.6, 1),
        'colsample_bytree': hp.uniform('colsample_bytree', 0.6, 1),
        'gamma': hp.uniform ('gamma', 0,1),
        'reg_alpha' : hp.uniform('reg_alpha', 0,50),
        'reg_lambda' : hp.uniform('reg_lambda', 10,100),
    }
    
    def calc_score(y_test, y_pred):
        return abs(sum(y_pred - y_test.values) / sum(y_test.values))

    
    # Define the objective function to minimize
    def objective(params):
        xgb_model = xgb.XGBRegressor(**params)
        xgb_model.fit(X_train, y_train)
        y_pred = xgb_model.predict(X_test)
        # Check if sum of y_test is zero
        if sum(y_test.values) == 0:
            # Calculate residuals
            residuals = y_pred - y_test.values
            score = np.sum(np.abs(residuals))
        else:
            score = calc_score(y_test, y_pred)
        return {'loss': score, 'status': STATUS_OK, 'prediction': np.sum(y_pred), 'actual': np.sum(y_test)}
    
    # Perform the optimization
    ## Create dtrain
    X_train = trn_df_xfm
    y_train = target_df_trn
    X_train = pd.get_dummies(X_train, columns=[x for x in cat_cols], drop_first=True)
    logger.info('Created DF Train')
    ## Create dtest
    # Make predictions
    X_test = tst_df_xfm
    y_test = target_df_tst
    X_test = pd.get_dummies(X_test, columns=[x for x in cat_cols], drop_first=True)
    logger.info('Created DF Test')

    d_cols = list(set(X_train.columns) - set(X_test.columns)) + list(set(X_test.columns) - set(X_train.columns))
    f_cols = list(set(X_test.columns) - set(d_cols))
    
    X_train = X_train[f_cols]
    X_test = X_test[f_cols]
    logger.info('Created DF Train and Test with relevant features')
    
    trials = Trials()
    logger.info('Initialized Trials')
    best_params = fmin(objective, space, algo=tpe.suggest, max_evals=100, trials=trials, show_progressbar=False)
    rounded_best_params = {k: round(float(v), 5) if isinstance(v, (float, np.float64)) else int(v) for k, v in best_params.items()}
    return rounded_best_params