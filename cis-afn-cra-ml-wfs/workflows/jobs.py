# !pip install dagster\
# !pip install redis
import os
import json
from ops.build_ml_data import build_ml_data
from ops.get_cohorts import get_cohorts
from ops.initial_etl import initial_etl
from ops.request_train_reg import train_all_regression, train_regression
from ops.get_raw_data import get_raw_data, get_raw_data_files
from ops.post_results import post_results
from ops.initial_etl_spoke import etl_spoke
from ops.apply_manual_adj import apply_manual_adjustments
from ops.apply_overrides_adjustments import apply_overrides_adjustments
from ops.build_ml_data_cls import build_ml_data_cls
from ops.request_train_cls import train_all_classification, train_classification
from ops.create_classification_report import create_cls_report
from ops.create_regression_report import create_regression_report
from ops.create_revenue_report import create_revenue_report
from ops.shift_source_data_blobs import shift_source_data_blobs
from ops.revenue_dq_check import revenue_dq_check
deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()
# Regression Jobs #

def regression_training():
    hub_data = get_raw_data_files()
    cleaned_df = initial_etl(hub_data)
    spoke_df = etl_spoke()
    (adjusted_df, latest_year, latest_quarter) = apply_manual_adjustments(df_ml=cleaned_df, df_spoke=spoke_df)
    modeling_df = build_ml_data(adjusted_df, latest_year, latest_quarter)
    final_model_df = apply_overrides_adjustments(df=modeling_df, df_ml=cleaned_df, df_spoke=spoke_df, latest_year=latest_year, latest_quarter=latest_quarter)
    revenue_check_results = revenue_dq_check(source_df=cleaned_df, modeling_df=final_model_df, latest_year=latest_year, model_name='Regression')
    (cohorts, df_for_training) = get_cohorts(final_model_df)
    all_regression_results = train_all_regression(cohorts=cohorts, df=df_for_training)

    updated_report = create_regression_report(all_regression_results=all_regression_results, latest_quarter=latest_quarter, latest_year=latest_year)
    revenue_report = create_revenue_report(source_df=cleaned_df, latest_year=latest_year, latest_quarter=latest_quarter, model_name='Regression')

    post_results(container_name=f'data/AFN_ML/{deployment_mode}/cra/modeling_data',
                 blob_name='Cleaned Hub Data',
                 file_extension='csv',
                 df=cleaned_df
    )
    post_results(container_name=f'data/AFN_ML/{deployment_mode}/cra/regression_reports/latest',
                 blob_name='Historical Regression Results Report',
                 file_extension='csv',
                 df=updated_report)
    post_results(container_name=f'data/AFN_ML/{deployment_mode}/cra/regression_reports/latest',
                blob_name='Regression Revenue Report',
                file_extension='csv',
                df=revenue_report)

    if latest_quarter == 4:
        renamed_azure_blobs = shift_source_data_blobs()

    return True

    

# Classification Job #
def classification_training():
    
    hub_data = get_raw_data_files()
    cleaned_df = initial_etl(hub_data)
    spoke_df = etl_spoke()
    (adjusted_df, latest_year, latest_quarter) = apply_manual_adjustments(df_ml=cleaned_df, df_spoke=spoke_df)
    modeling_df = build_ml_data_cls(adjusted_df, latest_year, latest_quarter)
    revenue_check_results = revenue_dq_check(source_df=cleaned_df, modeling_df=modeling_df, latest_year=latest_year, model_name='Classification')
    all_classifiction_results, feat_percentile_df, feat_imp_df  = train_all_classification(df=modeling_df)

    classification_report = create_cls_report(all_classifiction_results, cleaned_df, latest_year)
    revenue_report = create_revenue_report(source_df=cleaned_df, classification_report = classification_report, latest_year=latest_year, latest_quarter=latest_quarter, model_name='Classification')

    post_results(container_name=f'data/AFN_ML/{deployment_mode}/cra/classification_reports/latest',
            blob_name='Classification Prediction Report',
            file_extension='csv',
            df=classification_report)

    post_results(container_name=f'data/AFN_ML/{deployment_mode}/cra/classification_reports/latest',
                blob_name='Classification Revenue Report',
                file_extension='csv',
                df=revenue_report)

    post_results(container_name=f'data/AFN_ML/{deployment_mode}/cra/classification_reports/latest',
                blob_name='Classification Feature Percentile Report',
                file_extension='csv',
                df=feat_percentile_df)

    post_results(container_name=f'data/AFN_ML/{deployment_mode}/cra/classification_reports/latest',
                blob_name='Classification Feature Importance Report',
                file_extension='csv',
                df=feat_imp_df)

    return True
    

    
