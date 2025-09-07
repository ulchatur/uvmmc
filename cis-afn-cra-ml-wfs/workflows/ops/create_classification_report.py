import os
import pandas as pd
from utils.etl.classification_report_utils import add_py_renewal_month, extract_reports, add_client_names, add_csr_numbers, get_revenue_numbers
from utils.azure.post_df import post_df

deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()

def create_cls_report(cls_report: pd.DataFrame, source_df: pd.DataFrame, latest_year):
    try:
        cls_report_w_months = add_py_renewal_month(prediction_df=cls_report, source_df=source_df, latest_year=latest_year)
        cls_report_clients = add_client_names(prediction_df=cls_report_w_months,source_df=source_df)

        # Reorder/rename columns for prediction report
        cls_report_clients['prediction_year'] = int(latest_year)
        cls_prediction_report = cls_report_clients[['client_number', 'client_nm', 'product_line_nm','Client Size', 'recurring', 'prediction_year', 'prediction', 'pred_renewal_prob', 'optimal_threshold', 'py_renewal_month', 'cy_renewal_month']]
        cls_prediction_report.rename(columns={'client_nm':'client_name', 'Client Size':'client_size','pred_renewal_prob': 'prediction_probability'}, inplace=True)

        # post reports
        # post_df(container_name=f'data/AFN_ML/{deployment_mode}/cra/classification_reports/latest',
        #         blob_name='Model Feature Importance Report',
        #         file_extension_azure='csv',
        #         dataframe_orient='tight',
        #         dataframe=feature_imp_report)

        # post_df(container_name=f'data/AFN_ML/{deployment_mode}/cra/classification_reports/latest',
        #         blob_name='Client Feature Importance Percentile Report',
        #         file_extension_azure='csv',
        #         dataframe_orient='tight',
        #         dataframe=feature_percentile_report
            
        # )
        # post_df(container_name=f'data/AFN_ML/{deployment_mode}/cra/classification_reports/latest',
        #         blob_name='Classification Predictions Report',
        #         file_extension_azure='csv',
        #         dataframe_orient='tight',
        #         dataframe=cls_prediction_report
            
        # )
        return cls_prediction_report
    except Exception as e:
        raise e

