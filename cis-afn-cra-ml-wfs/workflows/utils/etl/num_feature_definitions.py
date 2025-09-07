# feature_definitions.py
from dataclasses import dataclass

from functools import partial
from typing import Callable, List

import pandas as pd
from utils.etl.num_feature_functions import (
    compute_annual_policies_dollar_value,
    compute_avg_days_to_bill_effective,
    compute_avg_days_to_cv_effective_dt,
    compute_avg_days_to_cv_exp_dt,
    compute_avg_days_to_invoice,
    compute_avg_stickiness,
    compute_commission_dollar_value,
    compute_fee_dollar_value,
    compute_journal_volume,
    compute_multiyear_policies_dollar_value,
    compute_new_business_revenue,
    compute_non_recurring_percentage,
    compute_non_recurring_revenue,
    compute_renewal_revenue,
    compute_volume_of_audits,
    compute_volume_of_cancellations,
    compute_volume_of_endorsements,
    compute_volume_of_installments,
    compute_volume_of_reinstatements,
    compute_volume_of_reportings,
    compute_volume_of_rewrites,
    compute_volume_original_billings,
    compute_volume_other_billing_type,
    compute_renewal_rate,
    compute_qq_growth,
    compute_yy_growth,
    compute_num_subproducts,
    compute_expanded_services_revenue
)

# Import feature functions
# Import additional feature functions as needed; compute_quarter,;
#
# Include these ones once the framework is completed compute_product_growth,; compute_renewal_rate,


### Partial functions are being used here to generate multiple functions from one original function. 
# Identify the level of aggreggation/grouping that is needed, and then use the new function, created from the partial, in the feature definitions below as needed.
# I deleted the old qoq and yoy csr growth functions.
###

# # Client-level aggregation
client_level_grouping = ['client_number']

compute_renewal_rate_client = partial(
    compute_renewal_rate,
    grouping_columns=client_level_grouping
)

compute_growth_client = partial(
    compute_qq_growth,
    grouping_columns=client_level_grouping
)


compute_growth_client_year = partial(
    compute_yy_growth,
    grouping_columns=client_level_grouping
)

# # Product-level aggregation
product_level_grouping = ['product_line_nm']

compute_renewal_rate_product = partial(
    compute_renewal_rate,
    grouping_columns=product_level_grouping
)

compute_growth_product = partial(
    compute_qq_growth,
    grouping_columns=product_level_grouping
)


compute_growth_product_year = partial(
    compute_yy_growth,
    grouping_columns=product_level_grouping
)

# # Client-Product-level aggregation
client_product_level_grouping = ['client_number', 'product_line_nm']

compute_renewal_rate_client_product = partial(
    compute_renewal_rate,
    grouping_columns=client_product_level_grouping
)

compute_growth_client_product = partial(
    compute_qq_growth,
    grouping_columns=client_product_level_grouping
)

compute_growth_client_product_year = partial(
    compute_yy_growth,
    grouping_columns=client_product_level_grouping
)


@dataclass
class FeatureDefinition:
    name: str
    compute_function: Callable[[pd.DataFrame], pd.DataFrame]
    aggregation_levels: List[List[str]]  # List of levels, each level is a list of columns
    aggfunc: str = "sum"  # Default aggregation function


# Define feature definitions
feature_definitions = [
    FeatureDefinition(
        name="non_recurring_revenue",
        compute_function=compute_non_recurring_revenue,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="sum",
    ),
    FeatureDefinition(
        name="renewal_revenue",
        compute_function=compute_renewal_revenue,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="sum",
    ),
    FeatureDefinition(
        name="new_business_revenue",
        compute_function=compute_new_business_revenue,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="sum",
    ),
    FeatureDefinition(
        name="expanded_services_revenue",
        compute_function=compute_expanded_services_revenue,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="sum",
    ),
    FeatureDefinition(
        name="fee_dollar_value",
        compute_function=compute_fee_dollar_value,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="sum",
    ),
    FeatureDefinition(
        name="comm_dollar_value",
        compute_function=compute_commission_dollar_value,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="sum",
    ),
    # Add more features with their respective aggregation levels and functions
    FeatureDefinition(
        name="annual_policy_dollar_value",
        compute_function=compute_annual_policies_dollar_value,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="sum",
    ),
    FeatureDefinition(
        name="multiyear_policy_dollar_value",
        compute_function=compute_multiyear_policies_dollar_value,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="sum",
    ),
    FeatureDefinition(
        name="volume_of_journals",
        compute_function=compute_journal_volume,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),
    FeatureDefinition(
        name="volume_of_original_billings",
        compute_function=compute_volume_original_billings,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),
    FeatureDefinition(
        name="volume_of_other_billings",
        compute_function=compute_volume_other_billing_type,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),
    FeatureDefinition(
        name="volume_of_endorsements",
        compute_function=compute_volume_of_endorsements,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),
    FeatureDefinition(
        name="volume_of_audits",
        compute_function=compute_volume_of_audits,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),
    FeatureDefinition(
        name="volume_of_cancellations",
        compute_function=compute_volume_of_cancellations,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),
    FeatureDefinition(
        name="volume_of_reportings",
        compute_function=compute_volume_of_reportings,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),
    FeatureDefinition(
        name="volume_of_installments",
        compute_function=compute_volume_of_installments,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),
    FeatureDefinition(
        name="volume_of_rewrites",
        compute_function=compute_volume_of_rewrites,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),
    FeatureDefinition(
        name="volume_of_reinstatements",
        compute_function=compute_volume_of_reinstatements,
        aggregation_levels=[["client_number", "product_line_nm"]],
        aggfunc="count",
    ),

    # quarter:

    #     FeatureDefinition(
    #     name="quarter",
    #     compute_function=compute_quarter,
    #     aggregation_levels=[["quarter"]],
    #     aggfunc="last",
    # ),
    
    # commenting out avg days for now
    # FeatureDefinition(
    #     name="avg_days_to_invoice",
    #     compute_function=compute_avg_days_to_invoice,
    #     aggregation_levels=[["client_number", "product_line_nm"]],
    #     aggfunc="last",
    # ),
    # FeatureDefinition(
    #     name="avg_days_to_bill_effective",
    #     compute_function=compute_avg_days_to_bill_effective,
    #     aggregation_levels=[["client_number", "product_line_nm"]],
    #     aggfunc="last",
    # ),
    # FeatureDefinition(
    #     name="avg_days_to_cv_effective",
    #     compute_function=compute_avg_days_to_cv_effective_dt,
    #     aggregation_levels=[["client_number", "product_line_nm"]],
    #     aggfunc="last",
    # ),
    # FeatureDefinition(
    #     name="avg_days_to_cv_exp",
    #     compute_function=compute_avg_days_to_cv_exp_dt,
    #     aggregation_levels=[["client_number", "product_line_nm"]],
    #     aggfunc="last",
    # ),
    FeatureDefinition(
        name="client_nr_percentage",
        compute_function=compute_non_recurring_percentage,
        aggregation_levels=[["client_number"]],
        aggfunc="last",
    ),
    FeatureDefinition(
        name="client_avg_stickiness",
        compute_function=compute_avg_stickiness,
        aggregation_levels=[["company_number"]],
        aggfunc="last",
    ),
    #client retention rate
    FeatureDefinition(
        name='renewal_revenue_ratio_client',
        compute_function=compute_renewal_rate_client,
        aggregation_levels=[client_level_grouping],
        aggfunc='last'  # Since the ratio is already computed per record
    ),
    # client product retention rate
    FeatureDefinition(
        name='renewal_revenue_ratio_client_product',
        compute_function=compute_renewal_rate_client_product,
        aggregation_levels=[client_product_level_grouping],
        aggfunc='last'
    ),
    # product retention rate
    FeatureDefinition(
        name='renewal_revenue_ratio_product',
        compute_function=compute_renewal_rate_product,
        aggregation_levels=[product_level_grouping],
        aggfunc='last'
    ),
    # client QoQ csr growth
    FeatureDefinition(
        name='qq_growth_rate_client',
        compute_function=compute_growth_client,
        aggregation_levels=[client_level_grouping],
        aggfunc='last'
    ),
    # product QoQ csr growth
    FeatureDefinition(
        name='qq_growth_rate_product',
        compute_function=compute_growth_product,
        aggregation_levels=[product_level_grouping],
        aggfunc='last'
    ),
    # client product QoQ csr growth
    FeatureDefinition(
        name='qq_growth_rate_client_product',
        compute_function=compute_growth_client_product,
        aggregation_levels=[client_product_level_grouping],
        aggfunc='last'
    ),
    # client YoY csr growth
    FeatureDefinition(
        name='yy_growth_rate_client',
        compute_function=compute_growth_client_year,
        aggregation_levels=[client_level_grouping],
        aggfunc='last'
    ),
    # product YoY csr growth
    FeatureDefinition(
        name='yy_growth_rate_product',
        compute_function=compute_growth_product_year,
        aggregation_levels=[product_level_grouping],
        aggfunc='last'
    ),
    # client product YoY csr growth
    FeatureDefinition(
        name='yy_growth_rate_client_product',
        compute_function=compute_growth_client_product_year,
        aggregation_levels=[client_product_level_grouping],
        aggfunc='last'
    ),
    # number subproducts per client
    FeatureDefinition(
        name='num_client_subproducts',
        compute_function=compute_num_subproducts,
        aggregation_levels=[["client_number"]],
        aggfunc='last'
    )
]