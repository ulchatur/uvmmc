# feature_functions.py
from typing import List
import numpy as np
import pandas as pd

def compute_non_recurring_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes non-recurring revenue.
    """
    df["non_recurring_revenue"] = df["net_revenue"].where(df["non_recurring_flag"], 0)
    return df


def compute_renewal_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes renewal revenue.
    """
    df["renewal_revenue"] = df["net_revenue"].where(df["production_code"] == "Renewal", 0)
    return df


def compute_new_business_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes new business revenue.
    """
    df["new_business_revenue"] = df["net_revenue"].where(df["production_code"] == "New business", 0)
    return df


def compute_expanded_services_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes new business revenue.
    """
    df["expanded_services_revenue"] = df["net_revenue"].where(df["production_code"] == "Expanded services", 0)
    return df


def compute_product_growth(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the growth of 'net_revenue' at the product and quarter level.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'product_growth' column added.
    """
    # Sum 'net_revenue' per product per period
    product_revenue = df.groupby(["product_line_nm", "year", "quarter"])["net_revenue"].sum().reset_index()

    # Sort data by product line, year, and quarter to ensure correct alignment for growth calculation
    product_revenue = product_revenue.sort_values(by=["product_line_nm", "year", "quarter"])

    # Create a new column for previous year's revenue by shifting net revenue within each product and quarter group
    product_revenue["prev_year_revenue"] = product_revenue.groupby(["product_line_nm", "quarter"])["net_revenue"].shift(1)

    # Calculate the percentage growth by comparing current revenue to the previous year's revenue
    product_revenue["product_growth"] = (
        (product_revenue["net_revenue"] - product_revenue["prev_year_revenue"]) / product_revenue["prev_year_revenue"] * 100
    )

    # Merge the calculated growth back into the original DataFrame
    df = df.merge(
        product_revenue[["product_line_nm", "year", "quarter", "product_growth"]],
        on=["product_line_nm", "year", "quarter"],
        how="left",
    )

    return df


def compute_client_growth(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the growth of 'net_revenue' at the client and quarter level.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'client_growth' column added.
    """
    # Sum 'net_revenue' per client per period
    client_revenue = df.groupby(["client_number", "year", "quarter"])["net_revenue"].sum().reset_index()

    # Sort data by client number, year, and quarter to ensure correct alignment for growth calculation
    client_revenue = client_revenue.sort_values(by=["client_number", "year", "quarter"])

    # Create a new column for previous year's revenue by shifting net revenue within each client and quarter group
    client_revenue["prev_year_revenue"] = client_revenue.groupby(["client_number", "quarter"])["net_revenue"].shift(1)

    # Calculate the percentage growth by comparing current revenue to the previous year's revenue
    client_revenue["client_growth"] = (
        (client_revenue["net_revenue"] - client_revenue["prev_year_revenue"]) / client_revenue["prev_year_revenue"] * 100
    )

    # Merge the calculated growth back into the original DataFrame
    df = df.merge(
        client_revenue[["client_number", "year", "quarter", "client_growth"]],
        on=["client_number", "year", "quarter"],
        how="left",
    )

    return df


def compute_client_product_growth(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the growth of 'net_revenue' at the client and product level.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'client_product_growth' column added.
    """
    # Sum 'net_revenue' per client and product per period
    client_product_revenue = (
        df.groupby(["client_number", "product_line_nm", "year", "quarter"])["net_revenue"].sum().reset_index()
    )

    # Sort data by client number, product line, year, and quarter to ensure correct alignment for growth calculation
    client_product_revenue = client_product_revenue.sort_values(by=["client_number", "product_line_nm", "year", "quarter"])

    # Create a new column for previous year's revenue by shifting net revenue within each client, product, and quarter group
    client_product_revenue["prev_year_revenue"] = client_product_revenue.groupby(
        ["client_number", "product_line_nm", "quarter"]
    )["net_revenue"].shift(1)

    # Calculate the percentage growth by comparing current revenue to the previous year's revenue
    client_product_revenue["client_product_growth"] = (
        (client_product_revenue["net_revenue"] - client_product_revenue["prev_year_revenue"])
        / client_product_revenue["prev_year_revenue"]
        * 100
    )

    # Merge the calculated growth back into the original DataFrame
    df = df.merge(
        client_product_revenue[
            [
                "client_number",
                "product_line_nm",
                "year",
                "quarter",
                "client_product_growth",
            ]
        ],
        on=["client_number", "product_line_nm", "year", "quarter"],
        how="left",
    )

    return df


def compute_fee_dollar_value(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the fee dollar value by summing net revenue where 'basis code' is
    equal to 'Fee in Lieu of Commission' or 'Fee for Services'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'fee_dollar_value' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["basis_code_normalized"] = df["basis_code"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["fee in lieu of commission", "fee for services"]

    # Compute 'fee_dollar_value'
    df["fee_dollar_value"] = df["net_revenue"].where(df["basis_code_normalized"].isin(target_codes), 0)

    return df


def compute_commission_dollar_value(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the commission dollar value by summing net revenue where 'basis code' is
    is not equal to 'Fee in Lieu of Commission' or 'Fee for Services'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'commission_dollar_value' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["basis_code_normalized"] = df["basis_code"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["fee in lieu of commission", "fee for services"]

    # Compute 'fee_dollar_value'
    df["comm_dollar_value"] = df["net_revenue"].where(~df["basis_code_normalized"].isin(target_codes), 0)

    return df


# Additional feature functions can be added here...
# Each team member can add their functions in this module or separate modules as needed.


def compute_annual_policies_dollar_value(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the annual policy dollar value by summing net revenue where 'duration code' is
    equal to 'Annual Policy'

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'annual_policy_dollar_value' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["duration_cd_normalized"] = df["duration_cd"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["annual policy"]

    # Compute 'fee_dollar_value'
    df["annual_policy_dollar_value"] = df["net_revenue"].where(df["duration_cd_normalized"].isin(target_codes), 0)

    return df


def compute_multiyear_policies_dollar_value(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the multi year dollar value by summing net revenue where 'duration code' is
    equal to 'Policy period is more than 1 year'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'multiyear_policy_dollar_value' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["duration_cd_normalized"] = df["duration_cd"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["policy period is more than 1 year"]

    # Compute 'multiyear_policy_dollar_value'
    df["multiyear_policy_dollar_value"] = df["net_revenue"].where(df["duration_cd_normalized"].isin(target_codes), 0)

    return df


def compute_journal_volume(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of journals by counting the number Revenue IDs where 'entry mode' is
    equal to 'Journal Entry' and 'accrual_type_us' is equal to 'Y'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_journals' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["entry_mode_normalized"] = df["entry_mode"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["journal entry"]

    # Compute 'volume_of_journals'
    df["volume_of_journals"] = df[(df["entry_mode_normalized"].isin(target_codes)) & (df["accrual_type_us"] == "Y")][
        "revenue_id"
    ]

    return df


def compute_volume_original_billings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of original billings by counting the number of Revenue IDs where 'billing type' is
    equal to 'Original'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_original_billings' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["billing_type_normalized"] = df["billing_type"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["original"]

    # Compute 'volume_original_billings'
    df["volume_of_original_billings"] = df[df["billing_type_normalized"].isin(target_codes)]["revenue_id"]
    return df


def compute_volume_other_billing_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of other billings by counting the number Revenue IDs where 'billing_type' is
    equal to 'Other'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_other_billings' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["billing_type_normalized"] = df["billing_type"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["other"]

    # Compute 'volume_other_billings'
    df["volume_of_other_billings"] = df[df["billing_type_normalized"].isin(target_codes)]["revenue_id"]
    return df


def compute_volume_of_endorsements(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of endorsements by counting the number Revenue IDs where 'billing_type' is
    equal to 'Endorsement'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_endorsements' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["billing_type_normalized"] = df["billing_type"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["endorsement"]

    # Compute 'volume_of_endorsements'
    df["volume_of_endorsements"] = df[df["billing_type_normalized"].isin(target_codes)]["revenue_id"]
    return df


def compute_volume_of_audits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of audits by counting the number Revenue IDs where 'billing_type' is
    equal to 'Audit.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_audits' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["billing_type_normalized"] = df["billing_type"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["audit"]

    # Compute 'volume_of_audits'
    df["volume_of_audits"] = df[df["billing_type_normalized"].isin(target_codes)]["revenue_id"]
    return df


def compute_volume_of_cancellations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of cancellations by counting the number Revenue IDs where 'billing_type' is
    equal to 'Cancellation'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_cancellations' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["billing_type_normalized"] = df["billing_type"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["cancellation"]

    # Compute 'volume_of_cancellations'
    df["volume_of_cancellations"] = df[df["billing_type_normalized"].isin(target_codes)]["revenue_id"]
    return df


def compute_volume_of_reportings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of reportings by counting the number Revenue IDs where 'billing_type' is
    equal to 'Reporting'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_reportings' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["billing_type_normalized"] = df["billing_type"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["reporting"]

    # Compute 'volume_of_reportings'
    df["volume_of_reportings"] = df[df["billing_type_normalized"].isin(target_codes)]["revenue_id"]
    return df


def compute_volume_of_installments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of installments by counting the number Revenue IDs where 'billing_type' is
    equal to 'Installment'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_installments' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["billing_type_normalized"] = df["billing_type"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["installment"]

    # Compute 'volume_of_installments'
    df["volume_of_installments"] = df[df["billing_type_normalized"].isin(target_codes)]["revenue_id"]
    return df


def compute_volume_of_rewrites(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of rewrites by counting the number Revenue IDs where 'billing_type' is
    equal to 'Rewrite'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_rewrites' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["billing_type_normalized"] = df["billing_type"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["rewrite"]

    # Compute 'volume_of_rewrites'
    df["volume_of_rewrites"] = df[df["billing_type_normalized"].isin(target_codes)]["revenue_id"]
    return df


def compute_volume_of_reinstatements(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the volume of reinstatements by counting the number Revenue IDs where 'billing_type' is
    equal to 'Re-instatement'.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing revenue data.

    Returns:
        pd.DataFrame: DataFrame with 'volume_of_reinstatement' column added.
    """
    # Normalize 'bases code' to lowercase and strip whitespace
    df["billing_type_normalized"] = df["billing_type"].str.lower().str.strip()

    # Define the target bases codes
    target_codes = ["re-instatement"]

    # Compute 'volume_of_reinstatement'
    df["volume_of_reinstatements"] = df[df["billing_type_normalized"].isin(target_codes)]["revenue_id"]
    return df


# def compute_qq_growth_client_product(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Computes the QoQ growth in 'net_revenue' at the client product level.

#     Parameters:
#         df (pd.DataFrame): Input DataFrame.

#     Returns:
#         pd.DataFrame: DataFrame with 'client_growth' column added.
#     """
#     # Sum 'net_revenue' per client per product per period
#     client_prod_revenue = df.groupby(["client_number", "product_line_nm", "quarter"])["net_revenue"].sum().reset_index()

#     # Sort for correct diff calculation
#     client_prod_revenue = client_prod_revenue.sort_values(by=["client_number", "product_line_nm", "quarter"])

#     client_prod_revenue["prev_quarter_revenue"] = client_prod_revenue.groupby(["client_number", "product_line_nm"])[
#         "net_revenue"
#     ].shift(1)

#     # Calculate the percentage growth by comparing current revenue to the previous year's revenue
#     client_prod_revenue["quarter_growth_client_prod"] = (
#         (client_prod_revenue["net_revenue"] - client_prod_revenue["prev_quarter_revenue"])
#         / client_prod_revenue["prev_quarter_revenue"]
#         * 100
#     )
#     # Merge the growth back to the original DataFrame
#     df = df.merge(
#         client_prod_revenue[
#             [
#                 "client_number",
#                 "product_line_nm",
#                 "quarter",
#                 "quarter_growth_client_prod",
#             ]
#         ],
#         on=["client_number", "product_line_nm", "quarter"],
#         how="left",
#     )

#     return df


# def compute_qq_growth_client(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Computes the QoQ growth in 'net_revenue' at the client level.

#     Parameters:
#         df (pd.DataFrame): Input DataFrame.

#     Returns:
#         pd.DataFrame: DataFrame with 'client_growth' column added.
#     """
#     # Sum 'net_revenue' per client per period
#     client_revenue = df.groupby(["client_number", "quarter"])["net_revenue"].sum().reset_index()

#     # Sort for correct diff calculation
#     client_revenue = client_revenue.sort_values(by=["client_number", "quarter"])

#     client_revenue["prev_quarter_revenue"] = client_revenue.groupby("client_number")["net_revenue"].shift(1)

#     # Calculate the percentage growth by comparing current revenue to the previous year's revenue
#     client_revenue["quarter_growth_client"] = (
#         (client_revenue["net_revenue"] - client_revenue["prev_quarter_revenue"]) / client_revenue["prev_quarter_revenue"] * 100
#     )

#     # Merge the calculated growth back into the original DataFrame
#     df = df.merge(
#         client_revenue[["client_number", "quarter", "quarter_growth_client"]],
#         on=["client_number", "quarter"],
#         how="left",
#     )
#     return df


# def compute_qq_growth_product(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Computes the QoQ growth in 'net_revenue' at the product level.

#     Parameters:
#         df (pd.DataFrame): Input DataFrame.

#     Returns:
#         pd.DataFrame: DataFrame with 'client_growth' column added.
#     """
#     # Sum 'net_revenue' per product per period
#     product_revenue = df.groupby(["product_line_nm", "quarter"])["net_revenue"].sum().reset_index()

#     # Sort for correct diff calculation
#     product_revenue = product_revenue.sort_values(by=["product_line_nm", "quarter"])

#     product_revenue["prev_quarter_revenue"] = product_revenue.groupby("product_line_nm")["net_revenue"].shift(1)

#     # Calculate the percentage growth by comparing current revenue to the previous year's revenue
#     product_revenue["quarter_growth_product"] = (
#         (product_revenue["net_revenue"] - product_revenue["prev_quarter_revenue"])
#         / product_revenue["prev_quarter_revenue"]
#         * 100
#     )
#     # Merge the growth back to the original DataFrame
#     df = df.merge(
#         product_revenue[["product_line_nm", "quarter", "quarter_growth_product"]],
#         on=["product_line_nm", "quarter"],
#         how="left",
#     )

#     return df


# def compute_quarter(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Computes the quarter.

#     Parameters:
#         df (pd.DataFrame): Input DataFrame.

#     Returns:
#         pd.DataFrame: DataFrame with 'Quarter' column added.
#     """
#     quarter = df['quarter']
#     df["Quarter"] = quarter

#     return df['Quarter']


def compute_avg_days_to_invoice(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the average number of days to invoice date from the end of quarter date.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'avg_days_to_invoice' column added.
    """
    quarter_end_map = {1: "01-01", 2: "04-01", 3: "07-01", 4: "10-01"}
    df.dropna(subset=["invoice_date"], inplace=True)
    df["quarter_start_date"] = (
        pd.to_datetime(df["invoice_date"]).dt.year.astype(int).astype(str) + "-" + df["quarter"].map(quarter_end_map)
    )
    df['quarter_start_date'] = pd.to_datetime(df["quarter_start_date"])
    df['invoice_date'] = pd.to_datetime(df["invoice_date"])

    # Calculate the difference in days
    df["days_difference"] = (df["invoice_date"] - df["quarter_start_date"]).dt.days

    # Group by 'quarter' and 'year' and calculate the average
    avg_days = df.groupby(['quarter', 'year'])['days_difference'].mean().round(2).reset_index()
    avg_days.rename(columns={'days_difference': 'avg_days_to_invoice'}, inplace=True)

    # Merge the average back to the original DataFrame
    df = df.merge(avg_days, on=["quarter", "year"], how="left")

    return df


def compute_avg_days_to_bill_effective(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the average number of days to billing effective date from the end of quarter date.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'avg_days_to_billing_effective' column added.
    """
    quarter_end_map = {1: "01-01", 2: "04-01", 3: "07-01", 4: "10-01"}
    df.dropna(subset=["bill_effective_dt"], inplace=True)
     # Create a new column for the quarter end date
    df["quarter_start_date"] = (
        pd.to_datetime(df["bill_effective_dt"]).dt.year.astype(int).astype(str) + "-" + df["quarter"].map(quarter_end_map)
    )

    # Convert to datetime
    df["quarter_start_date"] = pd.to_datetime(df["quarter_start_date"])
    df["bill_effective_dt"] = pd.to_datetime(df["bill_effective_dt"])

    # Calculate the difference in days
    df["days_difference"] = abs(df["bill_effective_dt"] - df["quarter_start_date"]).dt.days

    # Group by 'quarter' and 'year' and calculate the average
    avg_days = df.groupby(['quarter', 'year'])['days_difference'].mean().round(2).reset_index()
    avg_days.rename(columns={'days_difference': 'avg_days_to_bill_effective'}, inplace=True)

    # Merge the average back to the original DataFrame
    df = df.merge(avg_days, on=["quarter", "year"], how="left")

    return df


def compute_avg_days_to_cv_effective_dt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the average number of days to coverage effective date from the end of quarter date.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'avg_days_to_cv_effective' column added.
    """
    quarter_end_map = {1: "01-01", 2: "04-01", 3: "07-01", 4: "10-01"}
    df.dropna(subset=["cv_effective_dt"], inplace=True)
    df["quarter_start_date"] = (
        pd.to_datetime(df["cv_effective_dt"]).dt.year.astype(int).astype(str) + "-" + df["quarter"].map(quarter_end_map)
    )
    
    # Convert to datetime
    df["quarter_start_date"] = pd.to_datetime(df["quarter_start_date"])
    df["cv_effective_dt"] = pd.to_datetime(df["cv_effective_dt"])

    # Calculate the difference in days
    df["days_difference"] = abs(df["cv_effective_dt"] - df["quarter_start_date"]).dt.days

    # Group by 'quarter' and 'year' and calculate the average
    avg_days = df.groupby(['quarter', 'year'])['days_difference'].mean().round(2).reset_index()
    avg_days.rename(columns={'days_difference': 'avg_days_to_cv_effective'}, inplace=True)

    # Merge the average back to the original DataFrame
    df = df.merge(avg_days, on=["quarter", "year"], how="left")

    return df

def compute_avg_days_to_cv_exp_dt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the average number of date to coverage expiration date from the end of quarter date.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'avg_days_to_cv_exp' column added.
    """
    quarter_end_map = {1: "01-01", 2: "04-01", 3: "07-01", 4: "10-01"}
    df.dropna(subset=["cv_expiration_dt"], inplace=True)
    df["quarter_start_date"] = (
        pd.to_datetime(df["cv_expiration_dt"]).dt.year.astype(int).astype(str) + "-" + df["quarter"].map(quarter_end_map)
    )
    
    # Convert to datetime
    df["quarter_start_date"] = pd.to_datetime(df["quarter_start_date"])
    df["cv_expiration_dt"] = pd.to_datetime(df["cv_expiration_dt"])

    # Calculate the difference in days
    df["days_difference"] = abs(df["cv_expiration_dt"] - df["quarter_start_date"]).dt.days

    # Group by 'quarter' and 'year' and calculate the average
    avg_days = df.groupby(['quarter', 'year'])['days_difference'].mean().round(2).reset_index()
    avg_days.rename(columns={'days_difference': 'avg_days_to_cv_exp'}, inplace=True)

    # Merge the average back to the original DataFrame
    df = df.merge(avg_days, on=["quarter", "year"], how="left")

    return df

def compute_non_recurring_percentage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the current quarter's percentage of non recurring revenue by dividing non-recurring revenue by new business
    revenue.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'client_nr_percentage' column added.
    """

    df["non_recurring_revenue"] = df["net_revenue"].where(df["non_recurring_flag"], 0)
    df["new_business_revenue"] = df["net_revenue"].where(df["production_code"].isin(["New business", "Expanded services"]), 0)

    df_quarter = df.groupby("quarter").agg({"non_recurring_revenue": "sum", "new_business_revenue": "sum"}).reset_index()

    # Calculate the client_nr_percentage
    df_quarter["client_nr_percentage"] = (df_quarter["non_recurring_revenue"] / df_quarter["new_business_revenue"]) * 100

    # # Merge the client_nr_percentage column back to the original dataframe
    df = pd.merge(df, df_quarter[["quarter", "client_nr_percentage"]], on="quarter", how="left")

    return df


def compute_avg_stickiness(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the average client stickiness.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'avg_client_stickiness' column added.
    """
    # get total number of product lines
    total_prod_lines = df["product_line_nm"].nunique()

    # Count number of unique product lines per period and per company number
    unique_product_lines = df.groupby(["company_number", "quarter"])["product_line_nm"].nunique().reset_index(name='unique_product_lines')

    # Calculate the percentage stickiness by dividing number of company lines by total lines
    unique_product_lines["client_avg_stickiness"] = (unique_product_lines['unique_product_lines'] / total_prod_lines).round(2)
    
    # merge back to original df
    df = df.merge(unique_product_lines, on=["company_number", "quarter"], how='left')

    return df


def compute_renewal_rate(df: pd.DataFrame, grouping_columns: List[str]) -> pd.DataFrame:
    """
    Computes the Renewal Ratio within the data processing step.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        grouping_columns (List[str]): Columns to group by.

    Returns:
        pd.DataFrame: DataFrame with 'renewal_rate_ratio' column added.
    """
    # Ensure necessary columns are present
    required_columns = ["year", "quarter", "net_revenue", "production_code"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame.")

    # Normalize 'production_code'
    df["production_code_normalized"] = df["production_code"].str.lower().str.strip()
    renewal_codes = ["renewal"]

    # Compute renewal revenue
    df["renewal_revenue"] = df["net_revenue"].where(df["production_code_normalized"].isin(renewal_codes), 0)

    #######
    # Aggregate renewal revenue by year and quarter
    renewal_revenue_total = df.groupby(grouping_columns + ["year", "quarter"])["renewal_revenue"].sum().reset_index()
    renewal_revenue_total.rename(columns={"renewal_revenue": "total_renewal_revenue"}, inplace=True)

    # Merge the total renewal revenue back into the original DataFrame
    df = df.merge(renewal_revenue_total, on=grouping_columns + ["year", "quarter"], how="left")
    ##############

    # Compute total revenue per group, year, and quarter
    total_revenue = df.groupby(grouping_columns + ["year", "quarter"])["net_revenue"].sum().reset_index()
    total_revenue.rename(columns={"net_revenue": "total_revenue"}, inplace=True)

    # Shift total revenue by 1 year to get last year's same quarter total revenue
    total_revenue["year"] = total_revenue["year"] + 1  # Shift forward by 1 year

    # Merge shifted total revenue back into the original DataFrame
    df = df.merge(total_revenue, on=grouping_columns + ["year", "quarter"], how="left")

    # Get the grouping columns as a string to use in the new column name
    grouping_str = '_'.join(grouping_columns)
    grouping_str = grouping_str.replace('client_number', 'client').replace('product_line_nm', 'product')

    # Calculate the Renewal Revenue Ratio -- ###########
    df[f"renewal_revenue_ratio_{grouping_str}"] = (df["total_renewal_revenue"].round(0) / df["total_revenue"].round(0)).round(2)

    # Handle division by zero or missing values
    df[f"renewal_revenue_ratio_{grouping_str}"].replace([np.inf, -np.inf], np.nan, inplace=True)
    df[f"renewal_revenue_ratio_{grouping_str}"].fillna(0, inplace=True)

    # Calculate the standard deviation of the renewal ratio rate

    # Apply threshold
    df[f"renewal_revenue_ratio_{grouping_str}"] = np.where(
    df[f"renewal_revenue_ratio_{grouping_str}"] > 20,
    20, # Set to 20 if above 20
    np.where(
        df[f"renewal_revenue_ratio_{grouping_str}"] < -20,
        -20,  # Set to -20 if below -20
        df[f"renewal_revenue_ratio_{grouping_str}"]
    )# Set to mean + std_dev if above upper threshold
    )
    return df

def compute_qq_growth(df: pd.DataFrame, grouping_columns: List[str]) -> pd.DataFrame:
    """
    Computes the QoQ growth in 'net_revenue' within the data processing step.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        grouping_columns (List[str]): Columns to group by.

    Returns:
        pd.DataFrame: DataFrame with 'qq_growth_rate' column added.
    """
    # Ensure necessary columns are present
    required_columns = ["quarter", "net_revenue"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame.")

    # Sum 'net_revenue' per product per period
    product_revenue = df.groupby(grouping_columns + ["year", "quarter"])["net_revenue"].sum().reset_index()
    product_revenue.rename(columns={"net_revenue": "current_revenue"}, inplace=True)
    df = df.merge(product_revenue, on=grouping_columns + ["year", "quarter"], how="left")

    # Get previous quarter's revenue
    product_revenue = df.groupby(grouping_columns + ["year", "quarter"])["net_revenue"].sum().reset_index()
    product_revenue.rename(columns={"net_revenue": "prev_quarter_revenue"}, inplace=True)

    # Shift total revenue by 1 year to get last year's same quarter total revenue
    product_revenue["quarter"] = product_revenue["quarter"] + 1
    df = df.merge(product_revenue, on=grouping_columns + ["year", "quarter"], how="left")
    # Get the grouping columns as a string to use in the new column name
    grouping_str = '_'.join(grouping_columns)
    grouping_str = grouping_str.replace('client_number', 'client').replace('product_line_nm', 'product')

    # Round revenues to nearest whole number
    df["prev_quarter_revenue"] = df["prev_quarter_revenue"].round(0)
    df["current_revenue"] = df["current_revenue"].round(0)

    # Calculate the percentage growth by comparing current revenue to the previous quarter's revenue
    df[f"qq_growth_rate_{grouping_str}"] = (
        (df["current_revenue"].round(0) - df["prev_quarter_revenue"].round(0))
        / (abs(df["prev_quarter_revenue"].round(0)))
        * 100
    ).round(2)
    
    # Handle division by zero or missing values
    df[f"qq_growth_rate_{grouping_str}"].replace([np.inf, -np.inf], np.nan, inplace=True)
    df[f"qq_growth_rate_{grouping_str}"].fillna(0, inplace=True)

    # Apply threshold
    df[f"qq_growth_rate_{grouping_str}"] = np.where(
    df[f"qq_growth_rate_{grouping_str}"] > 200,
    200,  # Set to 200 if above 200
    np.where(
        df[f"qq_growth_rate_{grouping_str}"] < -200,
        -200,  # Set to -200 if below -200
        df[f"qq_growth_rate_{grouping_str}"]  # Keep original value if within thresholds
    )
    )
    return df


def compute_yy_growth(df: pd.DataFrame, grouping_columns: List[str]) -> pd.DataFrame:
    """
    Computes the YoY growth in 'net_revenue' within the data processing step.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        grouping_columns (List[str]): Columns to group by.

    Returns:
        pd.DataFrame: DataFrame with 'yy_growth_rate' column added.
    """
    # Ensure necessary columns are present
    required_columns = ["quarter", "net_revenue"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame.")

    # Sum 'net_revenue' per product per period
    curr_rev = df.groupby(grouping_columns + ["year", "quarter"])["net_revenue"].sum().reset_index()
    curr_rev.rename(columns={"net_revenue": "curr_rev"}, inplace=True)
    df = df.merge(curr_rev, on=grouping_columns + ["year", "quarter"], how="left")

    # Get previous quarter's revenue
    prev_rev = df.groupby(grouping_columns + ["year", "quarter"])["net_revenue"].sum().reset_index()
    prev_rev.rename(columns={"net_revenue": "prev_revenue"}, inplace=True)

    # Shift total revenue by 1 year to get last year's same quarter total revenue
    prev_rev["year"] = prev_rev["year"] + 1
    df = df.merge(prev_rev, on=grouping_columns + ["year", "quarter"], how="left")
    
    # Get the grouping columns as a string to use in the new column name
    grouping_str = '_'.join(grouping_columns)
    grouping_str = grouping_str.replace('client_number', 'client').replace('product_line_nm', 'product')

    # Round revenues to nearest whole number
    df["prev_revenue"] = df["prev_revenue"].round(0)
    df["curr_rev"] = df["curr_rev"].round(0)

    # Calculate the percentage growth by comparing current revenue to the previous quarter's revenue
    df[f"yy_growth_rate_{grouping_str}"] = (
        (df["curr_rev"].round(0) - df["prev_revenue"].round(0))
        / (abs(df["prev_revenue"].round(0)))
        * 100
    ).round(2)

    # Handle division by zero or missing values
    df[f"yy_growth_rate_{grouping_str}"].replace([np.inf, -np.inf], np.nan, inplace=True)
    df[f"yy_growth_rate_{grouping_str}"].fillna(0, inplace=True)

    # Apply threshold
    df[f"yy_growth_rate_{grouping_str}"] = np.where(
    df[f"yy_growth_rate_{grouping_str}"] > 200,
    200,  # Set to 200 if above 200
    np.where(
        df[f"yy_growth_rate_{grouping_str}"] < -200,
        -200,  # Set to -200 if below -200
        df[f"yy_growth_rate_{grouping_str}"]  # Keep original value if within thresholds
    )
    )
    return df


# def compute_qq_growth(df: pd.DataFrame, grouping_columns: List[str]) -> pd.DataFrame:
#     """
#     Computes the QoQ growth in 'net_revenue' within the data processing step.


#     Parameters:
#         df (pd.DataFrame): Input DataFrame.
#         grouping_columns (List[str]): Columns to group by.

#     Returns:
#         pd.DataFrame: DataFrame with 'qq_growth_rate' column added.
#     """
#     # Ensure necessary columns are present
#     required_columns = ["quarter", "net_revenue"]
#     for col in required_columns:
#         if col not in df.columns:
#             raise ValueError(f"Column '{col}' not found in DataFrame.")

#     # Sum 'net_revenue' per product per period
#     product_revenue = df.groupby(grouping_columns + ["year", "quarter"])["net_revenue"].sum().reset_index()

#     # Sort for correct diff calculation
#     product_revenue = product_revenue.sort_values(by=grouping_columns + ["year", "quarter"])

#     # Get previous quarter's revenue
#     product_revenue["prev_quarter_revenue"] = product_revenue.groupby(grouping_columns)["net_revenue"].shift(1)

#     # Get the grouping columns as a string to use in the new column name
#     grouping_str = '_'.join(grouping_columns)
#     grouping_str = grouping_str.replace('client_number', 'client').replace('product_line_nm', 'product')

#     # Round revenues to nearest whole number
#     product_revenue["net_revenue"] = product_revenue["net_revenue"].round(0)
#     product_revenue["prev_quarter_revenue"] = product_revenue["prev_quarter_revenue"].round(0)

#     # Calculate the percentage growth by comparing current revenue to the previous quarter's revenue
#     product_revenue[f"qq_growth_rate_{grouping_str}"] = (
#         (product_revenue["net_revenue"] - product_revenue["prev_quarter_revenue"])
#         / (abs(product_revenue["prev_quarter_revenue"]))
#         * 100
#     ).round(2)

#     # Merge the growth back to the original DataFrame
#     df = df.merge(
#         product_revenue[grouping_columns + ["quarter", "year", f"qq_growth_rate_{grouping_str}"]],
#         on=grouping_columns + ["quarter", "year"],
#         how="left",
#     )

#     # Handle division by zero or missing values
#     df[f"qq_growth_rate_{grouping_str}"].replace([np.inf, -np.inf], np.nan, inplace=True)
#     df[f"qq_growth_rate_{grouping_str}"].fillna(0, inplace=True)

# #     return df

# def compute_yy_growth(df: pd.DataFrame, grouping_columns: List[str]) -> pd.DataFrame:
#     """
#     Computes the YoY growth in 'net_revenue' within the data processing step.

#     Parameters:
#         df (pd.DataFrame): Input DataFrame.
#         grouping_columns (List[str]): Columns to group by.

#     Returns:
#         pd.DataFrame: DataFrame with 'yy_growth_rate' column added.
#     """
#     # Ensure necessary columns are present
#     required_columns = ["year", "quarter", "net_revenue"]
#     for col in required_columns:
#         if col not in df.columns:
#             raise ValueError(f"Column '{col}' not found in DataFrame.")

#     # Sum 'net_revenue' per product per period
#     product_revenue = df.groupby(grouping_columns + ["year", "quarter"])["net_revenue"].sum().reset_index()

#     # Sort for correct diff calculation
#     product_revenue = product_revenue.sort_values(by=grouping_columns + ["year", "quarter"])

#     # Get previous year quarter's revenue
#     product_revenue["prev_quarter_revenue"] = product_revenue.groupby(grouping_columns)["net_revenue"].shift(4)
    
#     # Get the grouping columns as a string to use in the new column name
#     grouping_str = '_'.join(grouping_columns)
#     grouping_str = grouping_str.replace('client_number', 'client').replace('product_line_nm', 'product')

#     # Round revenues to nearest whole number
#     product_revenue["net_revenue"] = product_revenue["net_revenue"].round(0)
#     product_revenue["prev_quarter_revenue"] = product_revenue["prev_quarter_revenue"].round(0)
    
#     # Calculate the percentage growth by comparing current revenue to the previous quarter's revenue
#     product_revenue[f"yy_growth_rate_{grouping_str}"] = (
#         (product_revenue["net_revenue"] - product_revenue["prev_quarter_revenue"]) / abs(product_revenue["prev_quarter_revenue"])
#         * 100
#     ).round(2)


#     # Merge the growth back to the original DataFrame
#     df = df.merge(
#         product_revenue[grouping_columns + ["quarter", "year", f"yy_growth_rate_{grouping_str}"]],
#         on=grouping_columns + ["quarter", "year"],
#         how="left",
#     )
#         # Handle division by zero or missing values
#     df[f"yy_growth_rate_{grouping_str}"].replace([np.inf, -np.inf], np.nan, inplace=True)
#     df[f"yy_growth_rate_{grouping_str}"].fillna(0, inplace=True)

#     return df

def compute_num_subproducts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the number of subproducts within a client.

    Parameters:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'num_client_subproducts' column added.
    """

    num_client_subproducts = df.groupby(['client_number', 'quarter'])["product_subgroup_nm"].nunique().reset_index()
    num_client_subproducts.rename(columns={"product_subgroup_nm": "num_client_subproducts"}, inplace=True)
    df = df.merge(num_client_subproducts, on=['client_number', 'quarter'], how='left')
    return df
