import numpy as np
from sklearn.metrics import confusion_matrix
# Find most recent year (should always be 0) and quarter (0-3) within data
def get_last_year_quarter(df):
    """
    Get the quarter of the last year from a given DataFrame.
    Args:
        df (DataFrame): The DataFrame containing the data.
    Returns:
        str: The quarter of the last year in the format 'Q1', 'Q2', 'Q3', or 'Q4'.
"""
    
    # Initialize variables to track the most recent year and quarter
    most_recent_year = 10
    most_recent_quarter = -1

    # Iterate through the column names

    pattern = '.*_year_([0-9]\d*)_Q([1-4])$'
    # Filter the DataFrame to include only the matching columns
    no_adjust = [col for col in df.columns if 'adjustments_year_' not in col and col.count('year_') < 2]
    df_no_adjust = df[no_adjust]
    year_cols = df_no_adjust.filter(regex=pattern).columns
    for column in list(year_cols):
        # Split the column name to extract year and quarter
        parts = column.split('year_')
        year_quarter = parts[1]
        year = int(year_quarter[0])  # Extract the year part
        quarter = int(year_quarter[-1])  # Extract the quarter part (remove 'Q' and convert to int)

        # Check if this year is more recent
        if year < most_recent_year:
            most_recent_year = year
            most_recent_quarter = quarter  # Reset quarter to the current one
        elif year == most_recent_year:
            # If the year is the same, check for the greatest quarter
            if quarter > most_recent_quarter:
                most_recent_quarter = quarter

    # Output the most recent year and quarter
    return (most_recent_year,most_recent_quarter)

  
def get_preceding_quarters(year, quarter, num_quarters):
    """
    Generates a list of preceding year_quarters based on a year and quarter.
    Args:
        year (int): The current year.
        quarter (int): The current quarter (1-4).
        num_quarters (int): Number of preceding quarters to generate.

    Returns:
        list: A list of preceding quarters based on the input parameters.
    """
    preceding_quarters = []
    
    for _ in range(num_quarters):
        # Decrement the quarter
        if quarter == 1:
            quarter = 4
            year += 1  # Move to the next year
        else:
            quarter -= 1
        
        # Append the formatted string to the list
        preceding_quarters.append(f'year_{year}_Q{quarter}')
    
    return preceding_quarters


def find_best_weights(y_true, y_proba, weight_range=np.linspace(0.1, 10, 10), threshold_steps=1000):
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