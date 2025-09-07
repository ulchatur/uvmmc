from datetime import datetime


def get_now_timestamp() -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    return timestamp

def get_day_month_yr() -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d")
    return timestamp
