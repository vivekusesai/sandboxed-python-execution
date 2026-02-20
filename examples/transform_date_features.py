"""
Example: Extract date features

This script demonstrates extracting date components from a date column.
"""

import pandas as pd


def transform(df):
    """Extract year, month, and day of week from signup_date."""
    # Convert to datetime if not already
    df["signup_date"] = pd.to_datetime(df["signup_date"])

    # Extract components
    df["signup_year"] = df["signup_date"].dt.year
    df["signup_month"] = df["signup_date"].dt.month
    df["signup_day_of_week"] = df["signup_date"].dt.day_name()

    return df
