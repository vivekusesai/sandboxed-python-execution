"""
Example: Filter rows by condition

This script demonstrates filtering rows based on a condition.
"""


def transform(df):
    """Keep only rows where price is greater than 20."""
    df = df[df["price"] > 20]
    return df
