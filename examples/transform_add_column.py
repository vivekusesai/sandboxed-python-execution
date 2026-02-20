"""
Example: Add a calculated column

This script demonstrates adding a new column based on existing columns.
"""


def transform(df):
    """Add a 'total' column by multiplying price and quantity."""
    df["total"] = df["price"] * df["qty"]
    return df
