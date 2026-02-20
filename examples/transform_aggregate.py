"""
Example: Aggregate data with groupby

This script demonstrates grouping and aggregating data.
"""

import pandas as pd


def transform(df):
    """Group by region and calculate total sales."""
    # Calculate total for each row first
    df["total"] = df["price"] * df["qty"]

    # Group by region and sum
    result = df.groupby("region").agg({
        "total": "sum",
        "qty": "sum",
        "price": "mean"
    }).reset_index()

    # Rename columns for clarity
    result.columns = ["region", "total_sales", "total_qty", "avg_price"]

    return result
