"""
Example: Handle null/missing values

This script demonstrates cleaning null values in a DataFrame.
"""


def transform(df):
    """Clean null values: fill numeric with 0, strings with 'Unknown'."""
    # Get column types
    for col in df.columns:
        if df[col].dtype in ["float64", "int64"]:
            # Fill numeric nulls with 0
            df[col] = df[col].fillna(0)
        else:
            # Fill string nulls with 'Unknown'
            df[col] = df[col].fillna("Unknown")

    return df
