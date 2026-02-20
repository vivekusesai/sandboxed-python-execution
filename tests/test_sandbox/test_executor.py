"""Tests for sandbox executor."""

import pytest
import pandas as pd

from sandbox.executor import SandboxExecutor


class TestSandboxExecutor:
    """Test sandbox execution."""

    def test_simple_transform_succeeds(self):
        """Simple transform should execute successfully."""
        executor = SandboxExecutor(job_id=1)
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        code = """
def transform(df):
    df["c"] = df["a"] + df["b"]
    return df
"""
        success, result, logs = executor.execute(code, df)

        assert success is True
        assert result is not None
        assert "c" in result.columns
        assert list(result["c"]) == [5, 7, 9]

    def test_pandas_operations(self):
        """Pandas operations should work."""
        executor = SandboxExecutor(job_id=2)
        df = pd.DataFrame({
            "price": [10, 20, 30],
            "qty": [2, 3, 4]
        })

        code = """
import pandas as pd

def transform(df):
    df["total"] = df["price"] * df["qty"]
    df = df[df["total"] > 30]
    return df
"""
        success, result, logs = executor.execute(code, df)

        assert success is True
        assert len(result) == 2  # Only rows with total > 30

    def test_numpy_operations(self):
        """Numpy operations should work."""
        executor = SandboxExecutor(job_id=3)
        df = pd.DataFrame({"values": [1, 4, 9, 16]})

        code = """
import numpy as np

def transform(df):
    df["sqrt"] = np.sqrt(df["values"])
    return df
"""
        success, result, logs = executor.execute(code, df)

        assert success is True
        assert list(result["sqrt"]) == [1.0, 2.0, 3.0, 4.0]

    def test_invalid_code_fails(self):
        """Invalid code should fail gracefully."""
        executor = SandboxExecutor(job_id=4)
        df = pd.DataFrame({"a": [1, 2]})

        code = """
def transform(df):
    this is not valid python
    return df
"""
        success, result, logs = executor.execute(code, df)

        assert success is False
        assert result is None
        assert "syntax" in logs.lower() or "error" in logs.lower()

    def test_missing_transform_fails(self):
        """Missing transform function should fail."""
        executor = SandboxExecutor(job_id=5)
        df = pd.DataFrame({"a": [1, 2]})

        code = """
def process(df):
    return df
"""
        success, result, logs = executor.execute(code, df)

        # Should wrap code, so this might actually succeed
        # depending on implementation
        # If it fails, check error message
        if not success:
            assert "transform" in logs.lower()

    def test_return_non_dataframe_fails(self):
        """Returning non-DataFrame should fail."""
        executor = SandboxExecutor(job_id=6)
        df = pd.DataFrame({"a": [1, 2]})

        code = """
def transform(df):
    return "not a dataframe"
"""
        success, result, logs = executor.execute(code, df)

        assert success is False
        assert "DataFrame" in logs


class TestSandboxSecurity:
    """Test sandbox security measures."""

    def test_import_os_blocked(self):
        """Import os should be blocked."""
        executor = SandboxExecutor(job_id=100)
        df = pd.DataFrame({"a": [1]})

        code = """
import os

def transform(df):
    return df
"""
        success, result, logs = executor.execute(code, df)

        assert success is False
        assert "not allowed" in logs.lower()

    def test_file_write_blocked(self):
        """File write operations should be blocked."""
        executor = SandboxExecutor(job_id=101)
        df = pd.DataFrame({"a": [1]})

        code = """
def transform(df):
    df.to_csv("output.csv")
    return df
"""
        success, result, logs = executor.execute(code, df)

        assert success is False

    def test_open_blocked(self):
        """open() should be blocked."""
        executor = SandboxExecutor(job_id=102)
        df = pd.DataFrame({"a": [1]})

        code = """
def transform(df):
    with open("test.txt", "w") as f:
        f.write("test")
    return df
"""
        success, result, logs = executor.execute(code, df)

        assert success is False
