"""Tests for RestrictedPython compilation."""

import pytest

from sandbox.restricted_compiler import RestrictedCompiler


class TestRestrictedCompiler:
    """Test RestrictedPython compilation."""

    @pytest.fixture
    def compiler(self):
        return RestrictedCompiler()

    def test_valid_transform_compiles(self, compiler):
        """Valid transform function should compile."""
        code = """
def transform(df):
    df["new_col"] = df["col1"] + df["col2"]
    return df
"""
        compiled, error = compiler.compile_code(code)
        assert error is None
        assert compiled is not None

    def test_simple_expression_wrapped(self, compiler):
        """Simple expression should be wrapped in transform function."""
        code = 'df["total"] = df["price"] * df["qty"]'
        compiled, error = compiler.compile_code(code)
        assert error is None
        assert compiled is not None

    def test_import_pandas_allowed(self, compiler):
        """Import pandas should be allowed."""
        code = """
import pandas as pd

def transform(df):
    return df
"""
        compiled, error = compiler.compile_code(code)
        assert error is None

    def test_import_numpy_allowed(self, compiler):
        """Import numpy should be allowed."""
        code = """
import numpy as np

def transform(df):
    return df
"""
        compiled, error = compiler.compile_code(code)
        assert error is None


class TestBlockedImports:
    """Test that dangerous imports are blocked."""

    @pytest.fixture
    def compiler(self):
        return RestrictedCompiler()

    @pytest.mark.parametrize("import_statement", [
        "import os",
        "import sys",
        "import subprocess",
        "import socket",
        "import ctypes",
        "import multiprocessing",
        "import threading",
        "from os import system",
        "from subprocess import call",
        "import shutil",
        "import pathlib",
    ])
    def test_dangerous_imports_blocked(self, compiler, import_statement):
        """Dangerous imports should be blocked."""
        code = f"""
{import_statement}

def transform(df):
    return df
"""
        compiled, error = compiler.compile_code(code)
        assert error is not None
        assert "not allowed" in error.lower()


class TestBlockedBuiltins:
    """Test that dangerous builtins are blocked."""

    @pytest.fixture
    def compiler(self):
        return RestrictedCompiler()

    @pytest.mark.parametrize("builtin_call", [
        "eval('1+1')",
        "exec('x=1')",
        "compile('x=1', '<string>', 'exec')",
        "open('file.txt')",
        "__import__('os')",
        "globals()",
        "locals()",
    ])
    def test_dangerous_builtins_blocked(self, compiler, builtin_call):
        """Dangerous builtins should be blocked."""
        code = f"""
def transform(df):
    {builtin_call}
    return df
"""
        compiled, error = compiler.compile_code(code)
        assert error is not None


class TestBlockedAttributes:
    """Test that dangerous attribute access is blocked."""

    @pytest.fixture
    def compiler(self):
        return RestrictedCompiler()

    @pytest.mark.parametrize("attribute_access", [
        "df.__class__",
        "df.__class__.__bases__",
        "().__class__.__bases__[0]",
        "transform.__globals__",
        "transform.__code__",
    ])
    def test_dunder_access_blocked(self, compiler, attribute_access):
        """Dangerous dunder access should be blocked."""
        code = f"""
def transform(df):
    x = {attribute_access}
    return df
"""
        compiled, error = compiler.compile_code(code)
        assert error is not None
