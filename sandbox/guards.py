"""Custom guards for attribute and import access control."""

import pandas as pd
import numpy as np
from RestrictedPython.Eval import default_guarded_getattr
from RestrictedPython.Guards import guarded_iter_unpack_sequence, guarded_unpack_sequence

# Allowed modules with their references
ALLOWED_IMPORTS = {
    "pandas": pd,
    "pd": pd,
    "numpy": np,
    "np": np,
    "datetime": __import__("datetime"),
    "math": __import__("math"),
}

# Blocked attributes across all objects (security-critical)
BLOCKED_ATTRIBUTES = {
    # Class introspection
    "__class__",
    "__bases__",
    "__subclasses__",
    "__mro__",
    # Code access
    "__globals__",
    "__code__",
    "__closure__",
    "__func__",
    "__self__",
    # Dict access for escaping
    "__dict__",
    "__slots__",
    "__module__",
    # Attribute manipulation
    "__delattr__",
    "__setattr__",
    "__getattribute__",
    # Serialization (escape vectors)
    "__reduce__",
    "__reduce_ex__",
    "__getstate__",
    "__setstate__",
    # Context managers (potential resource abuse)
    "__enter__",
    "__exit__",
    # Async (not allowed)
    "__await__",
    "__aenter__",
    "__aexit__",
    "__aiter__",
    "__anext__",
}

# Safe dunder methods that are allowed
SAFE_DUNDERS = {
    "__len__",
    "__iter__",
    "__getitem__",
    "__contains__",
    "__str__",
    "__repr__",
    "__bool__",
    "__eq__",
    "__ne__",
    "__lt__",
    "__le__",
    "__gt__",
    "__ge__",
    "__hash__",
    "__add__",
    "__sub__",
    "__mul__",
    "__truediv__",
    "__floordiv__",
    "__mod__",
    "__pow__",
    "__neg__",
    "__pos__",
    "__abs__",
}

# Blocked DataFrame methods (file I/O, network)
BLOCKED_DF_METHODS = {
    # File output
    "to_pickle",
    "to_parquet",
    "to_sql",
    "to_excel",
    "to_csv",
    "to_json",
    "to_html",
    "to_latex",
    "to_feather",
    "to_stata",
    "to_gbq",
    "to_hdf",
    "to_clipboard",
    "to_markdown",
    "to_xml",
    # File input (shouldn't be accessible but block anyway)
    "read_pickle",
    "read_parquet",
    "read_sql",
    "read_excel",
    "read_csv",
    "read_json",
    "read_html",
    "read_feather",
    "read_stata",
    "read_hdf",
    "read_clipboard",
    "read_xml",
}


def guarded_getattr(obj, name):
    """
    Custom getattr guard that blocks dangerous attribute access.

    This is a critical security function that prevents:
    - Access to dunder methods that could escape the sandbox
    - Access to DataFrame I/O methods
    - Access to code objects and globals
    """
    # Block all dunder attributes except safe ones
    if name.startswith("_"):
        if name.startswith("__") and name.endswith("__"):
            if name not in SAFE_DUNDERS:
                raise AttributeError(
                    f"Access to '{name}' is not allowed for security reasons"
                )
        elif name.startswith("_"):
            # Single underscore - private attributes blocked
            raise AttributeError(
                f"Access to private attribute '{name}' is not allowed"
            )

    # Block explicitly dangerous attributes
    if name in BLOCKED_ATTRIBUTES:
        raise AttributeError(
            f"Access to '{name}' is not allowed for security reasons"
        )

    # Block DataFrame I/O operations
    if isinstance(obj, (pd.DataFrame, pd.Series)) and name in BLOCKED_DF_METHODS:
        raise AttributeError(
            f"DataFrame.{name}() is not allowed - data output is handled by the system"
        )

    # Use default guarded getattr for additional checks
    return default_guarded_getattr(obj, name)


def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    """
    Custom import guard that only allows whitelisted modules.

    Only pandas, numpy, datetime, and math are allowed.
    """
    # Check base module name
    base_module = name.split(".")[0]

    if base_module not in ALLOWED_IMPORTS:
        allowed_list = ", ".join(sorted(set(ALLOWED_IMPORTS.keys()) - {"pd", "np"}))
        raise ImportError(
            f"Import of '{name}' is not allowed. Allowed modules: {allowed_list}"
        )

    return ALLOWED_IMPORTS[base_module]


def guarded_write(obj):
    """
    Guard for write operations.

    Allows modifications to pandas/numpy objects which are needed for transforms.
    """
    # Allow pandas and numpy operations
    if isinstance(obj, (pd.DataFrame, pd.Series, np.ndarray, list, dict, set)):
        return obj
    return obj


# Export guards for use in restricted execution
__all__ = [
    "guarded_getattr",
    "guarded_import",
    "guarded_write",
    "guarded_iter_unpack_sequence",
    "guarded_unpack_sequence",
    "ALLOWED_IMPORTS",
]
