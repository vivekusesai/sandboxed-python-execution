"""Curated safe builtins for restricted execution."""

from RestrictedPython import safe_builtins


def get_safe_builtins() -> dict:
    """
    Return a curated set of safe builtins for sandbox execution.

    This removes dangerous functions and adds safe alternatives.
    """
    # Start with RestrictedPython's safe_builtins
    builtins = dict(safe_builtins)

    # Remove potentially dangerous functions
    dangerous = [
        "compile",
        "eval",
        "exec",
        "open",
        "input",
        "__import__",
        "globals",
        "locals",
        "vars",
        "getattr",
        "setattr",
        "delattr",
        "hasattr",
        "memoryview",
        "bytearray",
        "breakpoint",
        "credits",
        "copyright",
        "license",
        "help",
        "quit",
        "exit",
    ]
    for name in dangerous:
        builtins.pop(name, None)

    # Add safe functions we want to allow
    safe_additions = {
        # Basic types
        "len": len,
        "range": range,
        "enumerate": enumerate,
        "zip": zip,
        "map": map,
        "filter": filter,
        "sorted": sorted,
        "reversed": reversed,
        # Math operations
        "min": min,
        "max": max,
        "sum": sum,
        "abs": abs,
        "round": round,
        "pow": pow,
        "divmod": divmod,
        # Boolean operations
        "all": all,
        "any": any,
        # Type constructors
        "list": list,
        "dict": dict,
        "set": set,
        "frozenset": frozenset,
        "tuple": tuple,
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "complex": complex,
        # Constants
        "None": None,
        "True": True,
        "False": False,
        # String operations
        "chr": chr,
        "ord": ord,
        "ascii": ascii,
        "repr": repr,
        "format": format,
        # Itertools-like
        "iter": iter,
        "next": next,
        "slice": slice,
        # Safe printing (captured by subprocess)
        "print": print,
        # Type checking (read-only)
        "type": type,
        "isinstance": isinstance,
        "issubclass": issubclass,
        "callable": callable,
        "id": id,
        "hash": hash,
    }
    builtins.update(safe_additions)

    return builtins


# Pre-computed safe builtins for performance
SAFE_BUILTINS = get_safe_builtins()
