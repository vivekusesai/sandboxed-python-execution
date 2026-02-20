"""RestrictedPython compiler for safe code execution."""

import ast
from typing import Optional, Tuple

from RestrictedPython import compile_restricted
from RestrictedPython.Eval import default_guarded_getitem

from sandbox.guards import (
    guarded_getattr,
    guarded_import,
    guarded_write,
    guarded_iter_unpack_sequence,
    guarded_unpack_sequence,
)
from sandbox.safe_builtins import SAFE_BUILTINS


class RestrictedCompiler:
    """
    Compiles user code with RestrictedPython safety measures.

    This implements Layer 1 of the sandbox strategy:
    - Pre-validation of code for dangerous patterns
    - AST analysis to block dangerous constructs
    - RestrictedPython compilation with custom guards
    """

    # Dangerous import patterns to block
    BLOCKED_IMPORTS = {
        "os",
        "sys",
        "subprocess",
        "socket",
        "ctypes",
        "multiprocessing",
        "threading",
        "asyncio",
        "concurrent",
        "signal",
        "shutil",
        "tempfile",
        "glob",
        "fnmatch",
        "pathlib",
        "io",
        "builtins",
        "__builtins__",
        "importlib",
        "pkgutil",
        "code",
        "codeop",
        "compile",
        "dis",
        "inspect",
        "gc",
        "weakref",
        "pickle",
        "shelve",
        "marshal",
        "dbm",
        "sqlite3",
        "ssl",
        "http",
        "urllib",
        "ftplib",
        "smtplib",
        "email",
        "xml",
        "html",
        "webbrowser",
        "cmd",
        "pdb",
        "profile",
        "timeit",
        "trace",
        "platform",
        "getpass",
        "pty",
        "tty",
        "termios",
        "fcntl",
        "select",
        "mmap",
        "resource",
        "sysconfig",
        "warnings",
        "logging",
    }

    def compile_code(self, code: str) -> Tuple[Optional[object], Optional[str]]:
        """
        Compile user code with RestrictedPython.

        Returns:
            Tuple of (compiled_code, error_message)
            If successful: (code_object, None)
            If failed: (None, error_string)
        """
        # Step 1: Pre-validation
        error = self._pre_validate(code)
        if error:
            return None, error

        # Step 2: Wrap code to ensure transform function exists
        wrapped_code = self._wrap_transform_function(code)

        # Step 3: Compile with RestrictedPython
        try:
            result = compile_restricted(
                wrapped_code,
                filename="<user_script>",
                mode="exec",
            )

            # Check for compilation errors
            if result.errors:
                return None, "\n".join(result.errors)

            return result.code, None

        except SyntaxError as e:
            return None, f"Syntax error at line {e.lineno}: {e.msg}"
        except Exception as e:
            return None, f"Compilation error: {str(e)}"

    def _pre_validate(self, code: str) -> Optional[str]:
        """
        Pre-validation checks before compilation.

        Checks for dangerous patterns that should be blocked
        before even attempting to compile.
        """
        # Check for dangerous string patterns
        dangerous_patterns = [
            ("__import__", "Dynamic imports are not allowed"),
            ("importlib", "importlib is not allowed"),
            ("exec(", "exec() is not allowed"),
            ("eval(", "eval() is not allowed"),
            ("compile(", "compile() is not allowed"),
            ("open(", "File operations are not allowed"),
            ("globals(", "globals() is not allowed"),
            ("locals(", "locals() is not allowed"),
            ("vars(", "vars() is not allowed"),
            ("getattr(", "getattr() is not allowed - use direct attribute access"),
            ("setattr(", "setattr() is not allowed"),
            ("delattr(", "delattr() is not allowed"),
            ("__builtins__", "Access to __builtins__ is not allowed"),
            (".__class__", "Access to __class__ is not allowed"),
            (".__bases__", "Access to __bases__ is not allowed"),
            (".__subclasses__", "Access to __subclasses__ is not allowed"),
            (".__globals__", "Access to __globals__ is not allowed"),
            (".__code__", "Access to __code__ is not allowed"),
        ]

        code_lower = code.lower()
        for pattern, message in dangerous_patterns:
            if pattern.lower() in code_lower:
                return message

        # Parse AST to check for blocked constructs
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return f"Syntax error: {e.msg}"

        # Walk AST to check for dangerous constructs
        for node in ast.walk(tree):
            # Block async constructs
            if isinstance(node, (ast.AsyncFunctionDef, ast.AsyncFor, ast.AsyncWith)):
                return "Async constructs are not allowed"

            # Block await expressions
            if isinstance(node, ast.Await):
                return "Await expressions are not allowed"

            # Check imports
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module = alias.name.split(".")[0]
                    if module in self.BLOCKED_IMPORTS:
                        return f"Import of '{alias.name}' is not allowed"
                    if module not in {"pandas", "numpy", "datetime", "math"}:
                        return f"Import of '{alias.name}' is not allowed. Only pandas, numpy, datetime, and math are permitted."

            if isinstance(node, ast.ImportFrom):
                if node.module:
                    module = node.module.split(".")[0]
                    if module in self.BLOCKED_IMPORTS:
                        return f"Import from '{node.module}' is not allowed"
                    if module not in {"pandas", "numpy", "datetime", "math"}:
                        return f"Import from '{node.module}' is not allowed. Only pandas, numpy, datetime, and math are permitted."

            # Block dangerous function calls
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in {"exec", "eval", "compile", "open", "__import__"}:
                        return f"{node.func.id}() is not allowed"

        return None

    def _wrap_transform_function(self, code: str) -> str:
        """
        Ensure code has proper transform function structure.

        If code doesn't define a transform function, wrap it in one.
        """
        # Check if transform function is already defined
        if "def transform" in code:
            return code

        # Wrap code in transform function
        # Indent all lines by 4 spaces
        indented_code = "\n".join(
            f"    {line}" if line.strip() else line
            for line in code.split("\n")
        )

        return f"""
def transform(df):
{indented_code}
    return df
"""

    def get_restricted_globals(self) -> dict:
        """
        Get the restricted globals dictionary for execution.

        This sets up the security guards that will be used at runtime.
        """
        return {
            "__builtins__": SAFE_BUILTINS,
            "_getattr_": guarded_getattr,
            "_getitem_": default_guarded_getitem,
            "_getiter_": iter,
            "_iter_unpack_sequence_": guarded_iter_unpack_sequence,
            "_unpack_sequence_": guarded_unpack_sequence,
            "_write_": guarded_write,
            "__import__": guarded_import,
            # Provide allowed modules directly
            "pd": __import__("pandas"),
            "pandas": __import__("pandas"),
            "np": __import__("numpy"),
            "numpy": __import__("numpy"),
            "datetime": __import__("datetime"),
            "math": __import__("math"),
        }
