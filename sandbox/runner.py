#!/usr/bin/env python
"""
Sandbox runner script - executed as an isolated subprocess.

This script receives code and DataFrame via stdin (pickled),
executes the transform function, and returns the result via stdout.

SECURITY: This script runs in a subprocess with:
- Isolated working directory
- Restricted environment variables
- CREATE_NO_WINDOW flag on Windows
"""

import pickle
import sys
import traceback


def main():
    """Main entry point for sandbox subprocess."""
    try:
        # Read pickled input from stdin
        input_bytes = sys.stdin.buffer.read()
        input_data = pickle.loads(input_bytes)

        code = input_data["code"]
        df = input_data["dataframe"]
        restricted_globals = input_data["globals"]

        # Create local scope with DataFrame
        local_scope = {"df": df}

        # Execute the compiled code
        # This populates local_scope with the transform function
        exec(code, restricted_globals, local_scope)

        # Verify transform function exists
        if "transform" not in local_scope:
            raise ValueError(
                "No 'transform' function defined in script. "
                "Your code must define: def transform(df): ..."
            )

        # Get and call the transform function
        transform_func = local_scope["transform"]
        result_df = transform_func(df)

        # Validate result is a DataFrame
        import pandas as pd

        if not isinstance(result_df, pd.DataFrame):
            raise TypeError(
                f"transform() must return a DataFrame, got {type(result_df).__name__}"
            )

        # Validate DataFrame is not empty (warning, not error)
        if result_df.empty:
            print("WARNING: transform() returned an empty DataFrame", file=sys.stderr)

        # Return success response
        response = {
            "success": True,
            "dataframe": result_df,
            "row_count": len(result_df),
            "columns": list(result_df.columns),
        }

    except Exception as e:
        # Capture full traceback for debugging
        tb = traceback.format_exc()

        # Filter traceback to remove internal frames
        filtered_tb = _filter_traceback(tb)

        response = {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": filtered_tb,
        }

    # Write pickled response to stdout
    try:
        output_bytes = pickle.dumps(response)
        sys.stdout.buffer.write(output_bytes)
        sys.stdout.buffer.flush()
    except Exception as e:
        # Last resort error handling
        sys.stderr.write(f"Failed to serialize response: {e}\n")
        sys.exit(1)


def _filter_traceback(tb: str) -> str:
    """
    Filter traceback to show only user code frames.

    Removes internal RestrictedPython frames for cleaner output.
    """
    lines = tb.split("\n")
    filtered_lines = []
    skip_until_user = False

    for line in lines:
        # Skip RestrictedPython internal frames
        if "RestrictedPython" in line or "sandbox/runner.py" in line:
            skip_until_user = True
            continue

        # Show user script frames
        if "<user_script>" in line:
            skip_until_user = False

        if not skip_until_user:
            filtered_lines.append(line)

    return "\n".join(filtered_lines)


if __name__ == "__main__":
    main()
