"""
Sandbox executor - launches and monitors subprocess for safe code execution.

This implements Layers 2, 4, and 5 of the sandbox strategy:
- Layer 2: Subprocess isolation (CREATE_NO_WINDOW, restricted env)
- Layer 4: psutil monitoring (CPU, memory, timeout)
- Layer 5: File system controls (isolated temp dir)
"""

import logging
import os
import pickle
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import psutil

from sandbox.restricted_compiler import RestrictedCompiler

# Get settings - import here to avoid circular imports in worker
try:
    from app.config import get_settings

    settings = get_settings()
    SANDBOX_TIMEOUT = settings.SANDBOX_TIMEOUT_SECONDS
    SANDBOX_MAX_MEMORY = settings.SANDBOX_MAX_MEMORY_MB
except ImportError:
    # Fallback defaults if running standalone
    SANDBOX_TIMEOUT = 60
    SANDBOX_MAX_MEMORY = 512

logger = logging.getLogger("sandbox")


class SandboxExecutor:
    """
    Executes user code in an isolated subprocess with monitoring.

    Security layers implemented:
    1. RestrictedPython compilation (via RestrictedCompiler)
    2. Subprocess isolation with minimal environment
    3. Import allowlist (enforced by guards)
    4. psutil monitoring for resource limits
    5. Isolated working directory per job
    """

    def __init__(self, job_id: int):
        self.job_id = job_id
        self.compiler = RestrictedCompiler()
        self.sandbox_dir = Path(f"sandbox_temp/{job_id}")
        self.process: Optional[subprocess.Popen] = None

    def execute(
        self, code: str, df: pd.DataFrame
    ) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """
        Execute user code on DataFrame in isolated subprocess.

        Args:
            code: User's transformation code
            df: Input DataFrame

        Returns:
            Tuple of (success, result_df, log_output)
            - success: True if execution completed without errors
            - result_df: Transformed DataFrame or None if failed
            - log_output: Execution logs for debugging
        """
        logs = []
        self._log(logs, f"Starting sandbox execution for job {self.job_id}")
        self._log(logs, f"Input DataFrame: {len(df)} rows, {len(df.columns)} columns")

        try:
            # Step 1: Compile code with RestrictedPython
            self._log(logs, "Compiling code with RestrictedPython...")
            compiled_code, error = self.compiler.compile_code(code)

            if error:
                self._log(logs, f"COMPILATION ERROR: {error}")
                return False, None, "\n".join(logs)

            self._log(logs, "Code compiled successfully")

            # Step 2: Create isolated working directory
            self._setup_sandbox_dir()
            self._log(logs, f"Created sandbox directory: {self.sandbox_dir}")

            # Step 3: Prepare input data
            input_data = {
                "code": compiled_code,
                "dataframe": df,
                "globals": self.compiler.get_restricted_globals(),
            }

            # Step 4: Launch subprocess and monitor
            result = self._run_subprocess(input_data, logs)

            return result

        except Exception as e:
            self._log(logs, f"EXECUTION ERROR: {str(e)}")
            logger.exception(f"Sandbox execution failed for job {self.job_id}")
            return False, None, "\n".join(logs)

        finally:
            # Step 5: Cleanup
            self._cleanup_sandbox_dir()

    def _run_subprocess(
        self, input_data: dict, logs: list
    ) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Run the sandbox subprocess with monitoring."""

        # Prepare minimal environment
        env = {
            "PYTHONPATH": str(Path(__file__).parent.parent.absolute()),
            "PYTHONHASHSEED": "0",
            "PYTHONDONTWRITEBYTECODE": "1",
        }

        # Add essential Windows environment variables
        if os.name == "nt":
            for key in ["SYSTEMROOT", "TEMP", "TMP", "PATH"]:
                if key in os.environ:
                    env[key] = os.environ[key]

        # Windows-specific: CREATE_NO_WINDOW flag
        creation_flags = 0
        if os.name == "nt":
            creation_flags = subprocess.CREATE_NO_WINDOW

        # Path to runner script
        runner_path = Path(__file__).parent / "runner.py"

        self._log(logs, f"Launching subprocess: python {runner_path}")

        self.process = subprocess.Popen(
            [sys.executable, str(runner_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(self.sandbox_dir),
            env=env,
            creationflags=creation_flags,
        )

        self._log(logs, f"Subprocess started (PID: {self.process.pid})")

        # Send input data
        try:
            pickled_input = pickle.dumps(input_data)
            self._log(logs, f"Sending {len(pickled_input)} bytes to subprocess")
            self.process.stdin.write(pickled_input)
            self.process.stdin.close()
        except Exception as e:
            self._kill_process()
            self._log(logs, f"ERROR: Failed to send data: {e}")
            return False, None, "\n".join(logs)

        # Monitor process
        result = self._monitor_process(logs)
        return result

    def _monitor_process(
        self, logs: list
    ) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Monitor subprocess for resource limits and timeout."""
        start_time = time.time()

        try:
            ps_process = psutil.Process(self.process.pid)
        except psutil.NoSuchProcess:
            self._log(logs, "WARNING: Process ended before monitoring started")
            return self._handle_process_completion(logs)

        self._log(logs, f"Monitoring process (timeout: {SANDBOX_TIMEOUT}s, memory limit: {SANDBOX_MAX_MEMORY}MB)")

        while self.process.poll() is None:
            elapsed = time.time() - start_time

            # Check timeout
            if elapsed > SANDBOX_TIMEOUT:
                self._kill_process()
                self._log(logs, f"KILLED: Timeout exceeded ({SANDBOX_TIMEOUT}s)")
                return False, None, "\n".join(logs)

            # Check resource usage
            try:
                memory_mb = ps_process.memory_info().rss / (1024 * 1024)
                cpu_percent = ps_process.cpu_percent(interval=0.1)

                # Check memory limit
                if memory_mb > SANDBOX_MAX_MEMORY:
                    self._kill_process()
                    self._log(
                        logs,
                        f"KILLED: Memory limit exceeded ({memory_mb:.1f}MB > {SANDBOX_MAX_MEMORY}MB)",
                    )
                    return False, None, "\n".join(logs)

                # Log high CPU usage (but don't kill - it's expected for computations)
                if cpu_percent > 95 and elapsed > 5:
                    self._log(logs, f"Note: High CPU usage ({cpu_percent}%)")

            except psutil.NoSuchProcess:
                break  # Process ended

            time.sleep(0.5)

        return self._handle_process_completion(logs)

    def _handle_process_completion(
        self, logs: list
    ) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Handle process completion and parse output."""

        # Read output
        stdout = self.process.stdout.read()
        stderr = self.process.stderr.read()

        if stderr:
            stderr_text = stderr.decode("utf-8", errors="replace")
            self._log(logs, f"Stderr: {stderr_text}")

        if self.process.returncode != 0:
            self._log(logs, f"Process exited with code {self.process.returncode}")
            return False, None, "\n".join(logs)

        # Parse output
        try:
            response = pickle.loads(stdout)

            if response.get("success"):
                result_df = response["dataframe"]
                self._log(
                    logs,
                    f"SUCCESS: Transformed {response.get('row_count', len(result_df))} rows",
                )
                self._log(logs, f"Output columns: {response.get('columns', list(result_df.columns))}")
                return True, result_df, "\n".join(logs)
            else:
                error_msg = response.get("error", "Unknown error")
                error_type = response.get("error_type", "Error")
                self._log(logs, f"EXECUTION FAILED: {error_type}: {error_msg}")
                if response.get("traceback"):
                    self._log(logs, f"Traceback:\n{response['traceback']}")
                return False, None, "\n".join(logs)

        except Exception as e:
            self._log(logs, f"ERROR: Failed to parse subprocess output: {e}")
            self._log(logs, f"Raw output length: {len(stdout)} bytes")
            return False, None, "\n".join(logs)

    def _kill_process(self):
        """Force kill the subprocess and all children."""
        if self.process and self.process.poll() is None:
            try:
                parent = psutil.Process(self.process.pid)
                children = parent.children(recursive=True)

                # Kill children first
                for child in children:
                    try:
                        child.kill()
                    except psutil.NoSuchProcess:
                        pass

                # Kill parent
                parent.kill()
                parent.wait(timeout=5)

            except psutil.NoSuchProcess:
                pass
            except Exception as e:
                logger.warning(f"Error killing process: {e}")

    def _setup_sandbox_dir(self):
        """Create isolated sandbox directory."""
        self.sandbox_dir.mkdir(parents=True, exist_ok=True)

    def _cleanup_sandbox_dir(self):
        """Remove sandbox directory after execution."""
        try:
            if self.sandbox_dir.exists():
                shutil.rmtree(self.sandbox_dir)
        except Exception as e:
            logger.warning(f"Failed to cleanup sandbox dir: {e}")

    def _log(self, logs: list, message: str):
        """Add timestamped log entry."""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        entry = f"[{timestamp}] {message}"
        logs.append(entry)
        logger.debug(entry)
