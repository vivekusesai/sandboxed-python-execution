"""Job processor - executes transformation jobs with chunking support."""

import asyncio
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.models.job import Job, JobStatus
from app.models.script import Script
from sandbox.executor import SandboxExecutor
from worker.data_handler import DataHandler
from worker.queue_manager import QueueManager

settings = get_settings()
logger = logging.getLogger("worker")


class JobProcessor:
    """
    Processes transformation jobs with chunked execution support.

    Handles:
    - Loading source data (with chunking for large tables)
    - Executing code in sandbox
    - Writing results to destination table
    - Progress tracking and logging
    """

    def __init__(self):
        self.queue_manager = QueueManager()
        self.data_handler = DataHandler()

    async def process(self, job_id: int) -> bool:
        """
        Process a job from start to finish.

        Args:
            job_id: ID of job to process

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Processing job {job_id}")
        all_logs = []

        try:
            # Load job and script
            async with AsyncSessionLocal() as db:
                job = await self._load_job(db, job_id)
                if not job:
                    logger.error(f"Job {job_id} not found")
                    return False

                script = await self._load_script(db, job.script_id)
                if not script:
                    await self.queue_manager.mark_job_failed(
                        job_id, "Script not found", "Script was deleted"
                    )
                    return False

                source_table = job.source_table
                destination_table = job.destination_table
                code = script.code_text

            # Mark as running
            await self.queue_manager.mark_job_running(job_id)
            all_logs.append(f"[{self._timestamp()}] Job started")
            all_logs.append(f"[{self._timestamp()}] Source: {source_table}")
            all_logs.append(f"[{self._timestamp()}] Destination: {destination_table}")

            # Get row count
            row_count = self.data_handler.get_row_count(source_table)
            all_logs.append(f"[{self._timestamp()}] Source table has {row_count:,} rows")

            # Process with chunking for large tables
            if row_count > settings.CHUNK_SIZE:
                success = await self._process_chunked(
                    job_id, code, source_table, destination_table, row_count, all_logs
                )
            else:
                success = await self._process_full(
                    job_id, code, source_table, destination_table, all_logs
                )

            return success

        except Exception as e:
            logger.exception(f"Job {job_id} failed with exception")
            all_logs.append(f"[{self._timestamp()}] EXCEPTION: {str(e)}")
            await self.queue_manager.mark_job_failed(
                job_id, str(e), "\n".join(all_logs)
            )
            return False

    async def _process_full(
        self,
        job_id: int,
        code: str,
        source_table: str,
        destination_table: str,
        logs: list,
    ) -> bool:
        """Process entire table at once (for small tables)."""

        # Load data
        logs.append(f"[{self._timestamp()}] Loading table data...")
        df = self.data_handler.load_table(source_table)
        logs.append(f"[{self._timestamp()}] Loaded {len(df):,} rows")

        # Execute in sandbox
        logs.append(f"[{self._timestamp()}] Executing transformation...")
        executor = SandboxExecutor(job_id)
        success, result_df, exec_logs = executor.execute(code, df)
        logs.append(exec_logs)

        if not success:
            await self.queue_manager.mark_job_failed(
                job_id, "Transformation failed", "\n".join(logs)
            )
            return False

        # Check for cancellation
        if await self.queue_manager.check_job_cancelled(job_id):
            logs.append(f"[{self._timestamp()}] Job cancelled by user")
            return False

        # Write results
        logs.append(f"[{self._timestamp()}] Writing to {destination_table}...")
        rows_written = self.data_handler.write_dataframe(
            result_df, destination_table, if_exists="replace"
        )
        logs.append(f"[{self._timestamp()}] Wrote {rows_written:,} rows")

        # Mark complete
        await self.queue_manager.mark_job_completed(
            job_id, rows_written, "\n".join(logs)
        )

        logger.info(f"Job {job_id} completed: {rows_written} rows")
        return True

    async def _process_chunked(
        self,
        job_id: int,
        code: str,
        source_table: str,
        destination_table: str,
        total_rows: int,
        logs: list,
    ) -> bool:
        """Process large table in chunks."""

        logs.append(
            f"[{self._timestamp()}] Processing in chunks of {settings.CHUNK_SIZE:,} rows"
        )

        offset = 0
        chunk_num = 0
        total_rows_processed = 0
        first_chunk = True

        while offset < total_rows:
            chunk_num += 1

            # Check for cancellation
            if await self.queue_manager.check_job_cancelled(job_id):
                logs.append(f"[{self._timestamp()}] Job cancelled by user")
                return False

            logs.append(
                f"[{self._timestamp()}] Processing chunk {chunk_num} "
                f"(rows {offset:,}-{min(offset + settings.CHUNK_SIZE, total_rows):,})"
            )

            # Load chunk
            df_chunk = self.data_handler.load_table_chunk(
                source_table, settings.CHUNK_SIZE, offset
            )

            if df_chunk.empty:
                break

            logs.append(f"[{self._timestamp()}] Loaded {len(df_chunk):,} rows")

            # Execute transformation
            executor = SandboxExecutor(job_id)
            success, result_df, exec_logs = executor.execute(code, df_chunk)
            logs.append(exec_logs)

            if not success:
                await self.queue_manager.mark_job_failed(
                    job_id,
                    f"Transformation failed on chunk {chunk_num}",
                    "\n".join(logs),
                )
                return False

            # Write chunk (replace first, append rest)
            if_exists = "replace" if first_chunk else "append"
            rows_written = self.data_handler.write_dataframe(
                result_df, destination_table, if_exists=if_exists
            )

            total_rows_processed += rows_written
            first_chunk = False
            offset += settings.CHUNK_SIZE

            # Update progress
            await self.queue_manager.update_job_progress(
                job_id, total_rows_processed, "\n".join(logs)
            )

            logs.append(
                f"[{self._timestamp()}] Chunk {chunk_num} complete: "
                f"{rows_written:,} rows written"
            )

        # Mark complete
        logs.append(
            f"[{self._timestamp()}] All chunks processed: "
            f"{total_rows_processed:,} total rows"
        )
        await self.queue_manager.mark_job_completed(
            job_id, total_rows_processed, "\n".join(logs)
        )

        logger.info(f"Job {job_id} completed: {total_rows_processed} rows in {chunk_num} chunks")
        return True

    async def _load_job(self, db: AsyncSession, job_id: int) -> Optional[Job]:
        """Load job from database."""
        result = await db.execute(select(Job).where(Job.id == job_id))
        return result.scalar_one_or_none()

    async def _load_script(self, db: AsyncSession, script_id: int) -> Optional[Script]:
        """Load script from database."""
        if script_id is None:
            return None
        result = await db.execute(select(Script).where(Script.id == script_id))
        return result.scalar_one_or_none()

    @staticmethod
    def _timestamp() -> str:
        """Get formatted timestamp for logs."""
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def process_job_sync(job_id: int) -> bool:
    """
    Synchronous wrapper for multiprocessing.

    This function is called by the multiprocessing pool and
    runs the async process method in a new event loop.
    """
    processor = JobProcessor()
    return asyncio.run(processor.process(job_id))
