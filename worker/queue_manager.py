"""Job queue manager - polls PostgreSQL for pending jobs."""

import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.models.job import Job, JobStatus

logger = logging.getLogger("worker")


class QueueManager:
    """
    Manages job queue by polling PostgreSQL.

    This is a simpler alternative to Redis/RQ that works well
    for the target scale of 10-15 concurrent users.
    """

    async def get_pending_jobs(self, limit: int = 4) -> List[Job]:
        """
        Get pending jobs from the database.

        Args:
            limit: Maximum number of jobs to fetch

        Returns:
            List of pending Job objects
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Job)
                .where(Job.status == JobStatus.PENDING)
                .order_by(Job.created_at.asc())
                .limit(limit)
            )
            jobs = list(result.scalars().all())

            # Detach jobs from session for use in worker
            for job in jobs:
                await db.refresh(job)

            return jobs

    async def mark_job_running(self, job_id: int) -> bool:
        """
        Mark a job as running.

        Args:
            job_id: Job ID to update

        Returns:
            True if updated, False if job not found
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                update(Job)
                .where(Job.id == job_id, Job.status == JobStatus.PENDING)
                .values(status=JobStatus.RUNNING, started_at=datetime.utcnow())
            )
            await db.commit()
            return result.rowcount > 0

    async def mark_job_completed(
        self,
        job_id: int,
        rows_processed: int,
        logs: str,
    ) -> bool:
        """
        Mark a job as completed.

        Args:
            job_id: Job ID to update
            rows_processed: Number of rows processed
            logs: Execution logs

        Returns:
            True if updated, False if job not found
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    status=JobStatus.COMPLETED,
                    rows_processed=rows_processed,
                    logs=logs,
                    completed_at=datetime.utcnow(),
                )
            )
            await db.commit()
            return result.rowcount > 0

    async def mark_job_failed(
        self,
        job_id: int,
        error_message: str,
        logs: str,
        status: JobStatus = JobStatus.FAILED,
    ) -> bool:
        """
        Mark a job as failed.

        Args:
            job_id: Job ID to update
            error_message: Error description
            logs: Execution logs
            status: Failure status (FAILED, TIMEOUT, KILLED)

        Returns:
            True if updated, False if job not found
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    status=status,
                    error_message=error_message,
                    logs=logs,
                    completed_at=datetime.utcnow(),
                )
            )
            await db.commit()
            return result.rowcount > 0

    async def update_job_progress(
        self,
        job_id: int,
        rows_processed: int,
        logs: str,
    ) -> bool:
        """
        Update job progress during chunked processing.

        Args:
            job_id: Job ID to update
            rows_processed: Current number of rows processed
            logs: Current execution logs

        Returns:
            True if updated, False if job not found
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(rows_processed=rows_processed, logs=logs)
            )
            await db.commit()
            return result.rowcount > 0

    async def get_job(self, job_id: int) -> Optional[Job]:
        """
        Get a job by ID.

        Args:
            job_id: Job ID to fetch

        Returns:
            Job object or None if not found
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Job).where(Job.id == job_id))
            return result.scalar_one_or_none()

    async def check_job_cancelled(self, job_id: int) -> bool:
        """
        Check if a job has been cancelled (KILLED status).

        Args:
            job_id: Job ID to check

        Returns:
            True if job is killed/cancelled
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Job.status).where(Job.id == job_id)
            )
            status = result.scalar_one_or_none()
            return status == JobStatus.KILLED
