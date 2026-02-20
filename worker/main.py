"""
Worker main entry point - polls for jobs and executes them.

Run with: python -m worker.main
"""

import asyncio
import logging
import signal
import sys
from multiprocessing import Pool, cpu_count
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import get_settings
from app.core.logging_config import setup_worker_logging
from worker.job_processor import process_job_sync
from worker.queue_manager import QueueManager

settings = get_settings()
logger = logging.getLogger("worker")


class Worker:
    """
    Main worker process that polls for jobs and executes them.

    Uses a multiprocessing pool for parallel job execution.
    Each job runs in its own process for isolation.
    """

    def __init__(self):
        self.running = True
        self.queue_manager = QueueManager()
        self.active_jobs: dict = {}

    async def run(self):
        """Main worker loop."""
        setup_worker_logging()
        logger.info("=" * 50)
        logger.info("Worker starting...")
        logger.info(f"Poll interval: {settings.WORKER_POLL_INTERVAL}s")
        logger.info(f"Max concurrent jobs: {settings.MAX_CONCURRENT_JOBS}")
        logger.info(f"Sandbox timeout: {settings.SANDBOX_TIMEOUT_SECONDS}s")
        logger.info(f"Chunk size: {settings.CHUNK_SIZE:,} rows")
        logger.info("=" * 50)

        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()

        # Determine number of worker processes
        max_workers = min(settings.MAX_CONCURRENT_JOBS, cpu_count())
        logger.info(f"Starting process pool with {max_workers} workers")

        # Create process pool
        with Pool(processes=max_workers) as pool:
            while self.running:
                try:
                    await self._poll_and_process(pool, max_workers)
                except Exception as e:
                    logger.error(f"Worker error: {e}", exc_info=True)
                    await asyncio.sleep(5)  # Back off on error

        logger.info("Worker stopped")

    async def _poll_and_process(self, pool: Pool, max_workers: int):
        """Poll for pending jobs and dispatch to pool."""

        # Clean up completed jobs
        self._cleanup_completed()

        # Calculate available slots
        available_slots = max_workers - len(self.active_jobs)

        if available_slots > 0:
            # Get pending jobs
            jobs = await self.queue_manager.get_pending_jobs(limit=available_slots)

            if jobs:
                logger.info(f"Found {len(jobs)} pending job(s)")

                for job in jobs:
                    if len(self.active_jobs) >= max_workers:
                        break

                    # Mark job as running before dispatching
                    await self.queue_manager.mark_job_running(job.id)

                    # Submit to pool
                    result = pool.apply_async(
                        process_job_sync,
                        (job.id,),
                        callback=self._job_callback,
                        error_callback=self._job_error_callback,
                    )

                    self.active_jobs[job.id] = result
                    logger.info(f"Dispatched job {job.id} to worker pool")

        # Wait before next poll
        await asyncio.sleep(settings.WORKER_POLL_INTERVAL)

    def _cleanup_completed(self):
        """Remove completed jobs from active tracking."""
        completed = [
            job_id
            for job_id, result in self.active_jobs.items()
            if result.ready()
        ]
        for job_id in completed:
            del self.active_jobs[job_id]

    def _job_callback(self, success: bool):
        """Callback when job completes."""
        status = "SUCCESS" if success else "FAILED"
        logger.debug(f"Job completed with status: {status}")

    def _job_error_callback(self, error: Exception):
        """Callback when job raises exception."""
        logger.error(f"Job raised exception: {error}")

    def _setup_signal_handlers(self):
        """Setup handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self.running = False

        # Handle SIGINT (Ctrl+C) and SIGTERM
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)


def main():
    """Entry point for worker."""
    worker = Worker()

    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logger.info("Worker interrupted")
    except Exception as e:
        logger.exception(f"Worker crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
