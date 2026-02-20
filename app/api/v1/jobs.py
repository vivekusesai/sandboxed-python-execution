"""Job management endpoints."""

from typing import List

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import select

from app.api.deps import CurrentUser, DbSession
from app.models.job import Job, JobStatus
from app.models.script import Script
from app.schemas.job import JobListResponse, JobResponse, JobSubmit
from app.services.table_service import TableService

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def submit_job(
    job_data: JobSubmit,
    db: DbSession,
    current_user: CurrentUser,
) -> Job:
    """
    Submit a new transformation job.

    The job will be queued for processing by the worker.
    """
    # Validate script exists and belongs to user
    result = await db.execute(
        select(Script).where(
            Script.id == job_data.script_id,
            Script.user_id == current_user.id,
        )
    )
    script = result.scalar_one_or_none()

    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Script not found",
        )

    # Validate source table exists
    table_service = TableService(db)
    tables = await table_service.list_tables()

    if job_data.source_table not in tables:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Source table '{job_data.source_table}' not found",
        )

    # Validate destination table name
    if not table_service.is_valid_table_name(job_data.destination_table):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid destination table name. Use alphanumeric characters and underscores, starting with a letter.",
        )

    # Check destination doesn't conflict with system tables
    from worker.data_handler import SYSTEM_TABLES

    if job_data.destination_table.lower() in SYSTEM_TABLES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot use system table name as destination",
        )

    # Create job
    job = Job(
        user_id=current_user.id,
        script_id=job_data.script_id,
        source_table=job_data.source_table,
        destination_table=job_data.destination_table,
        status=JobStatus.PENDING,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    return job


@router.get("/", response_model=List[JobListResponse])
async def list_jobs(
    db: DbSession,
    current_user: CurrentUser,
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=100),
    status_filter: JobStatus | None = Query(default=None, alias="status"),
) -> List[Job]:
    """List all jobs for the current user."""
    query = select(Job).where(Job.user_id == current_user.id)

    if status_filter:
        query = query.where(Job.status == status_filter)

    query = query.order_by(Job.created_at.desc()).offset(skip).limit(limit)

    result = await db.execute(query)
    return list(result.scalars().all())


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: int,
    db: DbSession,
    current_user: CurrentUser,
) -> Job:
    """Get job details including logs."""
    result = await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == current_user.id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )

    return job


@router.get("/{job_id}/logs")
async def get_job_logs(
    job_id: int,
    db: DbSession,
    current_user: CurrentUser,
) -> dict:
    """Get job execution logs."""
    result = await db.execute(
        select(Job.logs, Job.error_message, Job.status).where(
            Job.id == job_id, Job.user_id == current_user.id
        )
    )
    row = result.one_or_none()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )

    logs, error_message, status = row

    return {
        "job_id": job_id,
        "status": status.value,
        "logs": logs or "",
        "error_message": error_message,
    }


@router.post("/{job_id}/cancel")
async def cancel_job(
    job_id: int,
    db: DbSession,
    current_user: CurrentUser,
) -> dict:
    """Cancel a pending or running job."""
    result = await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == current_user.id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )

    if job.status not in [JobStatus.PENDING, JobStatus.RUNNING]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job in {job.status.value} status",
        )

    job.status = JobStatus.KILLED
    await db.commit()

    return {"message": "Job cancellation requested", "job_id": job_id}


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_job(
    job_id: int,
    db: DbSession,
    current_user: CurrentUser,
) -> None:
    """Delete a job record (does not delete destination table)."""
    result = await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == current_user.id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )

    # Only allow deleting completed/failed jobs
    if job.status in [JobStatus.PENDING, JobStatus.RUNNING]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete pending or running job. Cancel it first.",
        )

    await db.delete(job)
    await db.commit()
