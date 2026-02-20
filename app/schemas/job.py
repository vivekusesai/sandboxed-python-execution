"""Job schemas."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from app.models.job import JobStatus


class JobSubmit(BaseModel):
    """Job submission schema."""

    script_id: int
    source_table: str = Field(..., min_length=1, max_length=255)
    destination_table: str = Field(..., min_length=1, max_length=255)


class JobResponse(BaseModel):
    """Job response schema with full details."""

    id: int
    user_id: int
    script_id: Optional[int]
    source_table: str
    destination_table: str
    status: JobStatus
    logs: Optional[str]
    error_message: Optional[str]
    rows_processed: int
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    created_at: datetime

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    """Job list response schema (summary)."""

    id: int
    script_id: Optional[int]
    source_table: str
    destination_table: str
    status: JobStatus
    rows_processed: int
    created_at: datetime
    completed_at: Optional[datetime]

    model_config = {"from_attributes": True}
