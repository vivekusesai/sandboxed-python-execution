"""Script schemas."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ScriptCreate(BaseModel):
    """Script creation schema."""

    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    code_text: str = Field(..., min_length=1)


class ScriptUpdate(BaseModel):
    """Script update schema."""

    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    code_text: Optional[str] = Field(None, min_length=1)


class ScriptResponse(BaseModel):
    """Script response schema."""

    id: int
    user_id: int
    name: str
    description: Optional[str]
    code_text: str
    created_at: datetime
    updated_at: Optional[datetime]

    model_config = {"from_attributes": True}
