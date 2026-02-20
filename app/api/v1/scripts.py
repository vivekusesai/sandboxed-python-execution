"""Script CRUD endpoints."""

from typing import List

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import select

from app.api.deps import CurrentUser, DbSession
from app.models.script import Script
from app.schemas.script import ScriptCreate, ScriptResponse, ScriptUpdate

router = APIRouter(prefix="/scripts", tags=["scripts"])


@router.post("/", response_model=ScriptResponse, status_code=status.HTTP_201_CREATED)
async def create_script(
    script_data: ScriptCreate,
    db: DbSession,
    current_user: CurrentUser,
) -> Script:
    """Create a new transformation script."""
    script = Script(
        user_id=current_user.id,
        name=script_data.name,
        description=script_data.description,
        code_text=script_data.code_text,
    )
    db.add(script)
    await db.commit()
    await db.refresh(script)
    return script


@router.get("/", response_model=List[ScriptResponse])
async def list_scripts(
    db: DbSession,
    current_user: CurrentUser,
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=100),
) -> List[Script]:
    """List all scripts for the current user."""
    result = await db.execute(
        select(Script)
        .where(Script.user_id == current_user.id)
        .order_by(Script.created_at.desc())
        .offset(skip)
        .limit(limit)
    )
    return list(result.scalars().all())


@router.get("/{script_id}", response_model=ScriptResponse)
async def get_script(
    script_id: int,
    db: DbSession,
    current_user: CurrentUser,
) -> Script:
    """Get a specific script by ID."""
    result = await db.execute(
        select(Script).where(
            Script.id == script_id,
            Script.user_id == current_user.id,
        )
    )
    script = result.scalar_one_or_none()

    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Script not found",
        )

    return script


@router.put("/{script_id}", response_model=ScriptResponse)
async def update_script(
    script_id: int,
    script_data: ScriptUpdate,
    db: DbSession,
    current_user: CurrentUser,
) -> Script:
    """Update an existing script."""
    result = await db.execute(
        select(Script).where(
            Script.id == script_id,
            Script.user_id == current_user.id,
        )
    )
    script = result.scalar_one_or_none()

    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Script not found",
        )

    # Update fields if provided
    update_data = script_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(script, field, value)

    await db.commit()
    await db.refresh(script)
    return script


@router.delete("/{script_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_script(
    script_id: int,
    db: DbSession,
    current_user: CurrentUser,
) -> None:
    """Delete a script."""
    result = await db.execute(
        select(Script).where(
            Script.id == script_id,
            Script.user_id == current_user.id,
        )
    )
    script = result.scalar_one_or_none()

    if not script:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Script not found",
        )

    await db.delete(script)
    await db.commit()
