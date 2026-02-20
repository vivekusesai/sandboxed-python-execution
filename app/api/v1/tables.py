"""Table explorer endpoints."""

from typing import List

from fastapi import APIRouter, HTTPException, Query, status

from app.api.deps import CurrentUser, DbSession
from app.schemas.table import ColumnInfo, TableInfo, TablePreview
from app.services.table_service import TableService

router = APIRouter(prefix="/tables", tags=["tables"])


@router.get("/", response_model=List[str])
async def list_tables(
    db: DbSession,
    current_user: CurrentUser,
) -> List[str]:
    """List all available data tables."""
    service = TableService(db)
    return await service.list_tables()


@router.get("/{table_name}", response_model=TableInfo)
async def get_table_info(
    table_name: str,
    db: DbSession,
    current_user: CurrentUser,
) -> TableInfo:
    """Get table information including schema and row count."""
    service = TableService(db)

    if not service.is_valid_table_name(table_name):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid table name",
        )

    # Check if table exists
    tables = await service.list_tables()
    if table_name not in tables:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' not found",
        )

    return await service.get_table_info(table_name)


@router.get("/{table_name}/schema", response_model=List[ColumnInfo])
async def get_table_schema(
    table_name: str,
    db: DbSession,
    current_user: CurrentUser,
) -> List[ColumnInfo]:
    """Get column schema for a table."""
    service = TableService(db)

    if not service.is_valid_table_name(table_name):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid table name",
        )

    tables = await service.list_tables()
    if table_name not in tables:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' not found",
        )

    return await service.get_table_schema(table_name)


@router.get("/{table_name}/preview", response_model=TablePreview)
async def preview_table(
    table_name: str,
    db: DbSession,
    current_user: CurrentUser,
    limit: int = Query(default=100, ge=1, le=1000),
) -> TablePreview:
    """Preview first N rows of a table."""
    service = TableService(db)

    if not service.is_valid_table_name(table_name):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid table name",
        )

    tables = await service.list_tables()
    if table_name not in tables:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' not found",
        )

    return await service.preview_table(table_name, limit=limit)
