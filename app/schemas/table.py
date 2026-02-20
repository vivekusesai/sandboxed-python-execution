"""Table schemas for data preview and introspection."""

from typing import Any, List, Optional

from pydantic import BaseModel


class ColumnInfo(BaseModel):
    """Column information schema."""

    name: str
    type: str
    nullable: bool
    default: Optional[str] = None


class TableInfo(BaseModel):
    """Table information schema."""

    name: str
    row_count: int
    columns: List[ColumnInfo]


class TablePreview(BaseModel):
    """Table preview schema."""

    table_name: str
    total_rows: int
    preview_rows: int
    columns: List[str]
    data: List[dict[str, Any]]
