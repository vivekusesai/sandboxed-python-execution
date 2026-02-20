"""Table introspection and data loading service."""

import re
from typing import Any, List, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.schemas.table import ColumnInfo, TableInfo, TablePreview

settings = get_settings()

# System tables to exclude from user table list
SYSTEM_TABLES = {"users", "scripts", "jobs", "alembic_version"}


class TableService:
    """Service for table introspection and data operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def list_tables(self) -> List[str]:
        """List all user data tables in the database."""
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        result = await self.db.execute(query)
        tables = [row[0] for row in result.fetchall()]
        # Filter out system tables
        return [t for t in tables if t not in SYSTEM_TABLES]

    async def get_table_schema(self, table_name: str) -> List[ColumnInfo]:
        """Get column information for a table."""
        if not self.is_valid_table_name(table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        query = text("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        result = await self.db.execute(query, {"table_name": table_name})
        return [
            ColumnInfo(
                name=row[0],
                type=row[1],
                nullable=row[2] == "YES",
                default=row[3],
            )
            for row in result.fetchall()
        ]

    async def get_row_count(self, table_name: str) -> int:
        """Get total row count for a table."""
        if not self.is_valid_table_name(table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        # Use identifier quoting for safety
        query = text(f'SELECT COUNT(*) FROM "{table_name}"')
        result = await self.db.execute(query)
        return result.scalar() or 0

    async def get_table_info(self, table_name: str) -> TableInfo:
        """Get table information including schema and row count."""
        columns = await self.get_table_schema(table_name)
        row_count = await self.get_row_count(table_name)
        return TableInfo(name=table_name, row_count=row_count, columns=columns)

    async def preview_table(
        self, table_name: str, limit: int = 100
    ) -> TablePreview:
        """Preview first N rows of a table."""
        if not self.is_valid_table_name(table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        # Cap limit at 1000
        limit = min(limit, 1000)

        # Get row count
        total_rows = await self.get_row_count(table_name)

        # Get preview data
        query = text(f'SELECT * FROM "{table_name}" LIMIT :limit')
        result = await self.db.execute(query, {"limit": limit})
        columns = list(result.keys())
        rows = [dict(zip(columns, row)) for row in result.fetchall()]

        return TablePreview(
            table_name=table_name,
            total_rows=total_rows,
            preview_rows=len(rows),
            columns=columns,
            data=rows,
        )

    async def load_table_chunk(
        self, table_name: str, chunk_size: int, offset: int
    ) -> pd.DataFrame:
        """Load a chunk of table data as DataFrame."""
        if not self.is_valid_table_name(table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        query = text(
            f'SELECT * FROM "{table_name}" LIMIT :limit OFFSET :offset'
        )
        result = await self.db.execute(
            query, {"limit": chunk_size, "offset": offset}
        )
        columns = list(result.keys())
        rows = result.fetchall()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows, columns=columns)

    async def load_table_as_dataframe(self, table_name: str) -> pd.DataFrame:
        """Load entire table as DataFrame."""
        if not self.is_valid_table_name(table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        row_count = await self.get_row_count(table_name)

        # For large tables, use chunked loading
        if row_count > settings.CHUNK_SIZE:
            chunks = []
            offset = 0
            while offset < row_count:
                chunk = await self.load_table_chunk(
                    table_name, settings.CHUNK_SIZE, offset
                )
                if chunk.empty:
                    break
                chunks.append(chunk)
                offset += settings.CHUNK_SIZE
            return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        else:
            return await self.load_table_chunk(table_name, row_count + 1, 0)

    @staticmethod
    def is_valid_table_name(name: str) -> bool:
        """Validate table name to prevent SQL injection."""
        # Only allow alphanumeric and underscore, starting with letter
        pattern = r"^[a-zA-Z][a-zA-Z0-9_]{0,62}$"
        if not re.match(pattern, name):
            return False

        # Block system table names
        if name.lower() in SYSTEM_TABLES:
            return False

        # Block PostgreSQL system prefixes
        reserved_prefixes = ("pg_", "sql_", "information_schema")
        if name.lower().startswith(reserved_prefixes):
            return False

        return True

    @staticmethod
    def is_valid_column_name(name: str) -> bool:
        """Validate column name."""
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$"
        return bool(re.match(pattern, str(name)))
