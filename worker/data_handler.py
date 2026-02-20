"""Data handler for reading/writing DataFrames to PostgreSQL."""

import logging
import re
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

from app.config import get_settings

settings = get_settings()
logger = logging.getLogger("worker")

# System tables that cannot be used as destinations
SYSTEM_TABLES = {"users", "scripts", "jobs", "alembic_version"}


class DataHandler:
    """
    Handles reading and writing DataFrames to PostgreSQL.

    Uses sync SQLAlchemy engine for pandas compatibility.
    """

    def __init__(self):
        self.engine = create_engine(
            settings.SYNC_DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )

    def load_table(self, table_name: str) -> pd.DataFrame:
        """
        Load entire table as DataFrame.

        Args:
            table_name: Source table name

        Returns:
            DataFrame with table data
        """
        if not self.is_valid_table_name(table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        query = f'SELECT * FROM "{table_name}"'
        return pd.read_sql(query, self.engine)

    def load_table_chunk(
        self, table_name: str, chunk_size: int, offset: int
    ) -> pd.DataFrame:
        """
        Load a chunk of table data.

        Args:
            table_name: Source table name
            chunk_size: Number of rows to load
            offset: Starting row offset

        Returns:
            DataFrame with chunk data
        """
        if not self.is_valid_table_name(table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        query = f'SELECT * FROM "{table_name}" LIMIT {chunk_size} OFFSET {offset}'
        return pd.read_sql(query, self.engine)

    def get_row_count(self, table_name: str) -> int:
        """
        Get total row count for a table.

        Args:
            table_name: Table name

        Returns:
            Number of rows
        """
        if not self.is_valid_table_name(table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        with self.engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            return result.scalar() or 0

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
    ) -> int:
        """
        Write DataFrame to PostgreSQL table.

        Args:
            df: DataFrame to write
            table_name: Destination table name
            if_exists: How to handle existing table ('replace', 'append', 'fail')

        Returns:
            Number of rows written

        Raises:
            ValueError: If validation fails
        """
        # Validate table name
        if not self.is_valid_destination_table(table_name):
            raise ValueError(f"Invalid destination table name: {table_name}")

        # Validate DataFrame
        if df.empty:
            raise ValueError("Cannot write empty DataFrame")

        if len(df) > settings.SANDBOX_MAX_OUTPUT_ROWS:
            raise ValueError(
                f"Output exceeds maximum rows ({len(df):,} > {settings.SANDBOX_MAX_OUTPUT_ROWS:,})"
            )

        # Validate column names
        for col in df.columns:
            if not self.is_valid_column_name(str(col)):
                raise ValueError(f"Invalid column name: {col}")

        # Write DataFrame
        rows_written = df.to_sql(
            table_name,
            self.engine,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=10000,
        )

        logger.info(f"Wrote {len(df)} rows to table '{table_name}'")
        return len(df)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Table name to check

        Returns:
            True if table exists
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                )
                """
                ),
                {"table_name": table_name},
            )
            return result.scalar() or False

    @staticmethod
    def is_valid_table_name(name: str) -> bool:
        """
        Validate table name format.

        Args:
            name: Table name to validate

        Returns:
            True if valid
        """
        # Only allow alphanumeric and underscore, starting with letter
        pattern = r"^[a-zA-Z][a-zA-Z0-9_]{0,62}$"
        return bool(re.match(pattern, name))

    @staticmethod
    def is_valid_destination_table(name: str) -> bool:
        """
        Validate destination table name.

        More restrictive than source table validation.
        Blocks system tables and reserved prefixes.
        """
        if not DataHandler.is_valid_table_name(name):
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
        """
        Validate column name format.

        Args:
            name: Column name to validate

        Returns:
            True if valid
        """
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$"
        return bool(re.match(pattern, name))
