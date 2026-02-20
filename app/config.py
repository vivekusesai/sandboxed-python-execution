"""Application configuration using Pydantic Settings."""

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/transform_db"
    SYNC_DATABASE_URL: str = "postgresql://postgres:password@localhost:5432/transform_db"

    # JWT Authentication
    JWT_SECRET_KEY: str = "change-this-secret-key-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 60

    # Sandbox Configuration
    SANDBOX_TIMEOUT_SECONDS: int = 60
    SANDBOX_MAX_MEMORY_MB: int = 512
    SANDBOX_MAX_OUTPUT_ROWS: int = 1_000_000
    CHUNK_SIZE: int = 50_000

    # Worker Configuration
    WORKER_POLL_INTERVAL: float = 1.0
    MAX_CONCURRENT_JOBS: int = 4

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_DIR: str = "logs"

    # Application
    APP_NAME: str = "Data Transformation Platform"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
