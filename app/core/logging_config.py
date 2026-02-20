"""Logging configuration with rotating file handlers."""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from app.config import get_settings

settings = get_settings()


def setup_logging() -> None:
    """Configure application logging with rotating file handlers."""
    log_dir = Path(settings.LOG_DIR)
    log_dir.mkdir(exist_ok=True)

    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.LOG_LEVEL))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)

    # File handler for app.log
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )

    app_handler = RotatingFileHandler(
        log_dir / "app.log",
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )
    app_handler.setLevel(logging.DEBUG)
    app_handler.setFormatter(file_format)
    root_logger.addHandler(app_handler)

    # Sandbox-specific logger
    sandbox_logger = logging.getLogger("sandbox")
    sandbox_handler = RotatingFileHandler(
        log_dir / "sandbox.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    sandbox_handler.setFormatter(file_format)
    sandbox_logger.addHandler(sandbox_handler)

    # Reduce noise from external libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


def setup_worker_logging() -> None:
    """Configure worker-specific logging."""
    log_dir = Path(settings.LOG_DIR)
    log_dir.mkdir(exist_ok=True)

    worker_logger = logging.getLogger("worker")
    worker_logger.setLevel(getattr(logging, settings.LOG_LEVEL))
    worker_logger.handlers.clear()

    # File format
    file_format = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )

    # Worker file handler
    worker_handler = RotatingFileHandler(
        log_dir / "worker.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    worker_handler.setFormatter(file_format)
    worker_logger.addHandler(worker_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_format)
    worker_logger.addHandler(console_handler)
