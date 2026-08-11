"""Centralized logging setup: stdout + optional rotating file channel.

Call setup_logging() once from main.py before starting the engine.
Business code must only use `logger = logging.getLogger(f"cloudsync.{__name__}")`.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import sys
from datetime import UTC, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

LOG_FILE_NAME = "cloudsync.log"

CONSOLE_FORMAT = "[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_RESERVED_KEYS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}

SENSITIVE_KEYS = {
    "password",
    "token",
    "secret",
    "api_key",
    "authorization",
    "access_key_id",
    "access_key_secret",
    "service_account_json",
}


def filter_sensitive_data(data: dict[str, Any]) -> dict[str, Any]:
    """Replace sensitive fields in a dict with '***' recursively."""
    filtered: dict[str, Any] = {}
    for key, value in data.items():
        if key.lower() in SENSITIVE_KEYS:
            filtered[key] = "***"
        elif isinstance(value, dict):
            filtered[key] = filter_sensitive_data(value)
        else:
            filtered[key] = value
    return filtered


class JsonFormatter(logging.Formatter):
    """Single-line JSON formatter for production output."""

    def format(self, record: logging.LogRecord) -> str:
        extra = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _RESERVED_KEYS and not k.startswith("_")
        }
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if extra:
            payload["extra"] = filter_sensitive_data(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class _GzipTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Midnight rotation + gzip compression (cloudsync.log.YYYY-MM-DD.gz)."""

    def rotation_filename(self, default_name: str) -> str:
        return default_name + ".gz"

    def rotate(self, source: str, dest: str) -> None:
        if os.path.exists(source):
            with open(source, "rb") as sf, gzip.open(dest, "wb") as df:
                df.writelines(sf)
            os.remove(source)


def setup_logging(
    *,
    level: str = "INFO",
    debug: bool = True,
    log_dir: str | None = None,
    retention_days: int = 7,
) -> None:
    """Initialize root logger: stdout always on; file channel when log_dir set.

    Args:
        level: Log level name, e.g. "DEBUG", "INFO".
        debug: True for console format (dev), False for JSON lines (prod).
        log_dir: Directory for the file channel; skipped when None.
        retention_days: File retention in days; expired files auto-removed.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    formatter: logging.Formatter = (
        logging.Formatter(CONSOLE_FORMAT, datefmt=DATE_FORMAT) if debug else JsonFormatter()
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(stdout_handler)
    root.setLevel(log_level)

    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        file_handler = _GzipTimedRotatingFileHandler(
            Path(log_dir) / LOG_FILE_NAME,
            when="midnight",
            backupCount=retention_days,
            encoding="utf-8",
        )
        file_handler.setFormatter(JsonFormatter())  # file channel is always JSON
        root.addHandler(file_handler)

    # Silence noisy third-party loggers
    for noisy in ("aiokafka", "sqlalchemy.engine", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
