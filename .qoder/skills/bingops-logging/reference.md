# 日志工具参考实现

## 基础日志配置（cloudsync/core/logging.py）

```python
import gzip
import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

LOG_FILE_NAME = "cloudsync.log"

_RESERVED_KEYS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}

CONSOLE_FORMAT = "[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class JsonFormatter(logging.Formatter):
    """生产环境 JSON 单行格式，字段见 SKILL.md。"""

    def format(self, record: logging.LogRecord) -> str:
        extra = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _RESERVED_KEYS and not k.startswith("_")
        }
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc)
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


def setup_logging(
    *,
    level: str = "INFO",
    debug: bool = True,
    log_dir: str | None = None,
    retention_days: int = 7,
) -> None:
    """初始化日志系统：stdout 通道必开，log_dir 配置后开启文件通道。

    Args:
        level: 日志级别，如 "DEBUG", "INFO"。
        debug: True 控制台格式（开发），False JSON 单行格式（生产）。
        log_dir: 日志目录，配置后开启文件通道。
        retention_days: 文件通道保留天数，超期自动删除。
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    formatter: logging.Formatter
    if debug:
        formatter = logging.Formatter(CONSOLE_FORMAT, datefmt=DATE_FORMAT)
    else:
        formatter = JsonFormatter()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(stdout_handler)
    root.setLevel(log_level)

    if log_dir:
        file_handler = _GzipTimedRotatingFileHandler(
            Path(log_dir) / LOG_FILE_NAME,
            when="midnight",
            backupCount=retention_days,
            encoding="utf-8",
        )
        file_handler.setFormatter(JsonFormatter())  # 文件通道始终 JSON
        root.addHandler(file_handler)

    # 降低第三方库日志级别
    for noisy in ("aiokafka", "sqlalchemy.engine", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


class _GzipTimedRotatingFileHandler(TimedRotatingFileHandler):
    """午夜轮转 + gzip 压缩旧文件（cloudsync.log.YYYY-MM-DD.gz）。"""

    def rotation_filename(self, default_name: str) -> str:
        return default_name + ".gz"

    def rotate(self, source: str, dest: str) -> None:
        if os.path.exists(source):
            with open(source, "rb") as sf, gzip.open(dest, "wb") as df:
                df.writelines(sf)
            os.remove(source)
```

## 敏感信息过滤器

```python
SENSITIVE_KEYS = {
    "password", "token", "secret", "api_key", "authorization",
    "access_key_id", "access_key_secret", "service_account_json",
}


def filter_sensitive_data(data: dict[str, Any]) -> dict[str, Any]:
    """过滤字典中的敏感字段，替换为 '***'。"""
    filtered = {}
    for key, value in data.items():
        if key.lower() in SENSITIVE_KEYS:
            filtered[key] = "***"
        elif isinstance(value, dict):
            filtered[key] = filter_sensitive_data(value)
        else:
            filtered[key] = value
    return filtered
```

## 业务代码用法

```python
import logging

logger = logging.getLogger(f"cloudsync.{__name__}")


async def run_sync_round(task_id: int, provider: str) -> None:
    logger.info("Sync round started", extra={"task_id": task_id, "provider": provider})
    # ...
    logger.info(
        "Sync round completed",
        extra={"task_id": task_id, "provider": provider, "duration_ms": 1532.4},
    )
```
