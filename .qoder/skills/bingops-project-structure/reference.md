# 项目结构参考实现

## 数据库访问边界

本项目连接 **bingops 同一个 PostgreSQL**（决策 D2），权限最小化，只映射两张表：

| 表 | 权限 | 用途 |
|---|---|---|
| `cmdb_sync_tasks` | SELECT + UPDATE(last_synced_at) | 调度输入 / 执行记录 |
| `cmdb_resources` | SELECT | 差集软删对账、ACK enrichment 映射查询 |

不建任何 DDL/迁移，表结构归 bingops 所有。

## pyproject.toml 基础配置

```toml
[project]
name = "cloud-syncer"
version = "0.1.0"
description = "Cloud resource sync engine - Kafka producer for bingops CMDB"
requires-python = ">=3.13"
dependencies = [
    "aiokafka>=0.12",
    "sqlalchemy[asyncio]>=2.0",
    "asyncpg>=0.30",
    "pydantic>=2.10",
    "pydantic-settings>=2.7",
    "structlog>=24.4",
    "PyYAML>=6.0",
    "croniter>=5.0",
    "tenacity>=9.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
    "pytest-cov>=6.0",
    "ruff>=0.8",
    "mypy>=1.14",
]

[tool.ruff]
target-version = "py313"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP", "B", "A", "SIM", "TCH"]

[tool.ruff.lint.isort]
known-first-party = ["cloudsync"]

[tool.mypy]
python_version = "3.13"
strict = true
plugins = ["pydantic.mypy"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

## asyncio 入口（cloudsync/main.py）

```python
import asyncio
import logging

from cloudsync.core.accounts import load_accounts
from cloudsync.core.config import settings
from cloudsync.core.logging import setup_logging
from cloudsync.scheduler.engine import SyncEngine

logger = logging.getLogger("cloudsync.main")


async def main() -> None:
    setup_logging(level=settings.log_level, debug=settings.debug, log_dir=settings.log_dir)
    accounts = load_accounts(settings.accounts_file)
    engine = SyncEngine(accounts=accounts)
    logger.info("Cloud syncer starting", extra={"accounts": len(accounts)})
    try:
        await engine.run()
    finally:
        await engine.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

## 共享库访问（cloudsync/core/db.py）

```python
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from cloudsync.core.config import settings

engine = create_async_engine(settings.database_url, pool_pre_ping=True)
session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
```

## 共享表只读映射（cloudsync/scheduler/models.py）

```python
from datetime import datetime

from sqlalchemy import JSON, BigInteger, Boolean, DateTime, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SyncTaskRow(Base):
    """cmdb_sync_tasks 只读映射（引擎仅 UPDATE last_synced_at）。"""
    __tablename__ = "cmdb_sync_tasks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    task_type: Mapped[str] = mapped_column(String(32))
    target_id: Mapped[str] = mapped_column(String(128))
    provider: Mapped[str] = mapped_column(String(32))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    schedule: Mapped[str] = mapped_column(String(64))
    resource_types: Mapped[list] = mapped_column(JSON, default=list)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResourceRow(Base):
    """cmdb_resources 只读映射（差集对账用）。"""
    __tablename__ = "cmdb_resources"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    model_id: Mapped[int] = mapped_column(BigInteger)
    provider: Mapped[str] = mapped_column(String(32))
    provider_id: Mapped[str] = mapped_column(String(256))
    cloud_account: Mapped[str] = mapped_column(String(128))
    source: Mapped[str] = mapped_column(String(32))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
```

## 测试 conftest（tests/conftest.py）

```python
import pytest


@pytest.fixture
def sample_account_yaml(tmp_path):
    content = """
providers:
  aliyun:
    - account_id: "1234567890"
      display_name: prod
      access_key_id: "LTAI***"
      access_key_secret: "***"
      regions: [cn-beijing]
"""
    path = tmp_path / "accounts.yaml"
    path.write_text(content, encoding="utf-8")
    return path
```
