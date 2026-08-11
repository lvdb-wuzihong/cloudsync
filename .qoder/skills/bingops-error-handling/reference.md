# 错误处理工具参考实现

## 完整异常类定义（cloudsync/core/exceptions.py）

```python
from __future__ import annotations


class CloudSyncError(Exception):
    """引擎基础异常，所有业务异常的根类。"""

    def __init__(
        self,
        message: str = "Internal engine error",
        code: int = 50001,
        error_code: str = "INTERNAL_ERROR",
    ) -> None:
        self.message = message
        self.code = code
        self.error_code = error_code
        super().__init__(message)

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "error_code": self.error_code}


# ── 配置/凭据类 ─────────────────────────────────────────────────────────────

class ConfigError(CloudSyncError):
    def __init__(self, message: str = "Configuration error"):
        super().__init__(message, code=50002, error_code="CONFIG_ERROR")


class CredentialError(CloudSyncError):
    def __init__(self, message: str = "Credential missing or invalid"):
        super().__init__(message, code=50003, error_code="AUTH_FAILED")


# ── 云 API 类（error_code 归一：RATE_LIMITED / AUTH_FAILED / API_ERROR）──

class AuthFailedError(CloudSyncError):
    def __init__(self, provider: str, detail: str):
        super().__init__(f"Cloud auth failed for '{provider}': {detail}",
                         code=50202, error_code="AUTH_FAILED")
        self.provider = provider


class RateLimitError(CloudSyncError):
    def __init__(self, provider: str, detail: str):
        super().__init__(f"Cloud API rate limited for '{provider}': {detail}",
                         code=50203, error_code="RATE_LIMITED")
        self.provider = provider


class AdapterError(CloudSyncError):
    def __init__(self, provider: str, detail: str):
        super().__init__(f"Cloud API error for '{provider}': {detail}",
                         code=50201, error_code="API_ERROR")
        self.provider = provider


# ── 数据/对账/消息类 ────────────────────────────────────────────────────────

class ValidationError(CloudSyncError):
    def __init__(self, message: str = "Validation failed", errors: list[dict] | None = None):
        super().__init__(message, code=40001, error_code="VALIDATION_ERROR")
        self.errors = errors or []


class ReconcileError(CloudSyncError):
    def __init__(self, message: str = "Reconcile failed"):
        super().__init__(message, code=50004, error_code="RECONCILE_ERROR")


class KafkaPublishError(CloudSyncError):
    def __init__(self, topic: str, detail: str):
        super().__init__(f"Kafka publish to '{topic}' failed: {detail}",
                         code=50204, error_code="KAFKA_ERROR")
        self.topic = topic
```

## 重试工具封装（cloudsync/core/retry.py）

```python
from __future__ import annotations

import logging
from typing import TypeVar

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)

from cloudsync.core.exceptions import RateLimitError

logger = logging.getLogger("cloudsync.core.retry")
T = TypeVar("T")

# 云 SDK 限流重试：3 次指数退避，重试记 WARNING
cloud_api_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(RateLimitError),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)

# 共享库连接重试
db_retry = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)

# Kafka 发送重试：固定间隔 1s
kafka_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
```

## 使用示例

### Adapter 层（归一 SDK 错误并抛出）

```python
import logging

from cloudsync.core.exceptions import AdapterError, AuthFailedError, RateLimitError

logger = logging.getLogger(f"cloudsync.{__name__}")


async def fetch_ecs_instances(client, account):
    try:
        return await client.describe_instances()
    except SdkThrottlingException as e:
        raise RateLimitError("aliyun", str(e)) from e
    except SdkAuthException as e:
        raise AuthFailedError("aliyun", str(e)) from e
    except SdkException as e:
        raise AdapterError("aliyun", str(e)) from e
```

### 引擎层（任务级收敛，单任务失败不影响其他任务）

```python
async def _run_task(self, task: SyncTask) -> None:
    try:
        await self._run_round(task)
    except CloudSyncError as e:
        logger.error("Sync task failed with business error",
                     extra={"task_id": task.id, "error_code": e.error_code})
    except Exception:
        logger.exception("Sync task failed with unexpected error",
                         extra={"task_id": task.id})
```
