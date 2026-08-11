---
name: bingops-error-handling
description: 云同步引擎（cloud-syncer）异常处理与错误处理机制规范。适用于调度循环、云 API 调用、Kafka 发送、数据操作中的异常捕获与处理。当编写 try/except 块、定义异常类、处理外部服务调用时必须遵循此规范。
---

# 错误处理机制规范

## 异常类层级结构

所有业务异常必须继承自引擎基础异常类，禁止直接使用内置异常：

```python
class CloudSyncError(Exception):
    """引擎基础异常。"""
    def __init__(self, message: str, code: int = 50001, error_code: str = "INTERNAL_ERROR"):
        self.message = message
        self.code = code
        self.error_code = error_code   # 日志/Kafka 侧归一错误码
        super().__init__(message)

class ConfigError(CloudSyncError):
    """配置/凭据加载失败。"""
    def __init__(self, message: str):
        super().__init__(message, code=50002, error_code="CONFIG_ERROR")

class CredentialError(CloudSyncError):
    """云账号凭据缺失或无效。"""
    def __init__(self, message: str):
        super().__init__(message, code=50003, error_code="AUTH_FAILED")

class AuthFailedError(CloudSyncError):
    """云 API 认证失败。"""
    def __init__(self, provider: str, detail: str):
        super().__init__(f"Cloud auth failed for '{provider}': {detail}",
                         code=50202, error_code="AUTH_FAILED")

class RateLimitError(CloudSyncError):
    """云 API 限流（重试耗尽后抛出）。"""
    def __init__(self, provider: str, detail: str):
        super().__init__(f"Cloud API rate limited for '{provider}': {detail}",
                         code=50203, error_code="RATE_LIMITED")

class AdapterError(CloudSyncError):
    """云 API 调用失败（非限流/认证类）。"""
    def __init__(self, provider: str, detail: str):
        super().__init__(f"Cloud API error for '{provider}': {detail}",
                         code=50201, error_code="API_ERROR")

class ValidationError(CloudSyncError):
    """数据校验异常。"""
    def __init__(self, message: str, errors: list[dict] | None = None):
        super().__init__(message, code=40001, error_code="VALIDATION_ERROR")
        self.errors = errors

class ReconcileError(CloudSyncError):
    """差集对账异常（对账失败必须 abort 本轮，禁止发 delete）。"""
    def __init__(self, message: str):
        super().__init__(message, code=50004, error_code="RECONCILE_ERROR")

class KafkaPublishError(CloudSyncError):
    """Kafka 发送失败。"""
    def __init__(self, topic: str, detail: str):
        super().__init__(f"Kafka publish to '{topic}' failed: {detail}",
                         code=50204, error_code="KAFKA_ERROR")
```

## 云 API 错误码归一

SDK 错误必须归一到以下 `error_code` 后记 ERROR 日志，禁止直接透传 SDK 原始异常类型做分支：

| error_code | 触发场景 | 处置 |
|---|---|---|
| `RATE_LIMITED` | SDK 限流/Throttling 错误 | 指数退避重试 3 次，重试记 WARNING；耗尽后本轮失败 |
| `AUTH_FAILED` | 凭据无效/权限不足 | 不重试，该任务本轮 ERROR，不影响其他任务 |
| `API_ERROR` | 其他云 API 错误（分页错误、服务端错误等） | 整轮 abort，**禁止发 delete**（防 API 抖动批量误删） |

## 异常处理原则

### 1. 尽早抛出，引擎主循环统一收敛

adapter/reconcile 层直接抛出业务异常，由引擎主循环按任务粒度捕获收敛：

```python
# 正确 - adapter 层直接抛出
async def list_resources(self, account: AccountConfig, resource_type: str):
    if account is None:
        raise CredentialError("Account credential missing")
    ...

# 错误 - 在 adapter 层处理并返回空列表（会触发差集误删！）
async def list_resources(self, account, resource_type):
    try:
        ...
    except Exception:
        return []   # 绝对禁止：空 seen 集合会导致全量 delete
```

### 2. 禁止空 except

```python
# 禁止 - 捕获所有异常不处理
try:
    await fetch_page()
except Exception:
    pass  # 绝对禁止

# 正确 - 明确捕获并记录
try:
    await fetch_page()
except AdapterError:
    logger.error("Cloud API call failed", extra={"provider": "aliyun", "error_code": "API_ERROR"})
    raise  # 重新抛出，交给引擎收敛
```

### 3. 禁止使用异常控制正常业务流程

```python
# 禁止 - 用异常做流程控制
try:
    account = get_account(provider, target_id)
except CredentialError:
    account = default_account()

# 正确 - 用条件判断
account = account_registry.get((provider, target_id))
if account is None:
    logger.error("Task has no matching credential", extra={"task_id": task.id})
    return
```

## 引擎任务级收敛（替代全局异常处理器）

本项目无 HTTP 层，异常收敛点在 `scheduler/engine.py` 主循环。**单任务异常记 ERROR 后跳过，绝不影响其他任务**：

```python
async def _run_task(self, task: SyncTask) -> None:
    try:
        await self._run_round(task)
    except CloudSyncError as e:
        logger.error("Sync task failed with business error",
                     extra={"task_id": task.id, "provider": task.provider,
                            "error_code": e.error_code})
    except Exception:
        logger.exception("Sync task failed with unexpected error",
                         extra={"task_id": task.id, "provider": task.provider})
```

## 外部调用重试策略

对外部服务调用必须实现重试：

```python
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(RateLimitError),
    reraise=True,
)
async def call_cloud_api(account: AccountConfig) -> dict:
    """调用云 API，限流自动退避重试。"""
    # ...
```

### 重试规则

| 场景 | 最大重试次数 | 退避策略 |
|------|-------------|----------|
| 云 SDK 调用（限流） | 3 | 指数退避 1s-10s，重试记 WARNING |
| 数据库连接（共享库查询） | 5 | 指数退避 0.5s-5s |
| Kafka 发送 | 3 | 固定间隔 1s |

### 不可重试场景

`AUTH_FAILED`、配置错误、数据校验失败**不重试**，直接记 ERROR 结束本轮。

## 异步任务错误处理

```python
async def run_round(task: SyncTask) -> None:
    logger = logging.getLogger("cloudsync.scheduler.engine")
    try:
        logger.info("Sync round started", extra={"task_id": task.id, "provider": task.provider})
        # 拉取 → 归一化 → 对账 → 发送 ...
        logger.info("Sync round completed",
                    extra={"task_id": task.id, "provider": task.provider,
                           "upserted": upserted, "deleted": deleted,
                           "duration_ms": duration_ms})
    except CloudSyncError as e:
        logger.error("Sync round failed with business error",
                     extra={"task_id": task.id, "error_code": e.error_code})
        raise
    except Exception:
        logger.exception("Sync round failed with unexpected error",
                         extra={"task_id": task.id})
        raise
```

## 禁止事项

1. 禁止使用 `except Exception: pass` 静默吞掉异常
2. 禁止在拉取失败时返回空集合代替抛异常（会导致差集误删）
3. 禁止在业务层捕获异常后不重新抛出（除非有明确的恢复策略）
4. 禁止使用异常作为正常业务流程的控制手段
5. 禁止直接抛出 `Exception`、`ValueError` 等内置异常，必须使用引擎自定义异常
6. 禁止日志输出凭据内容；adapter 初始化失败只记 ERROR + error_code
