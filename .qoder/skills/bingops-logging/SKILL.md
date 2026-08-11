---
name: bingops-logging
description: 云同步引擎（cloud-syncer）日志记录格式标准与实现规范。适用于所有涉及日志输出的场景，包括调度循环、adapter 拉取、Kafka 发送、异常记录等。当编写任何包含日志调用的 Python 代码时必须遵循此规范。
---

# 云同步引擎日志记录规范

## 日志框架

统一使用 Python 标准库 `logging` 模块，禁止使用 `print()` 输出日志。推荐结合 `structlog` 实现结构化日志。

## 实现入口（已内置，禁止绕过）

日志基础设施已集中落地，业务代码只需 `logger = logging.getLogger(__name__)` 直接用：

| 组件 | 位置 | 职责 |
|------|------|------|
| `setup_logging()` | `cloudsync/core/logging.py` | 统一配置根 logger（stdout + 可选文件双通道）；由 `main.py` 在启动调度引擎前调用一次 |
| `JsonFormatter` | `cloudsync/core/logging.py` | 生产环境（`debug=false`）JSON 单行格式；开发环境走控制台格式 |

本项目是**纯后台进程，无 HTTP 入口**，不存在 request_id 中间件；`request_id` 仅作为可选 extra 字段保留（如未来接入追踪系统）。

**输出纪律**：双通道输出——

- **stdout 通道**：始终开启，供 `kubectl logs` / 控制台排障；
- **文件通道**：配置 `CLOUDSYNC_LOG_DIR` 后开启，供采集器（Filebeat / logtail）落盘采集；未配置则不落盘。

文件通道行为（由 `setup_logging()` 统一实现）：

- 当前文件：`<CLOUDSYNC_LOG_DIR>/cloudsync.log`，JSON 单行格式；
- 轮转：每天午夜轮转，旧文件 gzip 压缩为 `cloudsync.log.YYYY-MM-DD.gz`；
- 保留期：`CLOUDSYNC_LOG_RETENTION_DAYS`（默认 7 天），超期自动删除。

K8S 部署时日志目录挂 emptyDir（或 hostPath），采集器读该目录；Pod 重建不要求日志持久化。禁止在业务代码里自行 `basicConfig`、加 Handler 或重复配置根 logger。

## 日志级别定义

| 级别 | 使用场景 |
|------|----------|
| `DEBUG` | 开发调试信息，生产环境关闭 |
| `INFO` | 关键业务节点、任务轮次开始/完成、Kafka 批次发送完成 |
| `WARNING` | 可恢复的异常情况，如降级处理、限流重试、配置缺失使用默认值、单轮耗时超阈值 |
| `ERROR` | 不可恢复错误，如云 API 调用失败、Kafka 发送失败、凭据加载失败、数据校验失败 |
| `CRITICAL` | 系统级故障，如数据库连接中断、Kafka broker 长期不可用 |

## 日志格式标准

### 控制台输出格式（开发环境）

```
[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s
```

时间格式：`YYYY-MM-DD HH:MM:SS`

### JSON 结构化格式（生产环境）

每条日志必须包含以下字段：

```json
{
  "timestamp": "2026-07-10T14:30:00.000Z",
  "level": "INFO",
  "logger": "cloudsync.scheduler.engine",
  "message": "Sync round completed",
  "module": "engine",
  "function": "run_round",
  "line": 42,
  "extra": {}
}
```

### 必填字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp` | string | ISO 8601 格式，UTC 时区 |
| `level` | string | 日志级别大写 |
| `logger` | string | logger 名称，格式：`cloudsync.<module>.<submodule>` |
| `message` | string | 日志消息，英文，使用动宾结构 |
| `module` | string | Python 模块名 |
| `function` | string | 函数名 |
| `line` | int | 行号 |

### 可选上下文字段

| 字段 | 类型 | 触发条件 |
|------|------|----------|
| `task_id` | int | 同步任务执行时（cmdb_sync_tasks.id） |
| `provider` | string | 涉及云厂商时（aliyun / gcp） |
| `account` | string | 涉及云账号时（account_id） |
| `resource_type` | string | 涉及资源类型时（模型 code） |
| `error_code` | string | 记录错误时（如 `RATE_LIMITED`/`AUTH_FAILED`/`API_ERROR`） |
| `duration_ms` | float | 记录耗时操作时 |

## Logger 命名规范

```python
# 正确
logger = logging.getLogger("cloudsync.scheduler.engine")
logger = logging.getLogger("cloudsync.adapters.aliyun.ecs")
logger = logging.getLogger("cloudsync.kafka.producer")

# 错误 - 禁止使用 __name__ 以外的无意义名称
logger = logging.getLogger("mylogger")
logger = logging.getLogger()
```

推荐在模块顶部使用 `__name__` 快捷创建：

```python
import logging
logger = logging.getLogger(f"cloudsync.{__name__}")
```

## 日志消息规范

```python
# 正确 - 英文、动宾结构、携带上下文
logger.info("Sync round completed", extra={"task_id": task_id, "provider": provider, "duration_ms": 1234.5})
logger.error("Failed to call cloud API", extra={"provider": "aliyun", "error_code": "RATE_LIMITED"})

# 错误 - 中文消息、无上下文
logger.info("同步完成")
logger.error(f"调用失败: {provider}")  # 禁止 f-string 拼接
```

### 消息规则

1. 使用英文，动宾结构（如 `Sync round completed`, `Failed to fetch resources`）
2. 禁止使用 f-string 或 `%` 拼接变量，使用 `extra` 参数传递上下文
3. 禁止在日志中输出敏感信息（access_key_secret、service_account_json 等凭据字段永不入日志）

## 性能日志

耗时超过 **100ms** 的操作必须记录耗时：

```python
import time

start = time.perf_counter()
result = await some_slow_operation()
duration_ms = (time.perf_counter() - start) * 1000
logger.info("Slow operation completed", extra={"duration_ms": round(duration_ms, 2)})
```

云 API 调用必然超过 100ms，adapter 拉取、Kafka 发送、差集对账查询均必须记 `duration_ms`。

## 禁止事项

1. 禁止使用 `print()` 代替日志
2. 禁止在循环体内输出 DEBUG 级别日志（除非明确调试）
3. 禁止日志中输出密码、Token、API Key 等敏感信息
4. 禁止捕获异常后不记录日志直接 `pass`
5. 禁止使用 f-string 在日志消息中拼接变量
