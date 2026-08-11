---
name: bingops-project-structure
description: 云同步引擎（cloud-syncer）项目目录结构、Python 3.13 编码规范与技术栈约定。适用于创建新模块、新文件、新功能时遵循统一的项目组织方式。当进行任何代码开发、创建新文件或模块时必须遵循此规范。
---

# 项目结构与编码规范

## 项目定位

cloud-syncer 是 **Kafka 生产者 + 定时轮询引擎**（纯后台进程，无 HTTP 接口）：读 bingops 共享库 `cmdb_sync_tasks` 调度、经 Provider Adapter 拉取云资源归一化、差集对账后发 Kafka 给 bingops 消费端。设计事实源：`docs/cloud-sync-design.md`。

## 技术栈

| 组件 | 版本/选型 | 说明 |
|------|-----------|------|
| Python | 3.13+ | 使用最新特性（`type` 语句、改进的类型推断） |
| 异步框架 | asyncio | 全异步 I/O |
| ORM | SQLAlchemy 2.0+ | async 模式，只映射 bingops 共享库表，不做 DDL |
| 数据库 | PostgreSQL（bingops 共享库） | SELECT + 仅 `cmdb_sync_tasks.last_synced_at` UPDATE |
| 消息队列 | aiokafka | Kafka 生产者（topic：`cloud-sync-{provider}`） |
| 数据校验 | Pydantic v2 | NormalizedResource / CloudResourceMessage / 配置 |
| 配置管理 | pydantic-settings + PyYAML | env（`CLOUDSYNC_` 前缀）+ accounts.yaml 凭据 |
| 调度 | croniter | cron 表达式解析 |
| 日志 | structlog + logging | 结构化日志（见 bingops-logging skill） |
| 测试 | pytest + pytest-asyncio | 异步测试 |
| 代码质量 | ruff | lint + format |
| 类型检查 | mypy | 严格模式 |

## 目录结构

```
cloud-syncer/
├── pyproject.toml              # 项目配置与依赖
├── cloudsync/                  # 主包
│   ├── __init__.py
│   ├── main.py                 # asyncio 入口（setup_logging → 加载配置/凭据 → 启动引擎）
│   ├── core/                   # 核心基础模块
│   │   ├── config.py           # 配置管理（pydantic-settings，env 前缀 CLOUDSYNC_）
│   │   ├── logging.py          # setup_logging / JsonFormatter
│   │   ├── exceptions.py       # 异常类定义（CloudSyncError 层级）
│   │   ├── accounts.py         # accounts.yaml 凭据加载（严禁日志输出凭据）
│   │   └── db.py               # 共享库 async engine / session 工厂
│   ├── schemas/                # Pydantic 模型
│   │   ├── normalized.py       # NormalizedResource（归一化中间态）
│   │   └── messages.py         # CloudResourceMessage（Kafka 消息契约）
│   ├── scheduler/              # cron 调度、任务表热加载
│   │   ├── models.py           # cmdb_sync_tasks / cmdb_resources 只读 ORM 映射
│   │   ├── tasks.py            # 任务加载/热重载/last_synced_at 回写
│   │   └── engine.py           # 主循环与单轮执行编排
│   ├── adapters/               # 每云厂商一个包，每资源类型一个模块
│   │   ├── base.py             # ProviderAdapter Protocol + 注册表
│   │   ├── aliyun/
│   │   └── gcp/
│   ├── normalize/              # 标签归一化、status 词表、合成 ID、content hash
│   ├── reconcile/              # 差集软删、ACK enrichment
│   └── kafka/                  # producer 封装
├── deploy/                     # accounts.yaml 模板（无真值）、Dockerfile
├── docs/                       # 设计文档
└── tests/                      # 测试
```

**组织规则**：每个云厂商在 `adapters/` 下独立子包，内部按资源类型一个模块（如 `adapters/aliyun/ecs.py`）；CLB/NLB 等入口形态差异大的资源必须拆独立模块。

## 分层架构

执行链路：`Scheduler/Engine → Adapter/Reconcile → Repository（共享库只读）+ Kafka Producer`

| 层 | 职责 | 禁止 |
|----|------|------|
| **Scheduler/Engine** | cron 触发、任务热加载、单轮编排、超时保护 | 禁止包含云 API 细节 |
| **Adapter** | 云 SDK 拉取 → NormalizedResource 归一化 | 禁止写数据库、禁止直接发 Kafka |
| **Reconcile** | 差集软删对账、ACK enrichment | 禁止发 delete 以外的事件语义变更 |
| **Repository（db.py + scheduler/models.py）** | 共享库只读查询、last_synced_at 回写 | 禁止任何业务表写入（单一写者原则） |
| **Kafka Producer** | 消息序列化与发送重试 | 禁止包含业务逻辑 |

**边界纪律**（设计文档 §1）：本项目只生产消息；业务策略唯一事实源是 `cmdb_sync_tasks`；凭据不进数据库不进 git；字段白名单过滤由 bingops 消费端负责。

## Python 3.13 编码规范

### 类型注解（强制）

```python
# 使用 Python 3.13 现代类型语法
type AccountID = str
type ResourceList = list[NormalizedResource]

# 函数签名必须有类型注解
async def get_account(provider: str, account_id: str) -> AccountConfig | None:
    ...

# 禁止使用旧式 Optional
async def get_account(account_id: str) -> Optional[AccountConfig]:  # 禁止
async def get_account(account_id: str) -> AccountConfig | None:      # 正确
```

### Docstring（公共函数必须有）

使用 Google 风格：

```python
async def list_resources(account: AccountConfig, resource_type: str) -> list[NormalizedResource]:
    """List normalized resources of a given type from the cloud account.

    Args:
        account: Cloud account credentials and scope.
        resource_type: CMDB model code, e.g. "aliyun_ecs".

    Returns:
        Normalized resources fetched from the provider API.

    Raises:
        RateLimitError: Provider API rate limited after retries.
        AuthFailedError: Credential rejected by the provider.
    """
```

### 异步优先

```python
# 所有 I/O 操作必须使用 async
async def fetch_tasks() -> list[SyncTask]:    # 正确
def fetch_tasks() -> list[SyncTask]:          # 禁止（I/O 操作）

# CPU 密集/同步 SDK 使用 run_in_executor
import asyncio
result = await asyncio.to_thread(sync_sdk_call, arg)
```

### 命名规范

| 对象 | 命名风格 | 示例 |
|------|----------|------|
| 模块/包 | snake_case | `soft_delete.py` |
| 类 | PascalCase | `ProviderAdapter` |
| 函数/方法 | snake_case | `list_resources` |
| 常量 | UPPER_SNAKE_CASE | `MAX_RETRY_COUNT` |
| 私有成员 | 前导下划线 | `_internal_state` |

### 配置管理

使用 `pydantic-settings`，禁止硬编码配置值：

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    model_config = {"env_prefix": "CLOUDSYNC_"}

    database_url: str
    kafka_bootstrap_servers: str = "localhost:9092"
    accounts_file: str = "/etc/cloudsync/accounts.yaml"
    task_reload_interval: int = 60
    debug: bool = False
    log_level: str = "INFO"

settings = Settings()
```

## ORM 映射规范

本项目**只映射 bingops 共享库的既有表**，禁止创建 DDL / 迁移（表归 bingops 所有）：

```python
# scheduler/models.py
class Base(DeclarativeBase):
    pass

class SyncTaskRow(Base):
    __tablename__ = "cmdb_sync_tasks"
    # 只映射引擎需要的列；仅 SELECT + UPDATE(last_synced_at)
```

## 禁止事项

1. 禁止在代码中硬编码数据库连接串、密钥等敏感配置
2. 禁止写任何 CMDB 业务表（唯一例外：`cmdb_sync_tasks.last_synced_at`）
3. 禁止循环导入（使用 `TYPE_CHECKING` 解决）
4. 禁止使用 `import *`
5. 禁止提交未通过 `ruff check` 的代码
6. 禁止凭据（accounts.yaml）进 git、进镜像、进数据库
