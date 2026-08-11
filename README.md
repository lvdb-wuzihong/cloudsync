# cloud-syncer

云资源同步引擎：**Kafka 生产者 + 定时轮询**，拉取云厂商（aliyun / gcp）资源归一化后发消息给 bingops CMDB 消费端。

- 设计事实源：[docs/cloud-sync-design.md](docs/cloud-sync-design.md)
- 编码规范：`.qoder/skills/`（logging / error-handling / project-structure）

## 架构速览

```
cmdb_sync_tasks（bingops 共享库，60s 热加载）+ accounts.yaml（K8s Secret 挂载）
        ↓
Scheduler（cron 触发）→ Provider Adapters（拉取归一化）→ 差集对账（读 cmdb_resources）
        ↓
Kafka Producer → cloud-sync-{provider} → bingops cloud_consumer
```

边界纪律：本项目只生产消息，不写 CMDB 业务表（唯一例外：回写 `cmdb_sync_tasks.last_synced_at`）。

## 快速开始

```bash
pip install -e ".[dev]"

# 配置环境变量（CLOUDSYNC_ 前缀）
export CLOUDSYNC_DATABASE_URL=postgresql+asyncpg://user:pass@host/bingops
export CLOUDSYNC_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export CLOUDSYNC_ACCOUNTS_FILE=deploy/accounts.example.yaml

python -m cloudsync.main
```

## 目录

```
cloudsync/
├── core/        # config / logging / exceptions / accounts / db
├── schemas/     # NormalizedResource / CloudResourceMessage
├── scheduler/   # 任务表热加载、cron 调度、单轮编排
├── adapters/    # aliyun / gcp（每资源类型一模块）
├── normalize/   # 标签归一化、status 词表、内容哈希、合成 ID
├── reconcile/   # 差集软删、ACK enrichment
└── kafka/       # producer 封装
```

## 当前状态

骨架阶段：调度、对账、Kafka 发送链路已就绪；aliyun/gcp adapter 为空壳，资源拉取实现按路线图 P1-P4 迭代。
