# 云同步引擎（cloud-syncer）设计文档

> 本文是独立新项目 **cloud-syncer** 的开工设计文档，可直接拷贝到新仓库使用。
> 配套文档：`cmdb-design.md`（§6.4 标签归一化 / §9.2 云同步机制）、`cmdb-model-presets.md`（模型字段与关系约束、附录 B 决策）。
> 配套 Skills（一并拷贝到新项目 `.qoder/skills/`）：`bingops-logging`、`bingops-error-handling`、`bingops-api-response`（仅当暴露 HTTP 接口时）、`bingops-project-structure`。
>
> 本文与 `cmdb-design.md §9.2` 不一致处以本文为准（§9.2 的模型映射表为早期草稿，模型 code 以 `cmdb-model-presets.md` 为准）。

---

## 1. 定位与边界

cloud-syncer 是 **Kafka 生产者 + 定时轮询引擎**，不写任何 CMDB 业务表（唯一例外见 §2.3）。

```
accounts.yaml（K8s Secret 挂载，凭据）          cmdb_sync_tasks（bingops 库，业务开关/调度）
        │                                              │ 读（60s 热加载）
        └──────────────┬───────────────────────────────┘
                       ↓
              cloud-syncer（本项目）
              ├── Scheduler：按 task.schedule cron 触发
              ├── Provider Adapters：aliyun / gcp SDK 拉取 → 归一化
              ├── 差集对账：读 cmdb_resources 计算消失资源 → delete 事件
              └── Kafka Producer → cloud-sync-{provider}
                       ↓
              bingops cloud_consumer（既有，不改动契约）
              ├── 门控：cmdb_sync_tasks 默认拒绝
              ├── resource_type → model_code 直映射 upsert
              └── 云标签差异同步
```

**边界纪律：**

1. 本项目**只生产消息**，CMDB 落库/建边/审计全部由 bingops 消费端负责（单一写者原则）；
2. 业务策略（同步谁、同步什么、频率、启停）唯一事实源是 `cmdb_sync_tasks`，本项目不维护第二份配置；
3. 凭据是部署层关注点，不进数据库、不进 git（§3）；
4. 模型字段定义（`cmdb_model_fields`）不在本项目过滤——生产者产出全量归一化属性，**字段白名单过滤由 bingops 消费端负责**（与 K8s 链路同构，删字段自动生效）。

---

## 2. 调度模型

### 2.1 表驱动

读取 `cmdb_sync_tasks` 中 `task_type='cloud'` 的行：

| 列 | 引擎语义 |
|---|---|
| `target_id` | 云账号 ID，必须与 `accounts.yaml` 中的 `account_id` 一致 |
| `provider` | aliyun / gcp，决定用哪个 adapter 与 topic |
| `enabled` | false → 跳过该任务 |
| `schedule` | cron 表达式，每个资源类型按 §5 的默认频率兜底 |
| `resource_types` | 模型 code 白名单；**空列表 = 该 provider 默认全量集**（§5） |
| `last_synced_at` | 本轮成功后由引擎回写（唯一写权限） |

**默认拒绝**：表里没有的账号一律不碰（与 bingops 消费端门控语义一致）。

### 2.2 运行形态

- **v1 单副本部署**（K8s Deployment replicas=1）。重复生产者只会造成 API 配额浪费（消费端幂等），但没必要；
- v2 水平扩展方案：按 `cmdb_sync_tasks.id` 取 `pg_try_advisory_lock`，抢到的实例执行，抢不到跳过——届时再实现；
- 启动时全量加载任务表，之后每 60s 重读一次，任务增删/启停**热生效**，不重启；
- 单轮执行超时保护：超过 `schedule` 间隔的 80% 告警（日志 WARNING + duration_ms）。

### 2.3 共享库访问边界（决策 D2）

cloud-syncer 连接 **bingops 同一个 PostgreSQL**，权限最小化：

| 表 | 权限 | 用途 |
|---|---|---|
| `cmdb_sync_tasks` | SELECT + UPDATE(last_synced_at) | 调度输入 / 执行记录 |
| `cmdb_resources` | SELECT | 差集软删对账、ACK enrichment 映射查询 |

理由：内部单体平台（见项目定位），调度与对账走 HTTP API 会引入可用性耦合与分页复杂度；读共享库是务实选择。若未来拆库，把这两处查询替换为 bingops 内部 API 即可，接口面很小。

---

## 3. 凭据管理（决策 D1）

**v1：K8s Secret 挂载 `accounts.yaml`**，结构：

```yaml
providers:
  aliyun:
    - account_id: "1234567890"          # 必须等于 cmdb_sync_tasks.target_id
      display_name: 生产主账号
      access_key_id: "LTAI***"
      access_key_secret: "***"
      regions: [cn-beijing, cn-shanghai]   # 拉取范围；空 = SDK 默认/全量
  gcp:
    - account_id: "my-gcp-project"
      display_name: GCP 主项目
      service_account_json: |
        { ... }
      regions: [asia-east2]
```

纪律：

1. 文件经 K8s Secret 挂载，**不进 git、不进镜像、不进数据库**；
2. 不用 `.env` 承载多账号结构（python-dotenv 仅支持纯 KEY=VALUE，结构化配置用 YAML）；
3. 日志严禁输出凭据字段；adapter 初始化失败记 ERROR + error_code，不打印 secret 内容；
4. `accounts.yaml` 有账号但 `cmdb_sync_tasks` 无任务 → 不同步（业务表为准）；反之有任务无凭据 → 该任务 ERROR 告警，不影响其他任务。

**v2 备选**（账号多了再做）：`cmdb_cloud_accounts` 表 + UI 管理 + 字段级加密；届时 accounts.yaml 退化为 bootstrap。

---

## 4. 消息契约

**事实源：bingops `schemas/cmdb/kafka_messages.py` 的 `CloudResourceMessage`**，逐字段对齐，不得新增/改名字段（需扩展时先改 bingops schema 并同步消费端）。

| 字段 | 生产者填法 |
|---|---|
| `provider` | aliyun / gcp |
| `resource_type` | **等于 CMDB 模型 code**（如 `aliyun_ecs`、`gcp_vpc`），消费端 `get_model_by_code` 直映射 |
| `provider_id` | 云原始 ID；合成 ID 规则照 `cmdb-model-presets.md` 附录 B #19（如 `gcp_firewall` = `fw:{project}:{vpc}`） |
| `cloud_account` | 账号 ID（= target_id） |
| `event_type` | upsert / delete |
| `resource_version` | **内容哈希**（决策 D3）：`sha256(json.dumps({name,region,zone,status,attributes,cloud_tags}, sort_keys=True))[:16]` |
| `name/region/zone/status` | 归一化后填写；status 归一到 `running/stopped/maintenance/unknown` 词表 |
| `attributes` | 扩展属性，**key 必须等于模型字段 code**（照 presets 清单），通用层字段（name/provider/region…）不得塞进 attributes |
| `cloud_tags` | 归一化（§6.4：key 小写+短横线，raw_key 保留原值在 adapter 内部处理） |
| `parent_provider_id/parent_resource_type` | 从属关系提示（如 ECS→vswitch），消费端 v2 建边用；v1 消费端忽略，但生产者**必须填**，避免二次返工 |
| `timestamp` | UTC |

Topic：`cloud-sync-{provider}`（与 bingops 订阅正则 `cloud-sync-.*` 匹配）。

**D3 说明**：云 API 无 K8s resourceVersion 序语义，用内容哈希做"有无变更"检测；bingops 消费端配套改为哈希相等即跳过（见 §8）。

---

## 5. Provider Adapter 与首批范围

### 5.1 Adapter 接口

```python
class ProviderAdapter(Protocol):
    provider: str

    async def list_resources(
        self, account: AccountConfig, resource_type: str,
    ) -> AsyncIterator[NormalizedResource]: ...
```

`NormalizedResource` 即 §4 字段的结构化中间态（不含 event_type）。每个 adapter 一个包：`adapters/aliyun/`、`adapters/gcp/`，内部按资源类型一个模块；**跨厂商同构资源（VPC/子网/安全组/计算/云数据库）字段 code 严格对齐 presets**（纪律：`cidr_block`、`memory_gb`、`rules`+`rules_hash` 等跨云同名）。

### 5.2 首批资源清单与频率（presets 附录 B #6）

| 频率档 | 资源（模型 code） |
|---|---|
| 5–10min | `aliyun_ecs`、`gcp_compute` |
| 30min | `aliyun_rds`、`aliyun_redis`、`gcp_cloudsql`、`aliyun_oss` |
| 1h | 网络类：`aliyun_vpc/vswitch/security_group/clb/nlb/nat_gateway/eip/disk/nas`、`gcp_vpc/subnet/firewall/disk` |
| 1h 全量对账 | `dns_zone/dns_record`（P3，DNS 独立分组） |
| 随 ECS 档 | `aliyun_account`/`gcp_account` 账号根节点（provider_id=账号 ID，建树根） |

`resource_types` 白名单为空时的默认集 = 上表该 provider 全量。CLB/NLB **必须拆两个 adapter 模块**（决策记忆：入口形态差异导致关系重建逻辑根本不同）。

### 5.3 规则级 diff（presets 附录 B #8）

安全组/防火墙的 `rules`、NAT 的 `snat/dnat` 条目：adapter 计算 `rules_hash` 填入 attributes；哈希不变时整条消息的 content hash 自然不变 → 消费端自动跳过，无需额外逻辑。

---

## 6. 差集软删（决策 D4）

每轮（account × resource_type）拉取完成后：

```
seen_ids   = 本轮拉到的 provider_id 集合
stored_ids = SELECT provider_id FROM cmdb_resources
             WHERE model_id=<model> AND provider=<p> AND cloud_account=<acc>
               AND source='discovery' AND deleted_at IS NULL
for pid in stored_ids - seen_ids:
    emit CloudResourceMessage(event_type=delete, provider_id=pid, ...)
```

纪律：

1. **只软删 `source='discovery'` 行**，`manual` 录入的资源云侧看不到也不碰；
2. API 拉取部分失败（分页错误/限流）时**不发 delete**——整轮失败直接 abort 并记 ERROR，防"API 抖动 → 批量误删"；
3. 与 K8s 的 #15 快照差集语义同构，未来消费端若统一快照会话逻辑可复用。

---

## 7. ACK 集群元数据 Enrichment

目标：把 ACK API 的元数据（API 端点、Pod/Service CIDR、节点数、VPC、K8s 版本、地域）合并进 informer 已创建的 `k8s_cluster` 行。

**映射方案（决策 D5，对应前序讨论 b 方案）**：

1. bingops 侧给 `k8s_cluster` 模型加字段 `cloud_cluster_id`（string，非 builtin，"同步保留"分组）+ v8 迁移（见 §8）；
2. engine 调 ACK `DescribeClusters` 列出原生集群（native_id, name, meta…）；
3. 读 `cmdb_resources` 中 `k8s_cluster` 行：优先按 `fields->>'cloud_cluster_id' = native_id` 匹配，未命中回退 `name = ACK name` 精确匹配并记 WARNING（提示运维补录 `cloud_cluster_id`）；
4. 命中后发 `CloudResourceMessage(resource_type='k8s_cluster', provider_id=<informer cluster_id>, cloud_account=<informer cluster_id>, attributes={api_endpoint, pod_cidr, service_cidr, node_count, vpc_id, k8s_version, ...}, region=...)`；
5. 消费端按 key 合并 attributes（§8 配套改动），`cluster_type` 等 informer 字段不被覆盖。

`cloud_cluster_id` 首值由 engine 在回退匹配命中时**不写库**（生产者不写业务表），由运维在 UI 补录；v2 可放开 engine 写该单字段。

---

## 8. bingops 侧配套改动清单（上线前置）

云引擎上线前/同期，bingops 仓库需完成：

| # | 改动 | 说明 |
|---|---|---|
| C1 | `cloud_consumer` fields **按 key 合并** | 现状 `existing.fields = message.attributes` 整包覆盖会冲掉 `cluster_type`；改为 `{**existing.fields, **incoming}` |
| C2 | 云路径变更检测改**内容哈希** | `resource_version` 相等 → 跳过（无实质变更）；不等 → update。替换现有 `_version_lte` 数值比较在云路径的误用 |
| C3 | 云路径加**模型字段白名单过滤** | 复用 K8s 链路 `filter_by_model_fields`，attributes 死键不落库 |
| C4 | v8 迁移：`k8s_cluster` 模型注册 `cloud_cluster_id` 字段 | `cmdb_model_fields` INSERT（is_builtin=false） |
| C5 | 云关系重建第二批 | `relationship_builder` 增加云边规则：#33 盘→ECS、#35/#36 node 承载、#38/#39 LB 桥接、#43/#44 CSI 桥接、#10–#16 从属树等（presets §4） |
| C6 | （可选，NAT 同步前）`cmdb_relates_to` 加 `kind` 列 | presets 附录 B #1 |

C1–C4 是云引擎上线的硬前置；C5 可随 P1/P2 分批。

---

## 9. 日志与可观测

遵循 `bingops-logging` skill（logger 前缀改为新项目名，如 `cloudsync.*`）：

- 每轮任务结束 INFO：`extra={task_id, provider, account, resource_type, upserted, deleted, duration_ms}`；
- 单资源类型耗时 >100ms 必记 duration_ms（skill 硬规则，云 API 调用必然超）；
- 云 API 错误 ERROR + `error_code`（SDK 错误码归一：`RATE_LIMITED`/`AUTH_FAILED`/`API_ERROR`）；限流退避重试 3 次（指数退避），重试记 WARNING；
- 凭据字段永不入日志（skill 禁止事项）。

---

## 10. 新项目目录建议

```
cloud-syncer/
├── cloudsync/
│   ├── core/            # config / logging / exceptions（照 skills 规范）
│   ├── scheduler/       # cron 调度、任务表热加载
│   ├── adapters/
│   │   ├── aliyun/      # 每资源类型一模块
│   │   └── gcp/
│   ├── normalize/       # 标签归一化、status 词表、合成 ID、content hash
│   ├── reconcile/       # 差集软删、ACK enrichment
│   └── kafka/           # producer 封装
├── docs/                # 拷贝本设计文档 + presets + design 相关章节
├── .qoder/skills/       # 拷贝 4 个 bingops skills
└── deploy/              # accounts.yaml 模板（无真值）、Dockerfile
```

技术栈建议与 bingops 对齐：Python 3.13 + asyncio + aiokafka + 官方 SDK（alibabacloud-v2 / google-cloud-compute），Pydantic v2 承载 NormalizedResource。

---

## 11. 路线图

| 阶段 | 内容 | 验收 |
|---|---|---|
| P1 | 骨架 + aliyun 计算/网络档（ecs/vpc/vswitch/security_group/account）+ 差集软删 | CMDB 出现 aliyun 资源，删一台 ECS 后行软删 |
| P2 | aliyun 存储/数据库/入口档（disk/nas/oss/rds/redis/clb/nlb/nat/eip）+ ACK enrichment（§7，依赖 C1/C4） | 集群详情页扩展字段有值；CLB→ECS 后端边建立（C5 分批） |
| P3 | gcp 全档 + DNS 分组 | GCE/CloudSQL 入库；dns_record→入口边 |
| P4 | AWS + 多实例 advisory lock | — |

---

## 附录：决策记录

| # | 决策 | 理由 | 备选（否决） |
|---|---|---|---|
| D1 | 凭据走 K8s Secret 挂载 accounts.yaml | 部署层关注点；多账号结构化；不进 DB/git | DB 表+UI（v2 再做）；.env（不支持结构） |
| D2 | 共享 bingops PostgreSQL 只读+last_synced_at 回写 | 内部单体平台务实选择；接口面小易替换 | HTTP API 耦合可用性 |
| D3 | resource_version = 内容哈希 | 云无序语义；哈希相等即无变更，与消费端"无变更跳过"同构 | 时间戳（时钟漂移/格式乱）；自增（无跨轮一致性） |
| D4 | 差集软删在生产者侧做 | 生产者持有本轮全量 seen 集合，最自然；消费端保持无状态 | 消费端快照会话（K8s #15 同款，云无快照通道） |
| D5 | ACK enrichment 用 cloud_cluster_id 映射字段 | 不改 informer cluster_id，零数据迁移；映射可 UI 维护 | informer 改用原生 ID（全量 provider_id 迁移痛） |
| D6 | 单副本调度，v2 advisory lock | 当前账号规模小；消费端幂等兜底重复 | 上来就分布式锁（过度设计） |
