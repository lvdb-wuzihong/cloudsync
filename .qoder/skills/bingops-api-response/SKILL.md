---
name: bingops-api-response
description: 运维平台 API 响应数据结构统一规范。适用于所有 REST API 端点的返回值设计，包括成功响应、错误响应、分页响应等。当创建或修改任何 API 接口时必须遵循此规范。
---

# API 响应结构规范

## 统一响应信封

所有 API 响应必须使用统一的信封结构，禁止直接返回裸数据。

### 成功响应

```json
{
  "code": 0,
  "message": "success",
  "data": {},
  "request_id": "req-abc123"
}
```

### 错误响应

```json
{
  "code": 40001,
  "message": "Validation failed",
  "data": null,
  "request_id": "req-abc123",
  "errors": [
    {"field": "hostname", "detail": "hostname is required"}
  ]
}
```

## 字段定义

### 顶层字段（必填）

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | int | 业务状态码，`0` 表示成功，非零表示失败 |
| `message` | string | 人类可读的描述，英文 |
| `data` | any \| null | 响应数据，失败时为 `null` |
| `request_id` | string | 请求追踪 ID，由中间件注入 |

### errors 字段（仅错误响应）

| 字段 | 类型 | 说明 |
|------|------|------|
| `errors` | list \| null | 详细错误列表，仅在验证失败等场景返回 |
| `errors[].field` | string | 出错字段名 |
| `errors[].detail` | string | 具体错误描述 |

## 业务状态码规范

| 范围 | 含义 | 示例 |
|------|------|------|
| `0` | 成功 | - |
| `40001-40099` | 请求参数错误 | 40001 参数校验失败 |
| `40101-40199` | 认证/鉴权失败 | 40101 Token 过期 |
| `40301-40399` | 权限不足 | 40301 无操作权限 |
| `40401-40499` | 资源不存在 | 40401 主机不存在 |
| `40901-40999` | 资源冲突 | 40901 名称重复 |
| `50001-50099` | 服务端内部错误 | 50001 数据库异常 |
| `50201-50299` | 外部服务调用失败 | 50201 远程执行超时 |

## HTTP 状态码映射

| 业务场景 | HTTP Status |
|----------|-------------|
| 成功 | 200 |
| 资源创建成功 | 201 |
| 参数校验失败 | 422 |
| 未认证 | 401 |
| 无权限 | 403 |
| 资源不存在 | 404 |
| 服务端错误 | 500 |

## 分页响应

分页接口 `data` 字段必须包含以下结构：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "items": [],
    "pagination": {
      "page": 1,
      "page_size": 20,
      "total": 150,
      "total_pages": 8
    }
  },
  "request_id": "req-abc123"
}
```

### 分页字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `items` | list | 当前页数据列表 |
| `pagination.page` | int | 当前页码，从 1 开始 |
| `pagination.page_size` | int | 每页条数 |
| `pagination.total` | int | 总记录数 |
| `pagination.total_pages` | int | 总页数 |

## 列表查询参数标准

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `page` | int | 1 | 页码 |
| `page_size` | int | 20 | 每页条数，最大 100 |
| `sort_by` | string | `created_at` | 排序字段 |
| `sort_order` | string | `desc` | `asc` 或 `desc` |
| `keyword` | string | - | 关键字搜索 |

## Pydantic 响应模型

使用 Pydantic v2 定义响应模型：

```python
from pydantic import BaseModel, Field
from typing import Generic, TypeVar

T = TypeVar("T")

class PaginationInfo(BaseModel):
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)
    total: int = Field(ge=0)
    total_pages: int = Field(ge=0)

class PaginatedData(BaseModel, Generic[T]):
    items: list[T]
    pagination: PaginationInfo

class ApiResponse(BaseModel, Generic[T]):
    code: int = 0
    message: str = "success"
    data: T | None = None
    request_id: str = ""
    errors: list[dict] | None = None
```

## 时间格式

所有时间字段统一使用 **ISO 8601** 格式，UTC 时区：

```
2026-07-10T14:30:00Z
2026-07-10T14:30:00.123Z
```

Pydantic 模型中使用 `datetime` 类型，序列化时自动转为 ISO 格式。

## 命名规范

- JSON 字段使用 **snake_case**（如 `created_at`、`host_name`）
- URL 路径使用 **kebab-case**（如 `/api/v1/host-groups`）
- 资源名称使用复数（如 `hosts`、`tasks`、`deployments`）

## 版本控制

API 路径必须包含版本号：

```
/api/v1/hosts
/api/v1/deployments
```

## 禁止事项

1. 禁止返回无信封的裸数据（如直接返回 `{"id": 1}`）
2. 禁止在成功响应中使用 `errors` 字段
3. 禁止混用 `camelCase` 和 `snake_case`
4. 禁止在响应中返回密码、Token 等敏感字段
5. 禁止使用 HTTP 200 返回错误业务状态
