# API 响应工具参考实现

## 响应构建器（bingops/core/response.py）

```python
from typing import Any
from fastapi.responses import JSONResponse
from bingops.api.middleware.logging import request_id_var


def success_response(
    data: Any = None,
    message: str = "success",
    http_status: int = 200,
) -> JSONResponse:
    """构建成功响应。"""
    return JSONResponse(
        status_code=http_status,
        content={
            "code": 0,
            "message": message,
            "data": data,
            "request_id": request_id_var.get(),
        },
    )


def error_response(
    code: int,
    message: str,
    http_status: int = 400,
    errors: list[dict] | None = None,
) -> JSONResponse:
    """构建错误响应。"""
    content = {
        "code": code,
        "message": message,
        "data": None,
        "request_id": request_id_var.get(),
    }
    if errors is not None:
        content["errors"] = errors
    return JSONResponse(status_code=http_status, content=content)


def paginated_response(
    items: list[Any],
    total: int,
    page: int,
    page_size: int,
) -> JSONResponse:
    """构建分页响应。"""
    total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0
    return success_response(
        data={
            "items": items,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages,
            },
        }
    )
```

## FastAPI 异常处理器（bingops/api/exception_handlers.py）

```python
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from bingops.core.response import error_response


def register_exception_handlers(app: FastAPI) -> None:
    """注册全局异常处理器。"""

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request: Request, exc: RequestValidationError):
        errors = [
            {"field": ".".join(str(loc) for loc in err["loc"]), "detail": err["msg"]}
            for err in exc.errors()
        ]
        return error_response(
            code=40001,
            message="Validation failed",
            http_status=422,
            errors=errors,
        )

    @app.exception_handler(Exception)
    async def generic_handler(request: Request, exc: Exception):
        # 记录日志（需导入 logger）
        import logging
        logger = logging.getLogger("bingops.api.exceptions")
        logger.exception("Unhandled exception occurred")
        return error_response(
            code=50001,
            message="Internal server error",
            http_status=500,
        )
```

## API 路由示例（bingops/api/v1/hosts.py）

```python
from fastapi import APIRouter, Depends
from bingops.core.response import success_response, error_response, paginated_response

router = APIRouter(prefix="/api/v1/hosts", tags=["hosts"])


@router.get("")
async def list_hosts(page: int = 1, page_size: int = 20):
    # 从数据库获取数据...
    items = []
    total = 0
    return paginated_response(items, total, page, page_size)


@router.get("/{host_id}")
async def get_host(host_id: str):
    host = await fetch_host(host_id)
    if not host:
        return error_response(code=40401, message="Host not found", http_status=404)
    return success_response(data=host)


@router.post("", status_code=201)
async def create_host(payload: CreateHostRequest):
    host = await create_host_in_db(payload)
    return success_response(data=host, message="Host created", http_status=201)
```
