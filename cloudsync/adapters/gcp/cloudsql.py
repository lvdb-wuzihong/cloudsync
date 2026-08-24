"""GCP Cloud SQL adapter: SQL Admin REST list (no GAPIC package exists).

PyPI has no sqladmin GAPIC library (google-cloud-sqladmin-* absent; the
official route is the REST API), so this adapter calls
sqladmin.googleapis.com directly through an AuthorizedSession (requests-
based, HTTPS_PROXY honored like the compute REST transport). Fetching rules
identical to the other adapters: raise on any failure so the engine aborts
the round without emitting deletes.

Field codes align with the CMDB model gcp_cloudsql (engine /
engine_version / tier / storage_gb / private_ip / public_ip / vpc_id).
databaseVersion "MYSQL_8_0" splits into engine=MYSQL / engine_version=8.0.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from cloudsync.adapters.gcp.client import (
    PROVIDER,
    build_sql_session,
    last_segment,
    project_of,
)
from cloudsync.core.exceptions import AdapterError, AuthFailedError, RateLimitError
from cloudsync.core.retry import cloud_api_retry
from cloudsync.normalize.status import normalize_status
from cloudsync.schemas.normalized import NormalizedResource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from cloudsync.core.accounts import AccountConfig

logger = logging.getLogger("cloudsync.adapters.gcp.cloudsql")

RESOURCE_TYPE = "gcp_cloudsql"
API_BASE = "https://sqladmin.googleapis.com/sql/v1beta4"


@cloud_api_retry
async def _fetch_page(
    session: Any, url: str, params: dict[str, str], account: AccountConfig,
) -> dict:
    """One REST list call with status-code normalization (throttle/auth/api)."""
    import asyncio

    resp = await asyncio.to_thread(session.get, url, params=params, timeout=60)
    if resp.status_code in (429, 503):
        raise RateLimitError(PROVIDER, f"status={resp.status_code} api=sqladmin.list")
    if resp.status_code in (401, 403):
        raise AuthFailedError(PROVIDER, f"status={resp.status_code} api=sqladmin.list")
    if resp.status_code != 200:
        raise AdapterError(PROVIDER, f"status={resp.status_code} api=sqladmin.list")
    return resp.json()


def _split_database_version(raw: str) -> tuple[str | None, str | None]:
    """'MYSQL_8_0' -> ('MYSQL', '8.0'); unknown shapes degrade to None."""
    if not raw:
        return None, None
    parts = raw.split("_", 1)
    engine = parts[0] or None
    version = parts[1].replace("_", ".") if len(parts) > 1 and parts[1] else None
    return engine, version


def map_cloudsql(item: dict[str, Any], account_id: str) -> NormalizedResource:
    """Map one SQL Admin instance dict to NormalizedResource."""
    settings = item.get("settings") or {}
    ip_config = settings.get("ipConfiguration") or {}
    engine, engine_version = _split_database_version(item.get("databaseVersion") or "")

    public_ip = None
    private_ip = None
    for addr in item.get("ipAddresses") or []:
        kind = addr.get("type")
        if kind == "PRIMARY" and not public_ip:
            public_ip = addr.get("ipAddress")
        elif kind == "PRIVATE" and not private_ip:
            private_ip = addr.get("ipAddress")

    vpc_name = last_segment(ip_config.get("privateNetwork") or "") or None

    attributes = {
        "engine": engine,
        "engine_version": engine_version,
        "tier": settings.get("tier"),
        "storage_gb": int(settings["dataDiskSizeGb"]) if settings.get("dataDiskSizeGb") else None,
        "private_ip": private_ip,
        "public_ip": public_ip,
        "vpc_id": vpc_name,
    }
    attributes = {k: v for k, v in attributes.items() if v is not None}

    return NormalizedResource(
        provider=PROVIDER,
        resource_type=RESOURCE_TYPE,
        provider_id=item.get("name") or "",  # instance names unique per project
        cloud_account=account_id,
        name=item.get("name") or "",
        region=item.get("region") or "",
        zone="",
        status=normalize_status(item.get("state")),
        attributes=attributes,
        cloud_tags=dict(settings.get("userLabels") or {}),
        parent_provider_id=vpc_name,
        parent_resource_type="gcp_vpc" if vpc_name else None,
    )


async def list_cloudsql(account: AccountConfig) -> AsyncIterator[NormalizedResource]:
    """Page through all Cloud SQL instances of the project."""
    started = time.perf_counter()
    session = build_sql_session(account)
    url = f"{API_BASE}/projects/{project_of(account)}/instances"
    page_token = ""
    count = 0
    while True:
        params = {"maxResults": "100"}
        if page_token:
            params["pageToken"] = page_token
        body = await _fetch_page(session, url, params, account)
        for item in body.get("items") or []:
            count += 1
            yield map_cloudsql(item, account.account_id)
        page_token = body.get("nextPageToken") or ""
        if not page_token:
            break
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info("CloudSQL fetch completed",
                extra={"provider": PROVIDER, "account": account.account_id,
                       "resource_type": RESOURCE_TYPE, "count": count,
                       "duration_ms": round(duration_ms, 2)})
