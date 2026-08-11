"""CloudResourceMessage: Kafka message contract (design doc section 4).

NOTE: The authoritative contract is bingops `schemas/cmdb/kafka_messages.py`
CloudResourceMessage. This derivation follows design doc section 4 and MUST be
field-by-field reconciled with the bingops schema before go-live; do not add
or rename fields here without first changing the bingops schema and consumer.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from cloudsync.schemas.normalized import NormalizedResource

EventType = Literal["upsert", "delete"]


class CloudResourceMessage(BaseModel):
    """One cloud resource event published to cloud-sync-{provider}."""

    provider: str
    resource_type: str  # equals CMDB model code, e.g. "aliyun_ecs"
    provider_id: str  # raw cloud ID or synthetic ID (presets appendix B #19)
    cloud_account: str  # account ID, equals cmdb_sync_tasks.target_id
    event_type: EventType
    # Content hash (decision D3): sha256 over canonical fields, first 16 hex chars
    resource_version: str
    name: str = ""
    region: str = ""
    zone: str = ""
    status: str = "unknown"
    attributes: dict = Field(default_factory=dict)
    cloud_tags: dict[str, str] = Field(default_factory=dict)
    parent_provider_id: str | None = None
    parent_resource_type: str | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def to_json_dict(self) -> dict:
        """Serialize for Kafka with ISO 8601 UTC timestamp."""
        return self.model_dump(mode="json")


def build_upsert_message(
    resource: NormalizedResource, resource_version: str
) -> CloudResourceMessage:
    """Build an upsert message from a normalized resource."""
    return CloudResourceMessage(
        event_type="upsert",
        resource_version=resource_version,
        **resource.model_dump(),
    )


def build_delete_message(
    resource: NormalizedResource, resource_version: str
) -> CloudResourceMessage:
    """Build a delete message (diff reconciliation) from a normalized resource."""
    return CloudResourceMessage(
        event_type="delete",
        resource_version=resource_version,
        **resource.model_dump(),
    )
