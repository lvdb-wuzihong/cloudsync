"""NormalizedResource: structured intermediate state produced by adapters.

Mirrors the CloudResourceMessage contract (design doc section 4) without
event_type, which the engine assigns when emitting messages.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class NormalizedResource(BaseModel):
    """Provider-agnostic resource produced by adapters before publishing.

    Field semantics follow design doc section 4:
    - resource_type equals the CMDB model code (e.g. "aliyun_ecs");
    - attribute keys must equal model field codes; common-layer fields
      (name/provider/region/...) must NOT be duplicated into attributes;
    - status is normalized to the running/stopped/maintenance/unknown vocabulary;
    - cloud_tags keys are lowercased with hyphens (see normalize.tags).
    """

    provider: str
    resource_type: str
    provider_id: str
    cloud_account: str
    name: str = ""
    region: str = ""
    zone: str = ""
    status: str = "unknown"
    attributes: dict = Field(default_factory=dict)
    cloud_tags: dict[str, str] = Field(default_factory=dict)
    # Dependency hints (e.g. ECS -> vswitch); consumer builds edges in v2,
    # but producers MUST fill them to avoid rework.
    parent_provider_id: str | None = None
    parent_resource_type: str | None = None
