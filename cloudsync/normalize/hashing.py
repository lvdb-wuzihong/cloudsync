"""Content hash for resource_version (decision D3).

Cloud APIs have no ordered resourceVersion semantics; a canonical content hash
lets the consumer skip unchanged resources (hash equal -> no change).
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cloudsync.schemas.normalized import NormalizedResource


def compute_resource_version(resource: NormalizedResource) -> str:
    """Compute the content hash used as resource_version.

    Formula per design doc section 4:
    sha256(json.dumps({name, region, zone, status, attributes, cloud_tags},
    sort_keys=True))[:16]

    Args:
        resource: Normalized resource produced by an adapter.

    Returns:
        First 16 hex chars of the sha256 digest.
    """
    payload = {
        "name": resource.name,
        "region": resource.region,
        "zone": resource.zone,
        "status": resource.status,
        "attributes": resource.attributes,
        "cloud_tags": resource.cloud_tags,
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
