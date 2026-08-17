"""Status normalization to the shared vocabulary (design doc section 4)."""

from __future__ import annotations

STATUS_VOCABULARY = ("running", "stopped", "maintenance", "unknown")

# Common provider status values mapped to the vocabulary; adapters may extend
# per provider but must always emit a vocabulary member.
_DEFAULT_MAP: dict[str, str] = {
    # aliyun ECS / RDS etc.
    "running": "running",
    "available": "running",
    "active": "running",
    "inuse": "running",
    "in-use": "running",
    "stopped": "stopped",
    "inactive": "stopped",
    "locked": "stopped",
    "deactivated": "stopped",
    "released": "stopped",
    # maintenance-ish states
    "migrating": "maintenance",
    "maintenance": "maintenance",
    "rebooting": "maintenance",
    "starting": "maintenance",
    "stopping": "maintenance",
    "creating": "maintenance",
    "pending": "maintenance",
    "provisioning": "maintenance",  # aliyun NLB
    "configuring": "maintenance",  # aliyun NLB
    "deleting": "maintenance",  # aliyun NLB
    "converting": "maintenance",  # aliyun NAT gateway
    "attaching": "maintenance",  # aliyun disk
    "detaching": "maintenance",  # aliyun disk
    "reiniting": "maintenance",  # aliyun disk
    "extending": "maintenance",  # aliyun NAS
    "shrinking": "maintenance",  # aliyun NAS
    # aliyun RDS operational states (all lowercased by normalize_status)
    "transing": "maintenance",
    "importing": "maintenance",
    "restoring": "maintenance",
    "dbinstance_class_changing": "maintenance",
    "engine_version_upgrading": "maintenance",
    "net_type_changing": "maintenance",
    "guard_db_creating": "maintenance",
    "temp_db_creating": "maintenance",
}


def normalize_status(raw_status: str | None) -> str:
    """Map a provider status string into the shared vocabulary.

    Args:
        raw_status: Raw status value from the provider API.

    Returns:
        One of running / stopped / maintenance / unknown.
    """
    if not raw_status:
        return "unknown"
    key = raw_status.strip().lower()
    return _DEFAULT_MAP.get(key, "unknown")
