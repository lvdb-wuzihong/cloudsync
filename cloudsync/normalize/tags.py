"""Cloud tag normalization (design doc section 4, referencing cmdb-design section 6.4).

Normalized key: lowercase with underscores replaced by hyphens.
The raw original key is kept inside adapters (never emitted), so downstream
consumers only see the normalized form.
"""

from __future__ import annotations

import re


def normalize_tag_key(key: str) -> str:
    """Normalize a tag key: trim, lowercase, underscores/spaces to hyphens."""
    collapsed = re.sub(r"[\s_]+", "-", key.strip())
    return collapsed.lower()


def normalize_tags(raw_tags: dict[str, str] | None) -> dict[str, str]:
    """Normalize a raw tag mapping; later duplicate keys win deterministically.

    Args:
        raw_tags: Raw tags from the provider API (may be None).

    Returns:
        Mapping of normalized key -> raw value (value untouched).
    """
    if not raw_tags:
        return {}
    normalized: dict[str, str] = {}
    for key in sorted(raw_tags):  # deterministic order for stable hashes
        normalized[normalize_tag_key(key)] = str(raw_tags[key])
    return normalized
