"""Cloud account credentials loading (accounts.yaml, K8s Secret mount).

Design doc section 3: credentials never enter git/db/image and never appear in logs.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

from cloudsync.core.exceptions import ConfigError

logger = logging.getLogger("cloudsync.core.accounts")

SUPPORTED_PROVIDERS = ("aliyun", "gcp")

type AccountKey = tuple[str, str]  # (provider, account_id)


class AccountConfig(BaseModel):
    """One cloud account entry from accounts.yaml.

    Credential fields are excluded from any string/dict representation
    so they can never leak into logs.
    """

    model_config = ConfigDict(repr=False)

    provider: str
    account_id: str
    display_name: str = ""
    regions: list[str] = Field(default_factory=list)  # empty = SDK default / all
    # aliyun
    access_key_id: str = ""
    access_key_secret: str = ""
    # gcp
    service_account_json: str = ""

    def __repr__(self) -> str:
        return (
            f"AccountConfig(provider={self.provider!r}, account_id={self.account_id!r}, "
            f"display_name={self.display_name!r}, regions={self.regions!r})"
        )


class AccountRegistry:
    """Lookup accounts by (provider, account_id); default-deny semantics."""

    def __init__(self, accounts: list[AccountConfig]) -> None:
        self._by_key: dict[AccountKey, AccountConfig] = {
            (a.provider, a.account_id): a for a in accounts
        }

    def get(self, provider: str, account_id: str) -> AccountConfig | None:
        """Return the account or None when no credential is configured."""
        return self._by_key.get((provider, account_id))

    def all(self) -> list[AccountConfig]:
        """Return all loaded accounts."""
        return list(self._by_key.values())

    def __len__(self) -> int:
        return len(self._by_key)


def load_accounts(path: str | Path) -> AccountRegistry:
    """Parse accounts.yaml into an AccountRegistry.

    Args:
        path: Path to the credentials file (K8s Secret mount).

    Returns:
        Registry indexed by (provider, account_id).

    Raises:
        ConfigError: File missing, unparsable, or structurally invalid.
    """
    file = Path(path)
    if not file.is_file():
        raise ConfigError(f"Accounts file not found: {path}")

    try:
        raw: Any = yaml.safe_load(file.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ConfigError(f"Accounts file is not valid YAML: {e}") from e

    if not isinstance(raw, dict) or not isinstance(raw.get("providers"), dict):
        raise ConfigError("Accounts file must contain a top-level 'providers' mapping")

    accounts: list[AccountConfig] = []
    for provider, entries in raw["providers"].items():
        if provider not in SUPPORTED_PROVIDERS:
            logger.warning("Unsupported provider in accounts file skipped",
                           extra={"provider": provider})
            continue
        if not isinstance(entries, list):
            raise ConfigError(f"providers.{provider} must be a list")
        for entry in entries:
            if not isinstance(entry, dict) or not entry.get("account_id"):
                raise ConfigError(f"providers.{provider} entry missing account_id")
            accounts.append(AccountConfig(provider=provider, **entry))

    logger.info("Cloud accounts loaded", extra={"count": len(accounts)})
    return AccountRegistry(accounts)
