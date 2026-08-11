"""Engine exception hierarchy (see bingops-error-handling skill)."""

from __future__ import annotations


class CloudSyncError(Exception):
    """Engine base exception; all business errors derive from this."""

    def __init__(
        self,
        message: str = "Internal engine error",
        code: int = 50001,
        error_code: str = "INTERNAL_ERROR",
    ) -> None:
        self.message = message
        self.code = code
        self.error_code = error_code
        super().__init__(message)

    def to_dict(self) -> dict:
        """Serialize to a plain dict for logging/reporting."""
        return {"code": self.code, "message": self.message, "error_code": self.error_code}


# ── Config / credential ─────────────────────────────────────────────────────


class ConfigError(CloudSyncError):
    """Configuration file invalid or missing."""

    def __init__(self, message: str = "Configuration error"):
        super().__init__(message, code=50002, error_code="CONFIG_ERROR")


class CredentialError(CloudSyncError):
    """Cloud account credential missing or invalid."""

    def __init__(self, message: str = "Credential missing or invalid"):
        super().__init__(message, code=50003, error_code="AUTH_FAILED")


# ── Cloud API (error_code normalization: RATE_LIMITED / AUTH_FAILED / API_ERROR)


class AuthFailedError(CloudSyncError):
    """Provider rejected the credential; not retryable."""

    def __init__(self, provider: str, detail: str):
        super().__init__(
            f"Cloud auth failed for '{provider}': {detail}",
            code=50202,
            error_code="AUTH_FAILED",
        )
        self.provider = provider


class RateLimitError(CloudSyncError):
    """Provider API throttled; retry with exponential backoff."""

    def __init__(self, provider: str, detail: str):
        super().__init__(
            f"Cloud API rate limited for '{provider}': {detail}",
            code=50203,
            error_code="RATE_LIMITED",
        )
        self.provider = provider


class AdapterError(CloudSyncError):
    """Generic cloud API failure (pagination/server errors etc.)."""

    def __init__(self, provider: str, detail: str):
        super().__init__(
            f"Cloud API error for '{provider}': {detail}",
            code=50201,
            error_code="API_ERROR",
        )
        self.provider = provider


# ── Data / reconcile / messaging ────────────────────────────────────────────


class ValidationError(CloudSyncError):
    """Normalized data failed validation."""

    def __init__(self, message: str = "Validation failed", errors: list[dict] | None = None):
        super().__init__(message, code=40001, error_code="VALIDATION_ERROR")
        self.errors = errors or []


class ReconcileError(CloudSyncError):
    """Reconciliation failed; the round must abort and emit no deletes."""

    def __init__(self, message: str = "Reconcile failed"):
        super().__init__(message, code=50004, error_code="RECONCILE_ERROR")


class KafkaPublishError(CloudSyncError):
    """Kafka publish failed after retries."""

    def __init__(self, topic: str, detail: str):
        super().__init__(
            f"Kafka publish to '{topic}' failed: {detail}",
            code=50204,
            error_code="KAFKA_ERROR",
        )
        self.topic = topic
