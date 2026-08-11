"""Predefined retry strategies (see bingops-error-handling skill)."""

from __future__ import annotations

import logging

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)

from cloudsync.core.exceptions import RateLimitError

logger = logging.getLogger("cloudsync.core.retry")

# Cloud SDK throttling: 3 attempts, exponential backoff 1s-10s, retries logged WARNING
cloud_api_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(RateLimitError),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)

# Shared bingops PostgreSQL connectivity
db_retry = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)

# Kafka publish: fixed 1s interval
kafka_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
