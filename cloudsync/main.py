"""Engine entrypoint: asyncio process (no HTTP layer).

Startup order per bingops-logging skill: setup_logging before anything else.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

from cloudsync.core.accounts import load_accounts
from cloudsync.core.config import settings
from cloudsync.core.db import dispose_engine
from cloudsync.core.exceptions import CloudSyncError
from cloudsync.core.logging import setup_logging
from cloudsync.kafka.producer import CloudSyncProducer, resolve_topics
from cloudsync.scheduler.engine import SyncEngine

logger = logging.getLogger("cloudsync.main")


async def main() -> None:
    """Load config/credentials and run the sync engine until interrupted."""
    setup_logging(
        level=settings.log_level,
        debug=settings.debug,
        log_dir=settings.log_dir,
        retention_days=settings.log_retention_days,
    )

    # Import adapter packages so they self-register into the adapter registry
    import cloudsync.adapters.aliyun  # noqa: F401
    import cloudsync.adapters.gcp  # noqa: F401
    from cloudsync.adapters.base import registered_providers

    try:
        accounts = load_accounts(settings.accounts_file)
    except CloudSyncError as e:
        logger.error("Failed to load cloud accounts",
                     extra={"error_code": e.error_code, "path": settings.accounts_file})
        raise

    producer = CloudSyncProducer()
    # Topic set: env override (CLOUDSYNC_KAFKA_TOPICS) or one per registered adapter
    topics = resolve_topics(settings.kafka_topics, registered_providers())
    await producer.start(topics=topics)
    engine = SyncEngine(accounts=accounts, producer=producer)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Windows event loop has no add_signal_handler support
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(engine.shutdown()))

    logger.info("Cloud syncer started", extra={"accounts": len(accounts)})
    try:
        await engine.run()
    finally:
        await engine.shutdown()
        await dispose_engine()
        logger.info("Cloud syncer stopped")


if __name__ == "__main__":
    asyncio.run(main())
