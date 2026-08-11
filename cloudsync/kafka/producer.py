"""Kafka producer wrapper for cloud-sync-{provider} topics.

Message contract is CloudResourceMessage (design doc section 4); the topic
naming matches the bingops consumer subscription regex `cloud-sync-.*`.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError

from cloudsync.core.config import settings
from cloudsync.core.exceptions import KafkaPublishError
from cloudsync.core.retry import kafka_retry

if TYPE_CHECKING:
    from cloudsync.schemas.messages import CloudResourceMessage

logger = logging.getLogger("cloudsync.kafka.producer")


def resolve_topics(override: str, providers: list[str]) -> list[str]:
    """Resolve the topic set: env override wins, else one per registered provider.

    Args:
        override: Comma-separated topic list from CLOUDSYNC_KAFKA_TOPICS.
        providers: Registered provider names for default derivation.

    Returns:
        Topic names, e.g. ["cloud-sync-aliyun", "cloud-sync-gcp"].
    """
    if override.strip():
        return [t.strip() for t in override.split(",") if t.strip()]
    return [CloudSyncProducer.topic_for(p) for p in providers]


class CloudSyncProducer:
    """Thin wrapper around AIOKafkaProducer with retries and structured logging."""

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self._bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            enable_idempotence=True,
        )
        self._started = False

    @staticmethod
    def topic_for(provider: str) -> str:
        """Topic name for a provider, e.g. cloud-sync-aliyun."""
        return f"{settings.kafka_topic_prefix}-{provider}"

    async def start(self, topics: list[str] | None = None) -> None:
        """Start the underlying producer; idempotently ensure topics exist.

        Args:
            topics: Topics to check/create before producing; skipped when None
                (e.g. when topics are managed externally by ops).
        """
        if topics is not None:
            await self._ensure_topics(topics)
        if not self._started:
            await self._producer.start()
            self._started = True

    async def _ensure_topics(self, topics: list[str]) -> None:
        """Create missing topics (idempotent); partitions/replication from config.

        Args:
            topics: Desired topic names.

        Raises:
            KafkaPublishError: Broker rejected topic creation.
        """
        admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
        await admin.start()
        try:
            existing = set(await admin.list_topics())
            to_create = [t for t in topics if t not in existing]
            if not to_create:
                logger.info("Kafka topics already exist", extra={"topics": topics})
                return
            new_topics = [
                NewTopic(
                    name=t,
                    num_partitions=settings.kafka_topic_partitions,
                    replication_factor=settings.kafka_topic_replication_factor,
                )
                for t in to_create
            ]
            await admin.create_topics(new_topics)
            logger.info("Kafka topics created",
                        extra={"topics": to_create,
                               "partitions": settings.kafka_topic_partitions,
                               "replication_factor": settings.kafka_topic_replication_factor})
        except KafkaError as e:
            logger.error("Kafka topic creation failed", extra={"error_code": "KAFKA_ERROR"})
            raise KafkaPublishError(",".join(topics), str(e)) from e
        finally:
            await admin.close()

    async def close(self) -> None:
        """Flush and stop the underlying producer (idempotent)."""
        if self._started:
            await self._producer.stop()
            self._started = False

    @kafka_retry
    async def send(self, provider: str, message: CloudResourceMessage) -> None:
        """Send one message to cloud-sync-{provider}.

        Args:
            provider: Cloud vendor selecting the topic.
            message: Contract message to publish.

        Raises:
            KafkaPublishError: Broker rejected or failed the send after retries.
        """
        topic = self.topic_for(provider)
        try:
            await self._producer.send_and_wait(
                topic,
                value=message.to_json_dict(),
                key=message.provider_id.encode("utf-8"),
            )
        except KafkaError as e:
            logger.error("Kafka send failed",
                         extra={"topic": topic, "provider_id": message.provider_id,
                                "error_code": "KAFKA_ERROR"})
            raise KafkaPublishError(topic, str(e)) from e

    async def send_batch(self, provider: str, messages: list[CloudResourceMessage]) -> None:
        """Send a batch of messages; empty batches are no-ops.

        Args:
            provider: Cloud vendor selecting the topic.
            messages: Messages to publish in order.
        """
        if not messages:
            return
        for message in messages:
            await self.send(provider, message)
        logger.info("Kafka batch published",
                    extra={"topic": self.topic_for(provider), "count": len(messages)})
