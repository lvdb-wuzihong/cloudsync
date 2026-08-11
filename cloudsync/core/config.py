"""Engine configuration (pydantic-settings, env prefix CLOUDSYNC_)."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Cloud syncer runtime settings.

    All values overridable via env vars with CLOUDSYNC_ prefix,
    e.g. CLOUDSYNC_DATABASE_URL, CLOUDSYNC_KAFKA_BOOTSTRAP_SERVERS.
    A local .env file (working directory) is also read; real env vars win.
    Credentials never go here - accounts.yaml is mounted from a K8s Secret.
    """

    model_config = SettingsConfigDict(
        env_prefix="CLOUDSYNC_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # bingops shared PostgreSQL (read-mostly; only last_synced_at writes)
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/bingops"

    # Kafka producer
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_prefix: str = "cloud-sync"
    # Comma-separated topic override; empty = derive cloud-sync-{provider} from
    # registered adapters. Must match the bingops consumer subscription regex.
    kafka_topics: str = ""
    kafka_topic_partitions: int = 3
    kafka_topic_replication_factor: int = 1

    # Credentials file (K8s Secret mount, never in git/db/image)
    accounts_file: str = "/etc/cloudsync/accounts.yaml"

    # Scheduler
    task_reload_interval: int = 60  # seconds, hot reload of cmdb_sync_tasks
    slow_round_ratio: float = 0.8  # warn when a round exceeds 80% of schedule interval

    # Logging
    debug: bool = True
    log_level: str = "INFO"
    log_dir: str | None = None
    log_retention_days: int = 7


@lru_cache
def get_settings() -> Settings:
    """Return the cached settings singleton."""
    return Settings()


settings = get_settings()
