"""Central configuration. Everything environment-specific lives here."""
from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: str = "development"

    # One SQLAlchemy URL per shard, comma separated. Defaults are file-backed
    # SQLite so the project runs with zero external services. In deployment
    # each URL points at a separate Postgres database/instance.
    shard_urls: str = (
        "sqlite:///./data/shard_0.db,sqlite:///./data/shard_1.db,"
        "sqlite:///./data/shard_2.db,sqlite:///./data/shard_3.db"
    )

    # "memory" = in-process bus (dev/tests), "kafka" = Kafka / Redpanda.
    event_bus_backend: str = "memory"
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic_transactions: str = "payments.transactions"
    kafka_consumer_group: str = "settlement-workers"

    # When true the API process also runs the outbox relay + settlement
    # consumer in-process. Only valid with the memory bus (a separate worker
    # process cannot share an in-memory queue). docker-compose sets this false.
    embed_worker: bool = True
    outbox_relay_interval_seconds: float = 0.2

    breaker_failure_threshold: int = 5
    breaker_reset_timeout_seconds: float = 30.0

    @property
    def shard_url_list(self) -> List[str]:
        return [u.strip() for u in self.shard_urls.split(",") if u.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
