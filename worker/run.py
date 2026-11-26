"""Standalone worker: outbox relay + settlement consumer.

    python -m worker.run

Use this in deployment (docker-compose `worker` service) with the Kafka bus; the
API then runs with EMBED_WORKER=false. You can run several worker replicas: relay
duplicates are harmless (consumers dedupe) and Kafka consumer groups split the
partitions between settlement consumers.
"""
from __future__ import annotations

import asyncio
import logging

from app.config import get_settings
from app.db.shard_router import get_shard_registry
from app.events.bus import build_event_bus
from app.services.runtime import start_background_tasks, stop_background_tasks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")


async def main() -> None:
    settings = get_settings()
    registry = get_shard_registry()
    registry.create_all()
    bus = build_event_bus(settings.event_bus_backend, settings.kafka_bootstrap_servers)
    await bus.start()
    tasks = start_background_tasks(registry, bus, settings)
    logger.info("worker started (bus=%s)", settings.event_bus_backend)
    try:
        await asyncio.gather(*tasks)
    finally:
        await stop_background_tasks(tasks)
        await bus.stop()


if __name__ == "__main__":
    asyncio.run(main())
