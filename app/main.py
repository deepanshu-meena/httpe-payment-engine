"""FastAPI application factory."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.config import get_settings
from app.db.shard_router import ShardRegistry, get_shard_registry
from app.events.bus import EventBus, build_event_bus
from app.services.runtime import start_background_tasks, stop_background_tasks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("httpe")


def create_app(
    registry: ShardRegistry | None = None,
    bus: EventBus | None = None,
    embed_worker: bool | None = None,
) -> FastAPI:
    """Arguments exist so tests can inject throwaway shards / a bus without
    touching global state. In production all three come from settings."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = get_settings()
        reg = registry or get_shard_registry()
        reg.create_all()
        event_bus = bus or build_event_bus(settings.event_bus_backend, settings.kafka_bootstrap_servers)
        await event_bus.start()
        app.state.registry = reg
        app.state.bus = event_bus

        run_embedded = settings.embed_worker if embed_worker is None else embed_worker
        tasks = start_background_tasks(reg, event_bus, settings) if run_embedded else []
        logger.info("HTTPE up: %s shard(s), bus=%s, embedded_worker=%s",
                    reg.shard_count, type(event_bus).__name__, run_embedded)
        try:
            yield
        finally:
            await stop_background_tasks(tasks)
            await event_bus.stop()

    app = FastAPI(
        title="HTTPE - High-Throughput Transaction Processing Engine",
        description="Sharded, idempotent payment engine with saga-based cross-shard settlement. "
        "See README.md and DECISIONS.md.",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


app = create_app()
