"""Long-running loops shared by the API (embedded mode) and the standalone worker."""
from __future__ import annotations

import asyncio
import logging

from app.config import Settings
from app.core.circuit_breaker import CircuitBreaker
from app.db.shard_router import ShardRegistry
from app.events.bus import EventBus
from app.events.schemas import TXN_DEBITED
from app.services.outbox_relay import relay_once
from app.services.settlement import handle_transaction_debited

logger = logging.getLogger(__name__)


async def relay_loop(registry: ShardRegistry, bus: EventBus, settings: Settings) -> None:
    breaker = CircuitBreaker(settings.breaker_failure_threshold, settings.breaker_reset_timeout_seconds)
    while True:
        try:
            await relay_once(registry, bus, breaker, settings.kafka_topic_transactions)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("outbox relay pass failed")
        await asyncio.sleep(settings.outbox_relay_interval_seconds)


async def settlement_consumer(registry: ShardRegistry, bus: EventBus, settings: Settings) -> None:
    async def handler(payload: dict) -> None:
        if payload.get("event_type") == TXN_DEBITED:
            await asyncio.to_thread(handle_transaction_debited, registry, payload)
        # transaction.completed / transaction.failed are for external consumers.

    await bus.subscribe(settings.kafka_topic_transactions, settings.kafka_consumer_group, handler)


def start_background_tasks(registry: ShardRegistry, bus: EventBus, settings: Settings) -> list[asyncio.Task]:
    return [
        asyncio.create_task(relay_loop(registry, bus, settings), name="outbox-relay"),
        asyncio.create_task(settlement_consumer(registry, bus, settings), name="settlement-consumer"),
    ]


async def stop_background_tasks(tasks: list[asyncio.Task]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
