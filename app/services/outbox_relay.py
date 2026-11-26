"""Outbox relay: moves committed-but-unpublished outbox rows to the event bus.

Design points
- Short DB sessions: fetch a batch, close the session, publish over the network,
  then open a new session to mark rows published. No DB transaction is ever held
  open across network I/O.
- Publish-then-mark means a crash in between re-publishes the rows later:
  at-least-once delivery. Consumers dedupe (see services/settlement.py).
- Publishing is guarded by a circuit breaker; while it is open, rows just wait in
  the table - nothing is lost.
- Blocking DB calls run in a worker thread so the event loop stays responsive.
"""
from __future__ import annotations

import asyncio
import json
import logging

from app.core import outbox
from app.core.circuit_breaker import CircuitBreaker, CircuitOpenError
from app.db.shard_router import ShardRegistry
from app.events.bus import EventBus

logger = logging.getLogger(__name__)


def _fetch(registry: ShardRegistry, shard: int, limit: int):
    with registry.session_for_shard(shard) as session:
        return outbox.fetch_unpublished(session, limit)


def _mark(registry: ShardRegistry, shard: int, ids: list[str]) -> None:
    with registry.session_for_shard(shard) as session:
        outbox.mark_published(session, ids)


async def relay_once(
    registry: ShardRegistry, bus: EventBus, breaker: CircuitBreaker, topic: str, batch_size: int = 100
) -> int:
    """One pass over every shard's outbox. Returns the number of events published."""
    relayed = 0
    for shard in range(registry.shard_count):
        batch = await asyncio.to_thread(_fetch, registry, shard, batch_size)
        sent: list[str] = []
        for event_id, aggregate_id, payload_json in batch:
            try:
                await breaker.call_async(bus.publish, topic, aggregate_id, json.loads(payload_json))
            except CircuitOpenError:
                logger.warning("circuit open; deferring remaining outbox rows on shard %s", shard)
                break
            except Exception:
                logger.exception("publish failed for outbox event %s", event_id)
                break
            sent.append(event_id)
        if sent:
            await asyncio.to_thread(_mark, registry, shard, sent)
            relayed += len(sent)
    return relayed
