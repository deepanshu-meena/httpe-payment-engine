"""Event bus abstraction (ports & adapters).

The rest of the app depends on the `EventBus` protocol, never on aiokafka:

  InMemoryEventBus  asyncio.Queue-backed; default for local dev and the test suite
  KafkaEventBus     aiokafka against Kafka/Redpanda; used when EVENT_BUS_BACKEND=kafka

Delivery semantics are AT-LEAST-ONCE on both. Consumers must be idempotent (the
settlement handler is - see services/settlement.py). A handler that keeps failing
is retried with backoff and then parked on a dead-letter queue instead of
blocking the partition forever (poison-message protection).

Trade-off: the in-memory bus cannot reproduce real broker failures. Broker-outage
behaviour is covered separately by circuit-breaker tests that make `publish`
itself fail.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable, Protocol

logger = logging.getLogger(__name__)

Handler = Callable[[dict], Awaitable[None]]
MAX_HANDLER_ATTEMPTS = 3


class EventBus(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def publish(self, topic: str, key: str, payload: dict) -> None: ...
    async def subscribe(self, topic: str, group_id: str, handler: Handler) -> None: ...


async def _deliver(handler: Handler, payload: dict, dead_letter: Callable[[dict], Awaitable[None]]) -> None:
    for attempt in range(1, MAX_HANDLER_ATTEMPTS + 1):
        try:
            await handler(payload)
            return
        except Exception:
            logger.exception("handler failed (attempt %s/%s)", attempt, MAX_HANDLER_ATTEMPTS)
            if attempt < MAX_HANDLER_ATTEMPTS:
                await asyncio.sleep(0.05 * attempt)
    await dead_letter(payload)


class InMemoryEventBus:
    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue] = {}
        self.published: list[dict] = []      # inspection hook for tests
        self.dead_letters: list[dict] = []

    def _queue(self, topic: str) -> asyncio.Queue:
        return self._queues.setdefault(topic, asyncio.Queue())

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish(self, topic: str, key: str, payload: dict) -> None:
        self.published.append(payload)
        await self._queue(topic).put(payload)

    async def _dead_letter(self, payload: dict) -> None:
        self.dead_letters.append(payload)

    async def subscribe(self, topic: str, group_id: str, handler: Handler) -> None:
        queue = self._queue(topic)
        while True:
            payload = await queue.get()
            await _deliver(handler, payload, self._dead_letter)


class KafkaEventBus:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producer = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, acks="all")
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

    async def publish(self, topic: str, key: str, payload: dict) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaEventBus.start() was not called")
        # key = transaction id -> all events of one transaction land on one
        # partition, preserving their relative order.
        await self._producer.send_and_wait(topic, key=key.encode(), value=json.dumps(payload).encode())

    async def subscribe(self, topic: str, group_id: str, handler: Handler) -> None:
        from aiokafka import AIOKafkaConsumer

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()

        async def dead_letter(payload: dict) -> None:
            await self.publish(topic + ".dlq", key=payload.get("transaction_id", ""), payload=payload)

        try:
            async for msg in consumer:
                await _deliver(handler, json.loads(msg.value.decode()), dead_letter)
                # Commit only AFTER the handler finished: a crash mid-handler
                # means redelivery, never silent loss. Hence idempotent handlers.
                await consumer.commit()
        finally:
            await consumer.stop()


def build_event_bus(backend: str, bootstrap_servers: str) -> EventBus:
    if backend == "kafka":
        return KafkaEventBus(bootstrap_servers)
    return InMemoryEventBus()
