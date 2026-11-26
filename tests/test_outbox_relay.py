import json

from sqlalchemy import select

from app.core.circuit_breaker import CircuitBreaker
from app.db.models import OutboxEvent
from app.events.bus import InMemoryEventBus
from app.services.outbox_relay import relay_once
from app.services.payments import create_payment
from tests.conftest import cross_shard_pair


def count_unpublished(registry):
    n = 0
    for shard in range(registry.shard_count):
        with registry.session_for_shard(shard) as s:
            n += len(s.execute(select(OutboxEvent).where(OutboxEvent.published == 0)).scalars().all())
    return n


async def test_relay_publishes_then_marks_published(registry):
    a, b = cross_shard_pair(registry)
    create_payment(registry, idempotency_key="r", debit_account_id=a, credit_account_id=b,
                   amount_minor=100, currency="INR")
    bus = InMemoryEventBus()
    breaker = CircuitBreaker(5, 30)
    assert count_unpublished(registry) == 1
    assert await relay_once(registry, bus, breaker, "t") == 1
    assert bus.published[0]["event_type"] == "transaction.debited"
    assert count_unpublished(registry) == 0
    assert await relay_once(registry, bus, breaker, "t") == 0   # nothing left


async def test_broker_outage_loses_nothing(registry):
    a, b = cross_shard_pair(registry)
    create_payment(registry, idempotency_key="r", debit_account_id=a, credit_account_id=b,
                   amount_minor=100, currency="INR")

    class DownBus(InMemoryEventBus):
        async def publish(self, *a, **k):
            raise ConnectionError("broker down")

    breaker = CircuitBreaker(2, 30)
    down = DownBus()
    for _ in range(4):
        await relay_once(registry, down, breaker, "t")
    assert count_unpublished(registry) == 1          # still safely in the outbox
    assert breaker.state.value == "OPEN"             # and we stopped hammering the broker

    healthy = InMemoryEventBus()
    assert await relay_once(registry, healthy, CircuitBreaker(2, 30), "t") == 1   # recovers
    assert count_unpublished(registry) == 0
