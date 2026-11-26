"""System-level property: money is conserved.

Hammer the system with many concurrent payments (a mix of same-shard and
cross-shard, with deliberate duplicate idempotency keys and overdraw attempts),
drain the outbox through settlement, and assert that the total money in the system
is exactly what we started with and that no account is negative."""
import json
import random
import threading

from sqlalchemy import func, select

from app.db.models import Account, OutboxEvent, Transaction, TransactionState
from app.services.payments import PaymentRejectedError, create_payment
from app.services.settlement import handle_transaction_debited
from tests.conftest import make_account_on_shard


def total_minor(reg):
    t = 0
    for shard in range(reg.shard_count):
        with reg.session_for_shard(shard) as s:
            t += s.execute(select(func.coalesce(func.sum(Account.balance_minor), 0))).scalar_one()
    return t


def drain_settlement(reg):
    for shard in range(reg.shard_count):
        with reg.session_for_shard(shard) as s:
            events = [json.loads(r.payload_json) for r in
                      s.execute(select(OutboxEvent).where(OutboxEvent.event_type == "transaction.debited")).scalars()]
        for e in events:
            handle_transaction_debited(reg, e)
            handle_transaction_debited(reg, e)  # duplicate delivery on purpose


def test_money_is_conserved_under_concurrent_mixed_load(registry):
    accounts = [make_account_on_shard(registry, i % 4, "100.00") for i in range(12)]
    start_total = total_minor(registry)
    assert start_total == 12 * 10_000
    rng = random.Random(7)
    plan = [(rng.choice(accounts), rng.choice(accounts), rng.randint(1, 15_000), f"key-{i % 60}")
            for i in range(200)]            # only 60 distinct keys -> plenty of duplicates
    plan = [p for p in plan if p[0] != p[1]]

    def worker(chunk):
        for a, b, amt, key in chunk:
            try:
                create_payment(registry, idempotency_key=key, debit_account_id=a, credit_account_id=b,
                               amount_minor=amt, currency="INR")
            except PaymentRejectedError:
                pass
            except Exception as exc:
                # same key reused with a different random body -> expected 409-type error
                assert exc.__class__.__name__ == "IdempotencyKeyReuseError", exc

    threads = [threading.Thread(target=worker, args=(plan[i::8],)) for i in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    drain_settlement(registry)

    assert total_minor(registry) == start_total
    for shard in range(registry.shard_count):
        with registry.session_for_shard(shard) as s:
            assert s.execute(select(func.min(Account.balance_minor))).scalar_one() >= 0
            stuck = s.execute(select(func.count()).select_from(Transaction)
                              .where(Transaction.state == TransactionState.PROCESSING)).scalar_one()
            assert stuck == 0
