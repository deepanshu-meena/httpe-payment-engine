"""The same core guarantees, proven against REAL Postgres shards.

Skipped unless TEST_POSTGRES_URLS is set to 4 comma-separated SQLAlchemy URLs
(CI does this with a Postgres service; locally: `docker compose up -d postgres`).
Postgres has true multi-version row locking, so this exercises genuine concurrent
writers, unlike SQLite's single-writer model.
"""
import os
import threading

import pytest

from app.core.concurrency import InsufficientFundsError, debit
from app.db.models import Base
from app.db.shard_router import ShardRegistry
from app.services.payments import create_payment
from tests.conftest import balance_minor, make_account_on_shard, same_shard_pair

URLS = os.getenv("TEST_POSTGRES_URLS")
pytestmark = pytest.mark.postgres


@pytest.fixture()
def pg_registry():
    if not URLS:
        pytest.skip("TEST_POSTGRES_URLS not set")
    reg = ShardRegistry([u.strip() for u in URLS.split(",")])
    for engine in reg._engines:
        Base.metadata.drop_all(engine)
    reg.create_all()
    yield reg
    reg.dispose()


def test_no_lost_updates_on_postgres(pg_registry):
    acct = make_account_on_shard(pg_registry, 0, "100.00")
    ok, rej, other, lock = [], [], [], threading.Lock()

    def worker():
        try:
            with pg_registry.session_for_account(acct) as s:
                debit(s, acct, 1000, "INR")
            with lock: ok.append(1)
        except InsufficientFundsError:
            with lock: rej.append(1)
        except Exception as e:
            with lock: other.append(e)

    ts = [threading.Thread(target=worker) for _ in range(40)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert not other, other
    assert (len(ok), len(rej)) == (10, 30)
    assert balance_minor(pg_registry, acct) == 0


def test_same_key_race_charges_once_on_postgres(pg_registry):
    a, b = same_shard_pair(pg_registry)
    out = []
    ts = [threading.Thread(target=lambda: out.append(create_payment(
        pg_registry, idempotency_key="race", debit_account_id=a, credit_account_id=b,
        amount_minor=1000, currency="INR"))) for _ in range(20)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert sum(1 for _, replayed in out if not replayed) == 1
    assert balance_minor(pg_registry, a) == 9000


def test_opposing_transfers_do_not_deadlock_on_postgres(pg_registry):
    """A->B and B->A at the same time would deadlock without a fixed lock order."""
    a, b = same_shard_pair(pg_registry, "1000.00", "1000.00")
    errors, lock = [], threading.Lock()

    def run(src, dst, n):
        for i in range(n):
            try:
                create_payment(pg_registry, idempotency_key=f"{src[:6]}-{i}-{threading.get_ident()}",
                               debit_account_id=src, credit_account_id=dst, amount_minor=100, currency="INR")
            except Exception as e:
                with lock: errors.append(e)

    ts = [threading.Thread(target=run, args=(a, b, 25)) for _ in range(8)] + \
         [threading.Thread(target=run, args=(b, a, 25)) for _ in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert not errors, errors[:3]
    assert balance_minor(pg_registry, a) + balance_minor(pg_registry, b) == 200_000
