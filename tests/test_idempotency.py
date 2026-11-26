import threading

import pytest

from app.core.idempotency import IdempotencyKeyReuseError
from app.services.payments import PaymentRejectedError, create_payment
from tests.conftest import balance_minor, same_shard_pair, cross_shard_pair


def pay(reg, key, a, b, amount=4000):
    return create_payment(reg, idempotency_key=key, debit_account_id=a, credit_account_id=b,
                          amount_minor=amount, currency="INR")


def test_retry_with_same_key_moves_money_once(registry):
    a, b = same_shard_pair(registry)
    t1, replay1 = pay(registry, "k1", a, b)
    t2, replay2 = pay(registry, "k1", a, b)
    assert (replay1, replay2) == (False, True)
    assert t1.id == t2.id
    assert balance_minor(registry, a) == 6000
    assert balance_minor(registry, b) == 4000


def test_retry_cross_shard_also_idempotent(registry):
    a, b = cross_shard_pair(registry)
    t1, _ = pay(registry, "k1", a, b)
    t2, replay = pay(registry, "k1", a, b)
    assert replay and t1.id == t2.id
    assert balance_minor(registry, a) == 6000  # debited exactly once


def test_different_keys_are_independent_payments(registry):
    a, b = same_shard_pair(registry)
    pay(registry, "a", a, b, 1000)
    pay(registry, "b", a, b, 1000)
    assert balance_minor(registry, a) == 8000


def test_same_key_different_body_is_rejected(registry):
    a, b = same_shard_pair(registry)
    pay(registry, "k", a, b, 1000)
    with pytest.raises(IdempotencyKeyReuseError):
        pay(registry, "k", a, b, 2000)
    assert balance_minor(registry, a) == 9000


def test_rejected_payment_leaves_nothing_behind_and_key_stays_reusable(registry):
    a, b = same_shard_pair(registry, balance_a="5.00")
    with pytest.raises(PaymentRejectedError):
        pay(registry, "k", a, b, 10_000)
    assert balance_minor(registry, a) == 500
    # key was not burned by the failed attempt: after a top-up the same key works
    a2, b2 = a, b
    t, replay = pay(registry, "k", a2, b2, 500)
    assert not replay and balance_minor(registry, b) == 500


def test_concurrent_requests_with_same_key_charge_once(registry):
    a, b = same_shard_pair(registry)
    results, errors = [], []

    def worker():
        try:
            results.append(pay(registry, "race-key", a, b, 1000))
        except Exception as exc:  # pragma: no cover - would fail the assert below
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(12)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    assert not errors, errors
    assert len({r[0].id for r in results}) == 1            # everyone got the SAME transaction
    assert sum(1 for _, replayed in results if not replayed) == 1  # exactly one real execution
    assert balance_minor(registry, a) == 9000              # charged once


def test_opposing_same_shard_transfers_conserve_money(registry):
    """Opposing transfers exercise the fixed lock-ordering path (see services/payments.py).
    SQLite cannot deadlock the way Postgres can, so this guards correctness; the
    deadlock itself is covered by the postgres-marked test."""
    a, b = same_shard_pair(registry, "1000.00", "1000.00")
    errs = []

    def run(src, dst):
        for i in range(25):
            try:
                pay(registry, f"{src[:6]}-{i}-{threading.get_ident()}", src, dst, 100)
            except Exception as e:
                errs.append(e)

    ts = [threading.Thread(target=run, args=(a, b)) for _ in range(4)] + \
         [threading.Thread(target=run, args=(b, a)) for _ in range(4)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert not errs, errs[:2]
    assert balance_minor(registry, a) + balance_minor(registry, b) == 200_000
