import uuid
from decimal import Decimal

from app.db.models import Account, Transaction, TransactionState as S
from app.services.payments import create_payment
from app.services.settlement import handle_transaction_debited
from tests.conftest import balance_minor, cross_shard_pair, make_account_on_shard
from app.core import outbox
from app.db.models import OutboxEvent
import json
from sqlalchemy import select


def debited_event(registry, shard=0):
    """Pull the transaction.debited event the API would have relayed to the bus."""
    with registry.session_for_shard(shard) as s:
        rows = s.execute(select(OutboxEvent).where(OutboxEvent.event_type == "transaction.debited")).scalars().all()
        return [json.loads(r.payload_json) for r in rows][-1]


def state_of(registry, txn):
    with registry.session_for_shard(0) as s:
        return s.get(Transaction, txn.id).state


def test_same_shard_completes_immediately(registry):
    a = make_account_on_shard(registry, 0, "100.00")
    b = make_account_on_shard(registry, 0, "0.00")
    txn, _ = create_payment(registry, idempotency_key="s", debit_account_id=a, credit_account_id=b,
                            amount_minor=3000, currency="INR")
    assert txn.state == S.COMPLETED
    assert (balance_minor(registry, a), balance_minor(registry, b)) == (7000, 3000)


def test_cross_shard_has_a_processing_window_then_completes(registry):
    a, b = cross_shard_pair(registry)
    txn, _ = create_payment(registry, idempotency_key="x", debit_account_id=a, credit_account_id=b,
                            amount_minor=3000, currency="INR")
    # the documented saga window: debited, not yet credited
    assert txn.state == S.PROCESSING
    assert (balance_minor(registry, a), balance_minor(registry, b)) == (7000, 0)

    assert handle_transaction_debited(registry, debited_event(registry)) == "credited"

    assert (balance_minor(registry, a), balance_minor(registry, b)) == (7000, 3000)
    assert state_of(registry, txn) == S.COMPLETED


def test_redelivered_event_credits_exactly_once(registry):
    a, b = cross_shard_pair(registry)
    txn, _ = create_payment(registry, idempotency_key="x", debit_account_id=a, credit_account_id=b,
                            amount_minor=3000, currency="INR")
    event = debited_event(registry)
    outcomes = [handle_transaction_debited(registry, event) for _ in range(3)]
    assert outcomes == ["credited", "already", "already"]
    assert balance_minor(registry, b) == 3000
    assert state_of(registry, txn) == S.COMPLETED


def test_missing_destination_refunds_payer_and_fails(registry):
    a = make_account_on_shard(registry, 0, "100.00")
    ghost = None
    from tests.conftest import shard_index_for
    while ghost is None:                     # a well-formed id on shard 1 with no account behind it
        cand = str(uuid.uuid4())
        if shard_index_for(cand, 4) == 1:
            ghost = cand
    txn, _ = create_payment(registry, idempotency_key="g", debit_account_id=a, credit_account_id=ghost,
                            amount_minor=3000, currency="INR")
    assert balance_minor(registry, a) == 7000            # debited optimistically
    assert handle_transaction_debited(registry, debited_event(registry)) == "rejected"
    assert balance_minor(registry, a) == 10000           # refunded in full
    assert state_of(registry, txn) == S.FAILED
    # and a redelivery must not refund twice
    handle_transaction_debited(registry, debited_event(registry))
    assert balance_minor(registry, a) == 10000


def test_currency_mismatch_destination_fails_and_refunds(registry):
    a = make_account_on_shard(registry, 0, "100.00", "INR")
    b = make_account_on_shard(registry, 1, "0.00", "USD")
    txn, _ = create_payment(registry, idempotency_key="c", debit_account_id=a, credit_account_id=b,
                            amount_minor=1000, currency="INR")
    assert handle_transaction_debited(registry, debited_event(registry)) == "rejected"
    assert balance_minor(registry, a) == 10000 and balance_minor(registry, b) == 0
    assert state_of(registry, txn) == S.FAILED
