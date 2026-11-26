"""Settlement: the asynchronous consumer half of the cross-shard saga.

Handles `transaction.debited` events (at-least-once, so possibly duplicated):

  Step A  credit the destination on ITS shard, in one local transaction that also
          inserts a ProcessedEvent marker. The marker's primary key makes a
          redelivered event a no-op: the second insert fails, so the credit is not
          applied twice. (At-least-once delivery + idempotent consumer =
          effectively-once processing.)

  Step B  on the DEBIT shard, finalise the transaction with a conditional UPDATE
          (state machine). Only the call that actually flips PROCESSING -> X
          performs X's side effects, so steps are safe to repeat after a crash
          between A and B.

Failure path: if the credit is impossible (destination missing / wrong currency)
the credit transaction rolls back, and Step B flips PROCESSING -> FAILED and
refunds the payer *in the same local transaction*. Money is never lost or created.
"""
from __future__ import annotations

import logging

from sqlalchemy.exc import IntegrityError

from app.core import outbox
from app.core.concurrency import AccountNotFoundError, CurrencyMismatchError, credit, refund
from app.core.state_machine import transition_state
from app.db.models import ProcessedEvent, TransactionState
from app.db.shard_router import ShardRegistry
from app.events import schemas as ev

logger = logging.getLogger(__name__)


def _apply_credit(registry: ShardRegistry, event: dict) -> tuple[str, str | None]:
    """Returns ('credited'|'already'|'rejected', reason)."""
    marker = f"credit:{event['transaction_id']}"
    try:
        with registry.session_for_shard(event["credit_shard_index"]) as session:
            session.add(ProcessedEvent(event_id=marker))
            session.flush()  # duplicate delivery fails right here
            credit(session, event["credit_account_id"], event["amount_minor"], event["currency"])
        return "credited", None
    except IntegrityError:
        return "already", None
    except (AccountNotFoundError, CurrencyMismatchError) as exc:
        # the session rolled back, taking the marker with it
        return "rejected", str(exc)


def _finalize_completed(registry: ShardRegistry, event: dict) -> None:
    debit_shard = registry.shard_for_account(event["debit_account_id"])
    with registry.session_for_shard(debit_shard) as session:
        if transition_state(session, event["transaction_id"], TransactionState.COMPLETED):
            outbox.enqueue(
                session,
                aggregate_id=event["transaction_id"],
                event_type=ev.TXN_COMPLETED,
                payload=ev.completed(
                    txn_id=event["transaction_id"],
                    amount_minor=event["amount_minor"],
                    currency=event["currency"],
                ),
            )


def _finalize_failed(registry: ShardRegistry, event: dict, reason: str) -> None:
    debit_shard = registry.shard_for_account(event["debit_account_id"])
    with registry.session_for_shard(debit_shard) as session:
        if transition_state(session, event["transaction_id"], TransactionState.FAILED, reason):
            refund(session, event["debit_account_id"], event["amount_minor"])
            outbox.enqueue(
                session,
                aggregate_id=event["transaction_id"],
                event_type=ev.TXN_FAILED,
                payload=ev.failed(
                    txn_id=event["transaction_id"],
                    amount_minor=event["amount_minor"],
                    currency=event["currency"],
                    reason=reason,
                ),
            )


def handle_transaction_debited(registry: ShardRegistry, event: dict) -> str:
    outcome, reason = _apply_credit(registry, event)
    if outcome == "rejected":
        _finalize_failed(registry, event, reason or "credit rejected")
    else:
        _finalize_completed(registry, event)
    return outcome
