"""Transaction lifecycle enforcement.

`VALID_TRANSITIONS` (app/db/models.py) is the single source of truth for which
moves are legal. Every state change goes through `transition_state`, which is a
*conditional UPDATE* (`... WHERE id = :id AND state IN (<legal sources>)`).

Why conditional UPDATE instead of "load row, check, set, commit"? Two settlement
workers can process the same (redelivered) event concurrently. With
load-check-set both could see PROCESSING and both act. With the conditional
UPDATE the database lets exactly one of them flip the state; the other gets
rowcount 0 and does nothing. The caller uses that boolean to decide whether to
perform side effects (e.g. the refund on FAILED), so side effects happen once.
"""
from __future__ import annotations

from sqlalchemy import update
from sqlalchemy.orm import Session

from app.db.models import VALID_TRANSITIONS, Transaction, TransactionState


class InvalidStateTransitionError(Exception):
    pass


def allowed_sources(to_state: TransactionState) -> set[TransactionState]:
    return {src for src, targets in VALID_TRANSITIONS.items() if to_state in targets}


def assert_transition(current: TransactionState, to_state: TransactionState) -> None:
    if to_state not in VALID_TRANSITIONS.get(current, set()):
        raise InvalidStateTransitionError(f"illegal transition {current.value} -> {to_state.value}")


def transition_state(
    session: Session, txn_id: str, to_state: TransactionState, reason: str | None = None
) -> bool:
    """Atomically move a transaction to `to_state`. True iff *this call* moved it."""
    sources = allowed_sources(to_state)
    if not sources:
        raise InvalidStateTransitionError(f"no state may transition into {to_state.value}")
    values: dict = {"state": to_state}
    if reason is not None:
        values["failure_reason"] = reason
    result = session.execute(
        update(Transaction)
        .where(Transaction.id == txn_id, Transaction.state.in_(sources))
        .values(**values)
    )
    return result.rowcount == 1
