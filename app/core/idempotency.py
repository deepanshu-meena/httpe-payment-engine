"""Idempotent payment creation.

Contract: every payment request carries an `Idempotency-Key`. Sending the same
key again (client retry after a timeout, double tap, upstream at-least-once
delivery) must return the *original* transaction and must never move money a
second time.

How it is enforced: by a UNIQUE constraint on `transactions.idempotency_key`
(app/db/models.py), not by an application-level "have I seen this key?" check.
Check-then-insert in application code is itself a race (two requests both pass
the check, both insert). The unique index makes the guarantee atomic: whichever
INSERT loses the race fails with IntegrityError, and we return the winner's row.

Ordering matters: the transaction row is inserted FIRST, then money moves, all in
one database transaction. If the insert loses the race, no money has moved yet in
this attempt, so there is nothing to undo.

Reuse protection: we also store a fingerprint of the request body. Re-sending an
existing key with a *different* body is a client bug (or an attack), so it is
rejected (HTTP 409) rather than silently returning an unrelated transaction.
"""
from __future__ import annotations

import hashlib

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import Transaction


class IdempotencyKeyReuseError(Exception):
    pass


def fingerprint(debit_account_id: str, credit_account_id: str, amount_minor: int, currency: str) -> str:
    raw = f"{debit_account_id}|{credit_account_id}|{amount_minor}|{currency}"
    return hashlib.sha256(raw.encode()).hexdigest()


def insert_or_get_existing(session: Session, txn: Transaction) -> tuple[Transaction, bool]:
    """Insert `txn`. Returns (transaction, replayed).

    On a duplicate key the session is rolled back (nothing from this attempt
    persists) and the original row is returned with replayed=True.
    """
    try:
        session.add(txn)
        session.flush()  # force the unique constraint to be evaluated now
        return txn, False
    except IntegrityError:
        session.rollback()
        existing = session.execute(
            select(Transaction).where(Transaction.idempotency_key == txn.idempotency_key)
        ).scalar_one_or_none()
        if existing is None:
            raise  # an IntegrityError that was not the idempotency constraint
        if existing.request_fingerprint != txn.request_fingerprint:
            raise IdempotencyKeyReuseError(
                "Idempotency-Key was already used with a different request body"
            )
        return existing, True
