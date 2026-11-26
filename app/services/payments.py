"""Payment creation: the synchronous, user-facing half of the system.

Two paths, chosen by whether both accounts live on the same shard:

SAME-SHARD  debit + credit + COMPLETED + outbox event happen in ONE local database
            transaction. Real ACID: all of it or none of it.

CROSS-SHARD Shards share no transaction coordinator (that is the point of
            sharding), so one ACID transaction across them is not available.
            Instead we run a SAGA:
              1. (this module) in ONE local tx on the debit shard: insert the
                 transaction as PROCESSING, debit, write a `transaction.debited`
                 outbox event. The event can never be lost, because it commits
                 atomically with the debit.
              2. (services/settlement.py) a worker credits the destination in its
                 own local tx, then marks the transaction COMPLETED - or, if the
                 credit is impossible, refunds the debit and marks it FAILED.
            Between 1 and 2 the money has left the payer but the payee has not
            received it, and the transaction reads PROCESSING. That window is an
            explicit, documented consequence of horizontal sharding, and the
            system is *eventually consistent* across shards, never inconsistent
            forever. See DECISIONS.md #5.
"""
from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from app.core import outbox
from app.core.concurrency import credit, debit
from app.core.idempotency import fingerprint, insert_or_get_existing
from app.db.models import Transaction, TransactionState
from app.db.shard_router import ShardRegistry
from app.events import schemas as ev


class PaymentRejectedError(Exception):
    """A business-rule rejection (unknown account, insufficient funds, currency
    mismatch). Nothing was persisted; the client may retry the same key later."""


class TransactionNotFoundError(Exception):
    pass


def new_transaction_id(debit_shard: int) -> str:
    """'<debit_shard>-<uuid>': the shard is encoded in the id so a transaction can
    be located from its id alone, without a global lookup table."""
    return f"{debit_shard}-{uuid.uuid4()}"


def shard_from_transaction_id(txn_id: str, shard_count: int) -> int:
    head, sep, _ = txn_id.partition("-")
    if not sep or not head.isdigit() or int(head) >= shard_count:
        raise TransactionNotFoundError(txn_id)
    return int(head)


def create_payment(
    registry: ShardRegistry,
    *,
    idempotency_key: str,
    debit_account_id: str,
    credit_account_id: str,
    amount_minor: int,
    currency: str,
) -> tuple[Transaction, bool]:
    """Returns (transaction, replayed). `replayed` is True when the idempotency
    key had been seen before and the original transaction is being returned."""
    debit_shard = registry.shard_for_account(debit_account_id)
    credit_shard = registry.shard_for_account(credit_account_id)
    same_shard = debit_shard == credit_shard

    txn = Transaction(
        id=new_transaction_id(debit_shard),
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint(debit_account_id, credit_account_id, amount_minor, currency),
        debit_account_id=debit_account_id,
        credit_account_id=credit_account_id,
        credit_shard_index=credit_shard,
        amount_minor=amount_minor,
        currency=currency,
        state=TransactionState.COMPLETED if same_shard else TransactionState.PROCESSING,
    )

    with registry.session_for_shard(debit_shard) as session:
        return _run(session, txn, same_shard=same_shard)


def _run(session: Session, txn: Transaction, *, same_shard: bool) -> tuple[Transaction, bool]:
    from app.core.concurrency import AccountNotFoundError, CurrencyMismatchError, InsufficientFundsError

    # 1) claim the idempotency key FIRST (atomic via the unique constraint).
    txn, replayed = insert_or_get_existing(session, txn)
    if replayed:
        return txn, True

    # 2) move money. Any failure raises -> the session context manager rolls the
    #    whole local transaction back, including the claim from step 1.
    try:
        if same_shard:
            # Deadlock avoidance: A->B and B->A running concurrently would lock the
            # two rows in opposite orders (each holds one, waits for the other) and
            # Postgres would abort one. Always touch the rows in a fixed global
            # order (by account id). Safe to reorder: if either step fails the whole
            # local transaction rolls back, so the order has no semantic effect.
            if txn.debit_account_id < txn.credit_account_id:
                debit(session, txn.debit_account_id, txn.amount_minor, txn.currency)
                credit(session, txn.credit_account_id, txn.amount_minor, txn.currency)
            else:
                credit(session, txn.credit_account_id, txn.amount_minor, txn.currency)
                debit(session, txn.debit_account_id, txn.amount_minor, txn.currency)
        else:
            debit(session, txn.debit_account_id, txn.amount_minor, txn.currency)
    except (AccountNotFoundError, InsufficientFundsError, CurrencyMismatchError) as exc:
        raise PaymentRejectedError(str(exc)) from exc

    # 3) outbox event, same transaction.
    if same_shard:
        outbox.enqueue(
            session,
            aggregate_id=txn.id,
            event_type=ev.TXN_COMPLETED,
            payload=ev.completed(txn_id=txn.id, amount_minor=txn.amount_minor, currency=txn.currency),
        )
    else:
        outbox.enqueue(
            session,
            aggregate_id=txn.id,
            event_type=ev.TXN_DEBITED,
            payload=ev.debited(
                txn_id=txn.id,
                debit_account_id=txn.debit_account_id,
                credit_account_id=txn.credit_account_id,
                credit_shard_index=txn.credit_shard_index,
                amount_minor=txn.amount_minor,
                currency=txn.currency,
            ),
        )
    return txn, False


def get_payment(registry: ShardRegistry, txn_id: str) -> Transaction:
    shard = shard_from_transaction_id(txn_id, registry.shard_count)
    with registry.session_for_shard(shard) as session:
        txn = session.get(Transaction, txn_id)
    if txn is None:
        raise TransactionNotFoundError(txn_id)
    return txn
