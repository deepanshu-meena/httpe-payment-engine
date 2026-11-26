"""Event types and payload builders. Plain dicts on purpose: the wire format is
obvious at a glance, and is evolved by *adding* keys, never changing existing ones."""
from __future__ import annotations

TXN_DEBITED = "transaction.debited"      # cross-shard: debit committed, credit still owed (drives settlement)
TXN_COMPLETED = "transaction.completed"  # terminal success (for notification/audit consumers)
TXN_FAILED = "transaction.failed"        # terminal failure, debit refunded


def debited(*, txn_id, debit_account_id, credit_account_id, credit_shard_index, amount_minor, currency) -> dict:
    return {
        "transaction_id": txn_id,
        "debit_account_id": debit_account_id,
        "credit_account_id": credit_account_id,
        "credit_shard_index": credit_shard_index,
        "amount_minor": amount_minor,
        "currency": currency,
    }


def completed(*, txn_id, amount_minor, currency) -> dict:
    return {"transaction_id": txn_id, "amount_minor": amount_minor, "currency": currency}


def failed(*, txn_id, amount_minor, currency, reason) -> dict:
    return {"transaction_id": txn_id, "amount_minor": amount_minor, "currency": currency, "reason": reason}
