"""Balance mutation under concurrency.

The problem: two requests debit the same account at the same instant. With
read-modify-write ("SELECT balance; compute; UPDATE") both read 100, both write
40, and 60 is lost twice for the price of once (the *lost update* anomaly).

Our approach: a single atomic, conditional UPDATE that does the check and the
change in one statement:

    UPDATE accounts
       SET balance_minor = balance_minor - :amt, version = version + 1
     WHERE id = :id AND currency = :cur AND balance_minor >= :amt

rowcount == 1  -> it happened.   rowcount == 0 -> it did not (we then diagnose why).

Why this and not SELECT ... FOR UPDATE? It is correct on *every* database (the
database serialises concurrent UPDATEs of one row), needs no explicit lock
handling or retry loop, holds the row lock for the shortest possible time, and
can be tested for real on SQLite. FOR UPDATE is a no-op on SQLite, which would
have left the core guarantee unverifiable in the fast test suite. The CHECK
constraint `balance_minor >= 0` on the table is a second, independent safety net.
See DECISIONS.md #3.
"""
from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models import Account


class AccountNotFoundError(Exception):
    pass


class InsufficientFundsError(Exception):
    pass


class CurrencyMismatchError(Exception):
    pass


def _diagnose(session: Session, account_id: str, currency: str, *, for_debit: bool, amount: int):
    row = session.execute(
        select(Account.currency, Account.balance_minor).where(Account.id == account_id)
    ).one_or_none()
    if row is None:
        raise AccountNotFoundError(f"account {account_id} not found")
    if row.currency != currency:
        raise CurrencyMismatchError(
            f"account {account_id} is in {row.currency}, payment is in {currency}"
        )
    if for_debit:
        raise InsufficientFundsError(
            f"account {account_id} has insufficient funds for this payment"
        )
    raise RuntimeError("credit failed for an unknown reason")  # pragma: no cover


def debit(session: Session, account_id: str, amount_minor: int, currency: str) -> None:
    result = session.execute(
        update(Account)
        .where(
            Account.id == account_id,
            Account.currency == currency,
            Account.balance_minor >= amount_minor,
        )
        .values(balance_minor=Account.balance_minor - amount_minor, version=Account.version + 1)
    )
    if result.rowcount != 1:
        _diagnose(session, account_id, currency, for_debit=True, amount=amount_minor)


def credit(session: Session, account_id: str, amount_minor: int, currency: str) -> None:
    result = session.execute(
        update(Account)
        .where(Account.id == account_id, Account.currency == currency)
        .values(balance_minor=Account.balance_minor + amount_minor, version=Account.version + 1)
    )
    if result.rowcount != 1:
        _diagnose(session, account_id, currency, for_debit=False, amount=amount_minor)


def refund(session: Session, account_id: str, amount_minor: int) -> None:
    """Return a previously debited amount. Unconditional: the account existed
    when it was debited, and accounts are never deleted."""
    session.execute(
        update(Account)
        .where(Account.id == account_id)
        .values(balance_minor=Account.balance_minor + amount_minor, version=Account.version + 1)
    )
