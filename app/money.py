"""Money is stored as integer minor units (paise/cents), never floats.

Why: float (and SQLite's NUMERIC-as-float) cannot represent 0.10 exactly, and
rounding drift in a ledger is a correctness bug. Integers are exact on every
database and make `balance >= amount` guards trivial. The API accepts/returns
decimal strings; conversion happens only at the edge, here.
"""
from __future__ import annotations

from decimal import Decimal

MINOR_PER_MAJOR = 100  # all supported currencies assumed 2-decimal (INR, USD, EUR)


def to_minor(amount: Decimal) -> int:
    scaled = Decimal(amount) * MINOR_PER_MAJOR
    if scaled != scaled.to_integral_value():
        raise ValueError("amount has more than 2 decimal places")
    return int(scaled)


def from_minor(minor: int) -> Decimal:
    return (Decimal(minor) / MINOR_PER_MAJOR).quantize(Decimal("0.01"))
