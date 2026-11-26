from decimal import Decimal

import pytest

from app.db.shard_router import shard_index_for
from app.money import from_minor, to_minor


def test_money_round_trip_is_exact():
    assert to_minor(Decimal("0.10")) == 10
    assert from_minor(10) == Decimal("0.10")
    assert to_minor(Decimal("1234.56")) == 123456


def test_money_rejects_sub_minor_units():
    with pytest.raises(ValueError):
        to_minor(Decimal("0.001"))


def test_routing_is_deterministic_and_in_range():
    assert shard_index_for("acct-1", 4) == shard_index_for("acct-1", 4)
    assert all(0 <= shard_index_for(f"a{i}", 4) < 4 for i in range(500))


def test_routing_spreads_evenly():
    counts = [0] * 4
    for i in range(4000):
        counts[shard_index_for(f"acct-{i}", 4)] += 1
    assert all(800 < c < 1200 for c in counts), counts
