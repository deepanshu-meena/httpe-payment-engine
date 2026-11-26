from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from app.db.models import Account
from app.db.shard_router import ShardRegistry, shard_index_for
from app.money import to_minor
from decimal import Decimal


@pytest.fixture()
def registry(tmp_path: Path):
    """4 throwaway SQLite shards, isolated per test."""
    reg = ShardRegistry([f"sqlite:///{tmp_path}/shard_{i}.db" for i in range(4)])
    reg.create_all()
    yield reg
    reg.dispose()


def make_account_on_shard(reg: ShardRegistry, shard: int, balance: str = "100.00", currency: str = "INR") -> str:
    """Account ids are random UUIDs and routing is a hash, so we cannot *choose* a
    shard directly - we draw ids until one lands where the test needs it."""
    for _ in range(1000):
        account_id = str(uuid.uuid4())
        if shard_index_for(account_id, reg.shard_count) == shard:
            with reg.session_for_shard(shard) as s:
                s.add(Account(id=account_id, owner_name="t", balance_minor=to_minor(Decimal(balance)), currency=currency))
            return account_id
    raise RuntimeError("no id found for shard")


def same_shard_pair(reg, balance_a="100.00", balance_b="0.00"):
    return make_account_on_shard(reg, 0, balance_a), make_account_on_shard(reg, 0, balance_b)


def cross_shard_pair(reg, balance_a="100.00", balance_b="0.00"):
    return make_account_on_shard(reg, 0, balance_a), make_account_on_shard(reg, 1, balance_b)


def balance_minor(reg: ShardRegistry, account_id: str) -> int:
    with reg.session_for_account(account_id) as s:
        return s.get(Account, account_id).balance_minor
