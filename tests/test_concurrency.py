import threading

import pytest

from app.core.concurrency import InsufficientFundsError, debit
from tests.conftest import balance_minor, make_account_on_shard


def test_concurrent_debits_never_lose_updates_or_overdraw(registry):
    """30 threads each try to debit 10.00 from a 100.00 account.
    Exactly 10 must succeed, 20 must be rejected, final balance exactly 0.
    A lost update would let more than 10 succeed or leave a wrong balance."""
    acct = make_account_on_shard(registry, 0, "100.00")
    ok, rejected, other = [], [], []
    lock = threading.Lock()

    def worker():
        try:
            with registry.session_for_account(acct) as s:
                debit(s, acct, 1000, "INR")
            with lock: ok.append(1)
        except InsufficientFundsError:
            with lock: rejected.append(1)
        except Exception as exc:
            with lock: other.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(30)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    assert not other, other
    assert len(ok) == 10 and len(rejected) == 20
    assert balance_minor(registry, acct) == 0


def test_rejected_debit_changes_nothing(registry):
    acct = make_account_on_shard(registry, 0, "5.00")
    with pytest.raises(InsufficientFundsError):
        with registry.session_for_account(acct) as s:
            debit(s, acct, 1000, "INR")
    assert balance_minor(registry, acct) == 500
