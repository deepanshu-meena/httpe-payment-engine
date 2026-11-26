import pytest

from app.core.state_machine import InvalidStateTransitionError, assert_transition, allowed_sources, transition_state
from app.db.models import Transaction, TransactionState as S
from tests.conftest import cross_shard_pair
from app.services.payments import create_payment


def test_legal_and_illegal_moves():
    assert_transition(S.PROCESSING, S.COMPLETED)
    assert_transition(S.PROCESSING, S.FAILED)
    for terminal in (S.COMPLETED, S.FAILED):
        for target in S:
            with pytest.raises(InvalidStateTransitionError):
                assert_transition(terminal, target)


def test_only_processing_can_become_terminal():
    assert allowed_sources(S.COMPLETED) == {S.PROCESSING}
    assert allowed_sources(S.FAILED) == {S.PROCESSING}


def test_conditional_transition_only_succeeds_once(registry):
    a, b = cross_shard_pair(registry)
    txn, _ = create_payment(registry, idempotency_key="k", debit_account_id=a, credit_account_id=b,
                            amount_minor=1000, currency="INR")
    with registry.session_for_account(a) as s:
        assert transition_state(s, txn.id, S.COMPLETED) is True
    with registry.session_for_account(a) as s:
        assert transition_state(s, txn.id, S.COMPLETED) is False   # already terminal
        assert transition_state(s, txn.id, S.FAILED, "x") is False  # cannot leave a terminal state
        assert s.get(Transaction, txn.id).state == S.COMPLETED
