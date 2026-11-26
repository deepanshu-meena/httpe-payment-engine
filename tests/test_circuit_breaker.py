import pytest

from app.core.circuit_breaker import BreakerState, CircuitBreaker, CircuitOpenError


class FakeClock:
    def __init__(self): self.now = 0.0
    def __call__(self): return self.now


def boom():
    raise RuntimeError("broker down")


def test_opens_after_threshold_then_fails_fast():
    b = CircuitBreaker(3, 60, clock=FakeClock())
    for _ in range(3):
        with pytest.raises(RuntimeError):
            b.call(boom)
    assert b.state == BreakerState.OPEN
    with pytest.raises(CircuitOpenError):
        b.call(lambda: "never runs")


def test_half_open_after_timeout_and_closes_on_success():
    clock = FakeClock()
    b = CircuitBreaker(1, 30, clock=clock)
    with pytest.raises(RuntimeError):
        b.call(boom)
    assert b.state == BreakerState.OPEN
    clock.now = 29
    assert b.state == BreakerState.OPEN
    clock.now = 30
    assert b.state == BreakerState.HALF_OPEN
    assert b.call(lambda: "ok") == "ok"
    assert b.state == BreakerState.CLOSED


def test_failed_half_open_trial_reopens_immediately():
    clock = FakeClock()
    b = CircuitBreaker(5, 30, clock=clock)
    for _ in range(5):
        with pytest.raises(RuntimeError):
            b.call(boom)
    clock.now = 31
    assert b.state == BreakerState.HALF_OPEN
    with pytest.raises(RuntimeError):
        b.call(boom)
    assert b.state == BreakerState.OPEN


async def test_async_variant():
    b = CircuitBreaker(2, 60, clock=FakeClock())

    async def fail(): raise RuntimeError()
    async def ok(): return 1

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await b.call_async(fail)
    with pytest.raises(CircuitOpenError):
        await b.call_async(ok)
