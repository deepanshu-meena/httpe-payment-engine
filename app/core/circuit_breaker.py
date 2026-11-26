"""A small, dependency-free circuit breaker guarding the outbox relay's publish call.

Why: if the broker is down, a naive relay retries every row in a tight loop,
hammering an already-unhealthy broker and burning CPU. After
`failure_threshold` consecutive failures the breaker OPENS and fails fast for
`reset_timeout_seconds`. Nothing is lost while it is open - rows simply stay
unpublished in the outbox table and go out once the broker recovers. After the
timeout it goes HALF_OPEN and lets one trial call through: success closes it,
failure re-opens it.

The clock is injectable so tests can advance time deterministically instead of
sleeping.
"""
from __future__ import annotations

import time
from enum import Enum
from typing import Callable


class BreakerState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    pass


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int,
        reset_timeout_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.failure_threshold = failure_threshold
        self.reset_timeout_seconds = reset_timeout_seconds
        self._clock = clock
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> BreakerState:
        if self._state == BreakerState.OPEN and self._opened_at is not None:
            if self._clock() - self._opened_at >= self.reset_timeout_seconds:
                self._state = BreakerState.HALF_OPEN
        return self._state

    def _before(self) -> None:
        if self.state == BreakerState.OPEN:
            raise CircuitOpenError("circuit open: skipping call")

    def _on_failure(self) -> None:
        self._failures += 1
        # A failed half-open trial re-opens immediately; otherwise wait for the threshold.
        if self._state == BreakerState.HALF_OPEN or self._failures >= self.failure_threshold:
            self._state = BreakerState.OPEN
            self._opened_at = self._clock()

    def _on_success(self) -> None:
        self._failures = 0
        self._state = BreakerState.CLOSED
        self._opened_at = None

    def call(self, fn, *args, **kwargs):
        self._before()
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._on_failure()
            raise
        self._on_success()
        return result

    async def call_async(self, fn, *args, **kwargs):
        self._before()
        try:
            result = await fn(*args, **kwargs)
        except Exception:
            self._on_failure()
            raise
        self._on_success()
        return result
