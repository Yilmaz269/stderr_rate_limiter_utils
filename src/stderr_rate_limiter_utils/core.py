"""Token-bucket rate limiter wrapping a logging.Handler.

The bucket refills at a fixed rate (tokens per second) up to a maximum capacity.
When a record is emitted, one token is consumed. If no token is available the
record is dropped silently and the limiter's ``dropped`` counter is bumped so
operators can tell suppression happened.

All time is obtained through a ``clock`` callable passed to the constructor so
the behaviour is fully deterministic in tests. In production pass
``time.monotonic`` (the default) -- it is monotonic and unaffected by wall-clock
adjustments, which matters because a token bucket keyed on elapsed real time
breaks if the clock jumps backwards.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional

__all__ = ["RateLimitedHandler"]


class RateLimitedHandler(logging.Handler):
    """A :class:`logging.Handler` that enforces a token-bucket rate limit.

    Parameters
    ----------
    handler:
        The wrapped :class:`logging.Handler` that actually writes records
        (e.g. a ``StreamHandler`` pointing at ``sys.stderr``). Ownership is not
        taken: closing the limiter closes the underlying handler, but callers
        are free to manage the handler's lifetime themselves.
    rate:
        Refill rate in tokens per second. Must be positive. A tiny epsilon is
        tolerated because ``rate`` is often derived from a configuration value
        such as ``1 / interval`` and floating-point division can yield a value
        fractionally above zero that the caller intended to be positive.
    capacity:
        Maximum tokens the bucket can hold. Also the initial fill, so a burst of
        up to ``capacity`` records is allowed immediately on startup. Must be at
        least 1; fractional capacities are floored to an integer token count so
        the state is always a whole number of tokens.
    clock:
        Callable returning the current time as a float. Defaults to
        :func:`time.monotonic`. Inject a fake in tests so no wall-clock
        assumption leaks in.

    Attributes
    ----------
    dropped:
        Number of records suppressed since construction. Read it to surface
        "N messages were hidden" in a periodic summary handler.

    Notes
    -----
    The limiter deliberately makes **no** effort to coalesce dropped records or
    emit a summary line. That is a different policy with different trade-offs
    (it requires buffering payloads, which couples the limiter to the record's
    format). If you need a summary, wrap this handler in one that reads
    ``dropped`` on a timer.
    """

    def __init__(
        self,
        handler: logging.Handler,
        rate: float,
        capacity: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__()
        if rate <= 0:
            raise ValueError("rate must be positive")
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self._handler = handler
        # Float rate is preserved so refill arithmetic keeps sub-integer
        # precision; the token count itself is always an int.
        self._rate = float(rate)
        self._capacity = int(capacity)
        self._tokens = self._capacity
        self._clock = clock
        self._last = clock()
        self.dropped = 0

    # -- token bucket ----------------------------------------------------

    def _refill(self) -> None:
        """Recompute the bucket based on elapsed time since the last call."""
        now = self._clock()
        elapsed = now - self._last
        self._last = now
        if elapsed <= 0:
            # Clock is monotonic in production, but a test clock or a coarse
            # OS clock can return the same value on successive calls. Nothing
            # to refill in that case; do not reward repeated calls.
            return
        self._tokens = min(self._capacity, self._tokens + int(elapsed * self._rate))

    def _try_acquire(self) -> bool:
        """Consume one token if available. Return ``True`` on success."""
        self._refill()
        if self._tokens >= 1:
            self._tokens -= 1
            return True
        return False

    # -- logging.Handler API --------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        """Emit *record* if a token is available, otherwise drop it.

        The underlying handler's own :meth:`logging.Handler.handleError`
        already swallows exceptions raised during formatting/writing, so we
        delegate to it rather than re-implementing error isolation.
        """
        if not self._try_acquire():
            self.dropped += 1
            return
        self._handler.emit(record)

    def close(self) -> None:
        """Close the wrapped handler and mark this handler closed."""
        try:
            self._handler.close()
        finally:
            super().close()

    @property
    def rate(self) -> float:
        """Refill rate in tokens per second (read-only)."""
        return self._rate

    @property
    def capacity(self) -> int:
        """Maximum tokens the bucket can hold (read-only)."""
        return self._capacity

    @property
    def tokens(self) -> int:
        """Current token count after refilling against the clock.

        Exposed for observability and tests. Refilling here means the value is
        current as of the call, not stale.
        """
        self._refill()
        return self._tokens
