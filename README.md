# stderr_rate_limiter_utils

A `logging.Handler` wrapper that enforces a token-bucket rate limit so noisy error streams do not flood stderr during incidents.

```python
import logging, sys
from stderr_rate_limiter_utils import RateLimitedHandler

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
limiter = RateLimitedHandler(stderr_handler, rate=5, capacity=20)

logger = logging.getLogger("app")
logger.addHandler(limiter)
logger.setLevel(logging.ERROR)
```

`RateLimitedHandler(handler, rate, capacity, clock=time.monotonic)` wraps any `logging.Handler`. `rate` is tokens per second; `capacity` is the bucket size and the initial fill, so up to `capacity` records pass immediately. Records beyond the budget are dropped and counted in `limiter.dropped`. The bucket refills on each `emit` call based on elapsed time from `clock`.

## Why this exists

During an incident a tight error loop can write thousands of identical tracebacks a second to stderr, crowding out the one diagnostic line that matters and pushing older log context off the screen. A token bucket caps the write rate while letting the first burst through, so you still see the start of the failure and then a steady sample of its continuation.

The trade-off is suppression without summaries. This handler drops records silently and bumps `dropped`; it does not coalesce them or print "… N messages hidden …". Coalescing requires buffering payloads and coupling to the record format, which is a different policy. If you need a summary, read `dropped` from a separate timer and emit your own line.

## The awkward edge

The bucket refills in whole tokens: `int(elapsed * rate)` is floored, so a fractional refill below one token is held until the next call accrues enough. At a `rate` of 1 token/sec, two `emit` calls 0.5s apart will not let the second one through even though "on average" it should. This keeps the state an integer token count and makes the limit a true floor on emissions, not an amortised average. If you need finer granularity, raise `rate` and `capacity` together.

## Design notes

The window stores values eagerly rather than keeping running aggregates. Running
sums drift with floating point over long streams, and recomputing from a small
buffer is cheap enough that the drift is not worth the speed.

