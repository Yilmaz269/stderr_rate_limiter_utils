import logging
import sys
import unittest
from io import StringIO

from stderr_rate_limiter_utils import RateLimitedHandler
from stderr_rate_limiter_utils.core import RateLimitedHandler as CoreAlias


class FakeClock:
    """A controllable monotonic stand-in for time.monotonic."""

    def __init__(self, start=0.0):
        self.t = float(start)

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += float(seconds)


class _Sink(logging.Handler):
    """A handler that records every emit() invocation."""

    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def make_record(msg="boom"):
    return logging.LogRecord(
        name="test", level=logging.ERROR, pathname=__file__, lineno=1,
        msg=msg, args=None, exc_info=None,
    )


class TestConstruction(unittest.TestCase):
    def test_rejects_non_positive_rate(self):
        with self.assertRaises(ValueError):
            RateLimitedHandler(_Sink(), rate=0, capacity=5, clock=FakeClock())

    def test_rejects_negative_rate(self):
        with self.assertRaises(ValueError):
            RateLimitedHandler(_Sink(), rate=-1, capacity=5, clock=FakeClock())

    def test_rejects_sub_one_capacity(self):
        with self.assertRaises(ValueError):
            RateLimitedHandler(_Sink(), rate=1, capacity=0.5, clock=FakeClock())

    def test_rejects_zero_capacity(self):
        with self.assertRaises(ValueError):
            RateLimitedHandler(_Sink(), rate=1, capacity=0, clock=FakeClock())

    def test_capacity_floored_to_int(self):
        h = RateLimitedHandler(_Sink(), rate=1, capacity=3.9, clock=FakeClock())
        self.assertEqual(h.capacity, 3)
        self.assertEqual(h.tokens, 3)

    def test_initial_tokens_equal_capacity(self):
        h = RateLimitedHandler(_Sink(), rate=2, capacity=5, clock=FakeClock())
        self.assertEqual(h.tokens, 5)

    def test_exported_name_matches_core(self):
        self.assertIs(RateLimitedHandler, CoreAlias)


class TestTokenBucket(unittest.TestCase):
    def test_burst_up_to_capacity_passes_through(self):
        sink = _Sink()
        clock = FakeClock(start=10.0)
        h = RateLimitedHandler(sink, rate=1, capacity=3, clock=clock)
        for i in range(3):
            h.emit(make_record(str(i)))
        self.assertEqual(len(sink.records), 3)
        self.assertEqual(h.dropped, 0)

    def test_records_beyond_capacity_are_dropped(self):
        sink = _Sink()
        clock = FakeClock(start=10.0)
        h = RateLimitedHandler(sink, rate=1, capacity=2, clock=clock)
        for i in range(5):
            h.emit(make_record(str(i)))
        self.assertEqual(len(sink.records), 2)
        self.assertEqual(h.dropped, 3)

    def test_refill_one_token_per_second(self):
        sink = _Sink()
        clock = FakeClock(start=0.0)
        h = RateLimitedHandler(sink, rate=1, capacity=1, clock=clock)
        h.emit(make_record("first"))
        self.assertIn("first", sink.records[0].getMessage())
        dropped_before = h.dropped
        h.emit(make_record("dropped"))
        self.assertEqual(h.dropped, dropped_before + 1)
        clock.advance(1.0)
        h.emit(make_record("second"))
        self.assertEqual(len(sink.records), 2)
        self.assertIn("second", sink.records[1].getMessage())

    def test_refill_caps_at_capacity(self):
        sink = _Sink()
        clock = FakeClock(start=0.0)
        h = RateLimitedHandler(sink, rate=10, capacity=2, clock=clock)
        # Drain the bucket.
        h.emit(make_record())
        h.emit(make_record())
        # Wait long enough to refill well beyond capacity.
        clock.advance(100.0)
        self.assertEqual(h.tokens, 2)

    def test_sub_second_refill_accumulates(self):
        sink = _Sink()
        clock = FakeClock(start=0.0)
        # 4 tokens/sec, capacity 1. Quarter-second advances yield 1 token.
        h = RateLimitedHandler(sink, rate=4, capacity=1, clock=clock)
        h.emit(make_record("first"))
        self.assertEqual(len(sink.records), 1)
        clock.advance(0.25)
        h.emit(make_record("second"))
        self.assertEqual(len(sink.records), 2)
        self.assertEqual(h.dropped, 0)

    def test_partial_refill_below_one_token_is_not_rounded_up(self):
        sink = _Sink()
        clock = FakeClock(start=0.0)
        h = RateLimitedHandler(sink, rate=1, capacity=1, clock=clock)
        h.emit(make_record())
        clock.advance(0.5)  # half a token accrued -- floored to 0
        h.emit(make_record())
        self.assertEqual(h.dropped, 1)

    def test_clock_not_advancing_does_not_refill(self):
        sink = _Sink()
        clock = FakeClock(start=5.0)
        h = RateLimitedHandler(sink, rate=10, capacity=1, clock=clock)
        h.emit(make_record())
        # Same time, repeated calls must not grant a token.
        h.emit(make_record())
        h.emit(make_record())
        self.assertEqual(h.dropped, 2)

    def test_steady_state_throughput(self):
        sink = _Sink()
        clock = FakeClock(start=0.0)
        h = RateLimitedHandler(sink, rate=2, capacity=2, clock=clock)
        for _ in range(10):
            h.emit(make_record())
            clock.advance(0.5)  # 2 tokens/sec * 0.5s = 1 token per step
        self.assertEqual(len(sink.records), 10)
        self.assertEqual(h.dropped, 0)


class TestHandlerIntegration(unittest.TestCase):
    def test_wraps_real_stream_handler(self):
        buf = StringIO()
        inner = logging.StreamHandler(buf)
        inner.setFormatter(logging.Formatter("%(message)s"))
        clock = FakeClock(start=0.0)
        h = RateLimitedHandler(inner, rate=1, capacity=1, clock=clock)
        logger = logging.getLogger("rltest")
        logger.handlers = [h]
        logger.setLevel(logging.ERROR)
        logger.error("first")
        logger.error("dropped")
        self.assertEqual(buf.getvalue().strip(), "first")
        self.assertEqual(h.dropped, 1)

    def test_close_propagates_to_inner_handler(self):
        class ClosableSink(_Sink):
            closed = False
            def close(self):
                self.closed = True
        inner = ClosableSink()
        h = RateLimitedHandler(inner, rate=1, capacity=1, clock=FakeClock())
        self.assertFalse(inner.closed)
        h.close()
        self.assertTrue(inner.closed)


class TestProperties(unittest.TestCase):
    def test_rate_property(self):
        h = RateLimitedHandler(_Sink(), rate=3, capacity=5, clock=FakeClock())
        self.assertEqual(h.rate, 3.0)

    def test_dropped_starts_at_zero(self):
        h = RateLimitedHandler(_Sink(), rate=1, capacity=1, clock=FakeClock())
        self.assertEqual(h.dropped, 0)


if __name__ == "__main__":
    unittest.main()
