"""stderr_rate_limiter_utils: a token-bucket rate limiter for logging handlers.

Exports:
    RateLimitedHandler -- logging.Handler wrapper enforcing a token-bucket limit.
"""

from stderr_rate_limiter_utils.core import RateLimitedHandler

__all__ = ["RateLimitedHandler"]
