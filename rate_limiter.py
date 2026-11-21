import time
import threading
from collections import deque
from typing import Deque, Dict, Tuple, TypedDict, Union

Key = Tuple[str, str]
Decision = Tuple[bool, int, int]


class StatsDict(TypedDict):
    strategy: str
    total_allowed: int
    total_rejected: int
    active_keys: int


class _Bucket:
    """Token-bucket state for one (client_id, endpoint)."""
    __slots__ = ("tokens", "last")

    def __init__(self, tokens: float, last: float):
        self.tokens = tokens  # fractional budget, never rounded internally
        self.last = last      # monotonic seconds of the last refill


class RateLimiter:
    def __init__(self, strategy: str = "policy_a"):
        if strategy not in ("policy_a", "policy_b"):
            raise ValueError(f"Unknown strategy: {strategy}")

        self.strategy = strategy
        self._lock = threading.Lock()

        # Policy A: key -> deque of accepted-request timestamps (ms, monotonic)
        # Policy B: key -> _Bucket
        self._state: Dict[Key, Union[Deque[float], _Bucket]] = {}

        self._total_allowed = 0
        self._total_rejected = 0

    def check(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """
        Run the configured policy and return (allowed, remaining, reset_after_ms).

        The whole read-evaluate-update sequence (clock read, eviction/refill,
        decision, state mutation, stats update) runs under one lock so that
        concurrent callers can never both consume the same capacity.
        """
        with self._lock:
            if self.strategy == "policy_a":
                result = self.policy_a_limit(client_id, endpoint, limit, window_ms)
            else:
                result = self.policy_b_limit(client_id, endpoint, capacity, refill_rate)

            if result[0]:
                self._total_allowed += 1
            else:
                self._total_rejected += 1

            return result

    # NOTE: the policy methods assume the caller already holds self._lock.
    # threading.Lock is not re-entrant, so they must not acquire it again.

    def policy_a_limit(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
    ) -> Decision:
        """
        Policy A (sliding window log):
        allow at most `limit` accepted requests in the most recent `window_ms` ms.
        """
        now = time.monotonic() * 1000.0
        key = (client_id, endpoint)

        q = self._state.get(key)
        if not isinstance(q, deque):
            q = deque()
            self._state[key] = q

        # Evict expired accepted requests before evaluating this one.
        cutoff = now - window_ms
        while q and q[0] <= cutoff:
            q.popleft()

        if len(q) < limit:
            q.append(now)  # only accepted requests are recorded
            allowed = True
            remaining = limit - len(q)
        else:
            allowed = False
            remaining = 0

        # Time until the oldest counted accepted request stops being counted.
        if q:
            reset_after_ms = max(0, int(q[0] + window_ms - now))
        else:
            reset_after_ms = 0

        return (allowed, remaining, reset_after_ms)

    def policy_b_limit(
        self,
        client_id: str,
        endpoint: str,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """
        Policy B (token bucket):
        bucket starts full at `capacity`, refills continuously at `refill_rate`
        tokens/second (capped at capacity); each accepted request costs 1 token.
        """
        now = time.monotonic()
        key = (client_id, endpoint)
        cap = float(capacity)

        b = self._state.get(key)
        if not isinstance(b, _Bucket):
            b = _Bucket(cap, now)  # new bucket starts full
            self._state[key] = b
        else:
            elapsed = max(0.0, now - b.last)
            b.tokens = min(cap, b.tokens + elapsed * refill_rate)
            b.last = now

        if b.tokens >= 1.0:
            b.tokens -= 1.0
            return (True, max(0, int(b.tokens)), 0)

        # Rejected: nothing is consumed.
        if refill_rate > 0:
            reset_after_ms = max(0, int((1.0 - b.tokens) / refill_rate * 1000.0))
        else:
            reset_after_ms = 0  # capacity never recovers; any non-negative int allowed
        return (False, 0, reset_after_ms)

    def stats(self) -> StatsDict:
        """
        Return aggregate limiter statistics.

        active_keys counts tracked (client_id, endpoint) entries for the active
        policy state.
        """
        with self._lock:
            return {
                "strategy": self.strategy,
                "total_allowed": self._total_allowed,
                "total_rejected": self._total_rejected,
                "active_keys": len(self._state),
            }
