# In-Memory Rate Limiter for an LLM API Gateway

A thread-safe, in-memory rate limiter for an LLM API gateway where several clients share endpoints such as `/v1/chat` and `/v1/complete`. It stops any single client from consuming all available capacity. It supports two strategies, **Sliding Window** and **Token Bucket**, and the active one is selected at runtime.

Built in pure Python using only the standard library. An `aiohttp` demo server sits on top of it.

## Features

- **Two policies**, selected with the `STRATEGY` environment variable
  - `policy_a`: Sliding Window. At most `limit` requests in the latest `window_ms` milliseconds.
  - `policy_b`: Token Bucket. A burst of up to `capacity` requests, refilled continuously at `refill_rate` tokens/second.
- **Per-client, per-endpoint isolation.** Each `(client_id, endpoint)` pair has its own state.
- **Client-facing metadata.** Every decision returns the remaining capacity and the time until recovery.
- **Thread-safe.** Each decision and its statistics update happen atomically under one lock.
- **Live statistics.** Reports allowed and rejected totals and the number of active keys.

## How it works

`RateLimiter.check(...)` returns a tuple:

```python
(allowed: bool, remaining: int, reset_after_ms: int)
```

| Field | Meaning |
|---|---|
| `allowed` | `True` if the request may proceed. Rejected requests never consume capacity. |
| `remaining` | Requests that can still be accepted **right now**, after this decision. Always `0` when rejected. |
| `reset_after_ms` | **Policy A:** time until the oldest counted request leaves the window. **Policy B:** `0` when accepted; otherwise the time until one full token is available. |

### Policy A: Sliding Window

Each key keeps a `deque` of timestamps for accepted requests. On every check:
1. Timestamps older than `now - window_ms` are evicted.
2. If fewer than `limit` remain, the request is accepted and its timestamp is appended.
3. `reset_after_ms = oldest_timestamp + window_ms - now`

Example with `limit=2, window_ms=10000`:

```
0 ms     -> (True,  1, 10000)
3000 ms  -> (True,  0, 7000)
3500 ms  -> (False, 0, 6500)
```

### Policy B: Token Bucket

Each key keeps a fractional token count and the time of its last refill. On every check:
1. `tokens = min(capacity, tokens + elapsed_seconds * refill_rate)`. The value is never rounded internally.
2. If `tokens >= 1`, one token is consumed and the request is accepted.
3. Otherwise the request is rejected, with `reset_after_ms = (1 - tokens) / refill_rate * 1000`.

Example with `capacity=3, refill_rate=0.5`:

```
request 1 -> (True,  2, 0)
request 2 -> (True,  1, 0)
request 3 -> (True,  0, 0)
request 4 -> (False, 0, ~2000)
```

### Thread safety

`check()` holds a single `threading.Lock` around the whole read, evaluate and update sequence: clock read, eviction or refill, decision, state update, and counter increment. As a result, two concurrent requests can never both claim the last slot. `stats()` uses the same lock, so it always returns a consistent snapshot. Clocks use `time.monotonic()`, so wall-clock changes cannot produce negative timers.

See [DESIGN.md](DESIGN.md) for locking trade-offs, ways to scale (lock striping, TTL cleanup), and how the design would move to Redis in a multi-instance deployment.

## Project structure

```
.
├── rate_limiter.py       # RateLimiter: Policy A, Policy B, stats
├── server.py             # aiohttp demo server (/v1/chat, /v1/complete, /stats)
├── test_rate_limiter.py  # Unit tests: limits, recovery, refill, isolation, concurrency
├── test_client.py        # Smoke test against a running server
├── DESIGN.md             # Concurrency and scaling design notes
└── requirements.txt
```

## Getting started

```bash
git clone https://github.com/Hafsa-Anwar65/rate-limiter.git
cd rate-limiter
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Run the tests

```bash
python3 -m pytest -v
```

### Run the server

```bash
STRATEGY=policy_a python3 server.py   # Sliding Window
# or
STRATEGY=policy_b python3 server.py   # Token Bucket
```

The server listens on `http://localhost:8080` with `LIMIT=5`, `WINDOW_MS=10000`, `CAPACITY=5` and `REFILL_RATE=0.5`.

In a second terminal:

```bash
python3 test_client.py
```

Or use curl:

```bash
curl -i -X POST localhost:8080/v1/chat -H "X-Client-ID: alice"
curl localhost:8080/stats
```

Every response includes these headers:

```
X-RateLimit-Remaining: 4
X-RateLimit-Reset-Ms: 10000
X-RateLimit-Policy: policy_a
```

A rejected request returns `429 Rate limit exceeded`.

## Usage as a library

```python
from rate_limiter import RateLimiter

limiter = RateLimiter(strategy="policy_b")
allowed, remaining, reset_ms = limiter.check(
    client_id="alice",
    endpoint="/v1/chat",
    limit=0, window_ms=0,           # unused by policy_b
    capacity=10, refill_rate=2.0,
)
print(limiter.stats())
# {'strategy': 'policy_b', 'total_allowed': 1, 'total_rejected': 0, 'active_keys': 1}
```

## Author

**Hafsa Anwar**, AI/ML & Robotics Engineer
[GitHub](https://github.com/Hafsa-Anwar65) · [LinkedIn](https://www.linkedin.com/in/hafsa-anwar-53b251313)
