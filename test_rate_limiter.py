import threading
import time

from rate_limiter import RateLimiter


def test_recovery_after_expiry():
    rl = RateLimiter("policy_a")
    rl.check("a", "/v1/chat", 2, 120, 0, 0)
    rl.check("a", "/v1/chat", 2, 120, 0, 0)
    assert rl.check("a", "/v1/chat", 2, 120, 0, 0)[0] is False
    time.sleep(0.14)
    ok, rem, reset = rl.check("a", "/v1/chat", 2, 120, 0, 0)
    assert ok and rem == 1 and 100 <= reset <= 120


def test_partial_refill():
    rl = RateLimiter("policy_b")
    for _ in range(3):
        rl.check("a", "/v1/chat", 0, 0, 3, 0.5)
    time.sleep(1.2)
    ok, rem, reset = rl.check("a", "/v1/chat", 0, 0, 3, 0.5)
    assert not ok and rem == 0 and 700 <= reset <= 800


def test_isolation():
    rl = RateLimiter("policy_b")
    for _ in range(3):
        rl.check("a", "/v1/chat", 0, 0, 3, 0)
    assert rl.check("a", "/v1/chat", 0, 0, 3, 0)[0] is False
    assert rl.check("b", "/v1/chat", 0, 0, 3, 0)[0] is True
    assert rl.check("a", "/v1/complete", 0, 0, 3, 0)[0] is True


def test_concurrent_exact_limit():
    rl = RateLimiter("policy_a")
    results = []

    def hit():
        results.append(rl.check("c1", "/v1/chat", 10, 60000, 0, 0)[0])

    threads = [threading.Thread(target=hit) for _ in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(True) == 10
    s = rl.stats()
    assert s["total_allowed"] == 10 and s["total_rejected"] == 90 and s["active_keys"] == 1


def test_concurrent_token_bucket():
    rl = RateLimiter("policy_b")
    results = []

    def hit():
        results.append(rl.check("c1", "/v1/chat", 0, 0, 5, 0)[0])

    threads = [threading.Thread(target=hit) for _ in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(True) == 5
