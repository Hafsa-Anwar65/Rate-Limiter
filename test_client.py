"""
Smoke test against a running server.

Start the server first:
    STRATEGY=policy_a python3 server.py
Then run:
    python3 test_client.py
"""

import httpx

BASE_URL = "http://localhost:8080"


def main() -> None:
    with httpx.Client(base_url=BASE_URL, timeout=5) as client:
        print("Sending 7 requests as 'alice' to /v1/chat (limit is 5):")
        for i in range(1, 8):
            r = client.post("/v1/chat", headers={"X-Client-ID": "alice"})
            print(
                f"  #{i}: {r.status_code}  "
                f"remaining={r.headers.get('X-RateLimit-Remaining')}  "
                f"reset_ms={r.headers.get('X-RateLimit-Reset-Ms')}"
            )

        print("\n'bob' is isolated from 'alice':")
        r = client.post("/v1/chat", headers={"X-Client-ID": "bob"})
        print(f"  bob: {r.status_code}  remaining={r.headers.get('X-RateLimit-Remaining')}")

        print("\n'alice' on a different endpoint is isolated too:")
        r = client.post("/v1/complete", headers={"X-Client-ID": "alice"})
        print(f"  alice /v1/complete: {r.status_code}")

        print("\nStats:", client.get("/stats").json())


if __name__ == "__main__":
    main()
