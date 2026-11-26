"""HTTP load test:   locust -f loadtest/locustfile.py --host http://localhost:8000

Creates a pool of funded accounts on start, then fires payments with fresh
idempotency keys. ~10% of requests deliberately replay a previous key to
exercise the idempotent-replay path under load.
"""
import random
import uuid

from locust import HttpUser, between, events, task

POOL: list[str] = []


@events.test_start.add_listener
def seed_accounts(environment, **_):
    import requests  # locust depends on requests

    for i in range(100):
        r = requests.post(f"{environment.host}/accounts",
                          json={"owner_name": f"load-{i}", "initial_balance": "1000000.00"})
        POOL.append(r.json()["id"])


class PayerUser(HttpUser):
    wait_time = between(0.0, 0.05)
    last_key = None
    last_body = None

    @task(9)
    def new_payment(self):
        a, b = random.sample(POOL, 2)
        self.last_key = str(uuid.uuid4())
        self.last_body = {"debit_account_id": a, "credit_account_id": b, "amount": "1.00", "currency": "INR"}
        self.client.post("/payments", json=self.last_body, headers={"Idempotency-Key": self.last_key})

    @task(1)
    def replay_payment(self):
        if self.last_key:
            self.client.post("/payments", json=self.last_body, headers={"Idempotency-Key": self.last_key},
                             name="/payments (replay)")
