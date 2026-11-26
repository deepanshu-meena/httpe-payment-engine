import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from app.events.bus import InMemoryEventBus
from app.main import create_app


def open_account(client, name, balance, currency="INR"):
    r = client.post("/accounts", json={"owner_name": name, "initial_balance": balance, "currency": currency})
    assert r.status_code == 201, r.text
    return r.json()


def pay(client, key, a, b, amount="25.00", currency="INR"):
    return client.post("/payments", headers={"Idempotency-Key": key},
                       json={"debit_account_id": a["id"], "credit_account_id": b["id"],
                             "amount": amount, "currency": currency})


@pytest.fixture()
def client(registry):
    # embed_worker=False: tests that need settlement drive it explicitly
    with TestClient(create_app(registry=registry, bus=InMemoryEventBus(), embed_worker=False)) as c:
        yield c


def test_account_lifecycle(client):
    acc = open_account(client, "Asha", "100.50")
    assert acc["balance"] == "100.50"
    assert client.get(f"/accounts/{acc['id']}").json()["balance"] == "100.50"
    assert client.get("/accounts/nope").status_code == 404


def test_payment_and_idempotent_replay(client):
    a, b = open_account(client, "A", "100.00"), open_account(client, "B", "0.00")
    first = pay(client, "k1", a, b)
    assert first.status_code == 201 and first.json()["replayed"] is False
    again = pay(client, "k1", a, b)
    assert again.status_code == 200 and again.json()["replayed"] is True
    assert again.headers["Idempotent-Replay"] == "true"
    assert again.json()["id"] == first.json()["id"]
    assert client.get(f"/accounts/{a['id']}").json()["balance"] == "75.00"


def test_key_reuse_with_different_body_is_409(client):
    a, b = open_account(client, "A", "100.00"), open_account(client, "B", "0.00")
    pay(client, "k", a, b, "10.00")
    assert pay(client, "k", a, b, "20.00").status_code == 409


def test_validation_and_business_errors(client):
    a, b = open_account(client, "A", "10.00"), open_account(client, "B", "0.00")
    assert pay(client, "i1", a, b, "50.00").status_code == 422        # insufficient funds
    assert pay(client, "i2", a, b, "-5").status_code == 422           # non-positive
    assert pay(client, "i3", a, b, "1.001").status_code == 422        # sub-minor-unit precision
    assert pay(client, "i4", a, a).status_code == 422                 # self-payment
    assert pay(client, "i5", a, b, currency="USD").status_code == 422 # currency mismatch
    r = client.post("/payments", json={"debit_account_id": a["id"], "credit_account_id": b["id"], "amount": "1"})
    assert r.status_code == 422                                       # missing Idempotency-Key
    assert client.get("/payments/9-doesnotexist").status_code == 404
    assert client.get("/payments/garbage").status_code == 404


def test_health(client):
    assert client.get("/healthz").json() == {"status": "ok", "shards": 4}


def test_end_to_end_cross_shard_with_embedded_worker(registry):
    """Full path with no manual stepping: API -> outbox -> relay -> bus -> settlement -> COMPLETED."""
    app = create_app(registry=registry, bus=InMemoryEventBus(), embed_worker=True)
    with TestClient(app) as c:
        # find a pair that is genuinely cross-shard
        for _ in range(50):
            a, b = open_account(c, "A", "100.00"), open_account(c, "B", "0.00")
            if registry.shard_for_account(a["id"]) != registry.shard_for_account(b["id"]):
                break
        created = pay(c, "e2e", a, b, "40.00").json()
        assert created["state"] == "PROCESSING"

        deadline = time.time() + 10
        state = created["state"]
        while state != "COMPLETED" and time.time() < deadline:
            time.sleep(0.1)
            state = c.get(f"/payments/{created['id']}").json()["state"]
        assert state == "COMPLETED"
        assert c.get(f"/accounts/{a['id']}").json()["balance"] == "60.00"
        assert c.get(f"/accounts/{b['id']}").json()["balance"] == "40.00"
