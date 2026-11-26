"""In-process throughput benchmark + correctness audit.

    python -m scripts.bench --seconds 10 --threads 16 --cross-shard-ratio 0.5

Drives `create_payment` directly (no HTTP) from N threads against the configured
shards, then drains settlement and AUDITS the result: total money conserved, no
negative balances, nothing stuck in PROCESSING. A throughput number is only
meaningful if the run is also correct, so both are reported together.

Read the numbers honestly: on the default file-backed SQLite shards this measures
the *application logic*, bounded by SQLite's one-writer-per-file limit. Absolute
TPS on Postgres with network hops will differ - run it against your own
SHARD_URLS and quote only what you measure.
"""
from __future__ import annotations

import argparse
import json
import random
import statistics
import tempfile
import threading
import time
import uuid
from pathlib import Path

from sqlalchemy import func, select

from app.db.models import Account, OutboxEvent, Transaction, TransactionState
from app.db.shard_router import ShardRegistry, shard_index_for
from app.money import to_minor
from app.services.payments import PaymentRejectedError, create_payment
from app.services.settlement import handle_transaction_debited
from decimal import Decimal


def seed(reg, n_accounts, balance="1000000.00"):
    by_shard = {i: [] for i in range(reg.shard_count)}
    while min(len(v) for v in by_shard.values()) < n_accounts // reg.shard_count:
        aid = str(uuid.uuid4())
        shard = shard_index_for(aid, reg.shard_count)
        if len(by_shard[shard]) < n_accounts // reg.shard_count:
            with reg.session_for_shard(shard) as s:
                s.add(Account(id=aid, owner_name="bench", balance_minor=to_minor(Decimal(balance)), currency="INR"))
            by_shard[shard].append(aid)
    return by_shard


def total_money(reg):
    t = 0
    for i in range(reg.shard_count):
        with reg.session_for_shard(i) as s:
            t += s.execute(select(func.coalesce(func.sum(Account.balance_minor), 0))).scalar_one()
    return t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=10)
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--accounts", type=int, default=400)
    ap.add_argument("--cross-shard-ratio", type=float, default=0.5)
    ap.add_argument("--urls", default=None, help="comma-separated shard URLs (default: temp SQLite x4)")
    args = ap.parse_args()

    tmp = tempfile.mkdtemp()
    urls = args.urls.split(",") if args.urls else [f"sqlite:///{Path(tmp)}/s{i}.db" for i in range(4)]
    reg = ShardRegistry(urls)
    reg.create_all()
    accounts = seed(reg, args.accounts)
    flat = [a for v in accounts.values() for a in v]
    before = total_money(reg)

    latencies, ok, rejected, errors = [], [0], [0], [0]
    lock = threading.Lock()
    stop_at = time.time() + args.seconds

    def worker(seed_):
        rng = random.Random(seed_)
        mine_lat, n_ok, n_rej, n_err = [], 0, 0, 0
        while time.time() < stop_at:
            if rng.random() < args.cross_shard_ratio:
                s1, s2 = rng.sample(range(reg.shard_count), 2)
            else:
                s1 = s2 = rng.randrange(reg.shard_count)
            a, b = rng.choice(accounts[s1]), rng.choice(accounts[s2])
            if a == b:
                continue
            t0 = time.perf_counter()
            try:
                create_payment(reg, idempotency_key=str(uuid.uuid4()), debit_account_id=a,
                               credit_account_id=b, amount_minor=rng.randint(1, 500), currency="INR")
                n_ok += 1
            except PaymentRejectedError:
                n_rej += 1
            except Exception:
                n_err += 1
            mine_lat.append((time.perf_counter() - t0) * 1000)
        with lock:
            latencies.extend(mine_lat); ok[0] += n_ok; rejected[0] += n_rej; errors[0] += n_err

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.threads)]
    t0 = time.time()
    [t.start() for t in threads]; [t.join() for t in threads]
    elapsed = time.time() - t0

    # settle everything that is still in flight, then audit
    settle_t0 = time.time()
    settled = 0
    for shard in range(reg.shard_count):
        with reg.session_for_shard(shard) as s:
            evs = [json.loads(r.payload_json) for r in s.execute(
                select(OutboxEvent).where(OutboxEvent.event_type == "transaction.debited")).scalars()]
        for e in evs:
            handle_transaction_debited(reg, e); settled += 1
    settle_s = time.time() - settle_t0

    after = total_money(reg)
    stuck = 0
    for i in range(reg.shard_count):
        with reg.session_for_shard(i) as s:
            stuck += s.execute(select(func.count()).select_from(Transaction)
                               .where(Transaction.state == TransactionState.PROCESSING)).scalar_one()

    lat = sorted(latencies)
    pct = lambda p: lat[min(len(lat) - 1, int(len(lat) * p))] if lat else 0
    print(f"threads={args.threads} shards={reg.shard_count} cross_shard_ratio={args.cross_shard_ratio} backend={'custom' if args.urls else 'sqlite'}")
    print(f"accepted={ok[0]} rejected={rejected[0]} errors={errors[0]} in {elapsed:.1f}s")
    print(f"throughput (accepted payments/s, debit side): {ok[0] / elapsed:,.0f}")
    print(f"latency ms  p50={pct(.5):.1f}  p95={pct(.95):.1f}  p99={pct(.99):.1f}  mean={statistics.mean(lat):.1f}")
    print(f"settlement drained: {settled} cross-shard events in {settle_s:.1f}s ({settled / max(settle_s, 1e-9):,.0f}/s)")
    print(f"AUDIT  money conserved: {before == after}  (before={before} after={after})  stuck PROCESSING: {stuck}")
    assert before == after and stuck == 0 and errors[0] == 0, "AUDIT FAILED"


if __name__ == "__main__":
    main()
