# Walkthrough: read it, then rebuild it yourself

Goal: you should be able to explain *why* every piece exists and rebuild it from a blank folder. Don't copy — close the repo and re-derive each milestone, then diff.

## Part 1 — Reading order (≈ 2–3 hours)

Read in this order; each file builds on the previous.

| # | File | Ask yourself |
|---|---|---|
| 1 | `app/money.py` | Why integers, not floats? What breaks with `0.1 + 0.2`? |
| 2 | `app/db/models.py` | Which constraint makes idempotency atomic? Which prevents overdraft? Why is `Transaction.id` shard-prefixed? |
| 3 | `app/db/shard_router.py` | Why is routing a pure function? What happens if I change `N`? |
| 4 | `app/core/concurrency.py` | Why one `UPDATE ... WHERE balance >= amt` instead of read-then-write? |
| 5 | `app/core/idempotency.py` | Why insert the row *first*? What does the fingerprint protect against? |
| 6 | `app/core/state_machine.py` | Why a conditional UPDATE instead of load-check-set? |
| 7 | `app/core/outbox.py` + `OutboxEvent` model | What bug does the outbox prevent? |
| 8 | `app/services/payments.py` | Where exactly are the two paths (same-shard vs saga) chosen? Why lock ordering? |
| 9 | `app/services/settlement.py` | Trace a duplicate event. Trace a missing destination account. |
| 10 | `app/services/outbox_relay.py`, `core/circuit_breaker.py` | What happens to events while the breaker is open? |
| 11 | `app/events/bus.py` | Where is the offset committed, and why *after* the handler? |
| 12 | `app/api/routes.py`, `app/main.py` | Which status codes map to which failures? |

Then run `python -m scripts.bench` and read the audit line — that's the whole thesis in one print statement.

## Part 2 — Rebuild milestones

Do each milestone with tests *first*, run them, then move on. Each ends with a sentence you should be able to say out loud in an interview.

1. **Single DB, correct money.** Accounts + atomic debit/credit. Test: 30 threads debit a 100.00 account by 10.00 → exactly 10 succeed.
   *Say:* "I use an atomic conditional UPDATE so the check and the change can't be separated."
   *Prove your test is real:* temporarily write a naive read-modify-write and watch the test fail.
2. **Idempotency.** `UNIQUE(idempotency_key)`, insert first, return the winner on conflict. Test: 12 threads, same key → one execution.
   *Say:* "Check-then-insert is a race; the unique index is the only atomic claim."
3. **State machine.** `PROCESSING → COMPLETED/FAILED`, conditional UPDATE. Test: second transition returns `False`.
4. **Sharding.** Router + per-shard sessions; same-shard payments in one transaction. Test: routing is deterministic and even.
5. **Outbox.** Write the event in the same transaction; a relay publishes + marks. Test: kill the relay mid-way → no event lost, maybe duplicated.
6. **Saga.** Cross-shard debit + event, then settlement credit with the `processed_events` marker. Tests: redelivery credits once; missing destination refunds exactly once.
   *Say:* "I trade atomicity across shards for availability, and I make the intermediate state explicit instead of hiding it."
7. **Event bus + circuit breaker.** In-memory first; Kafka adapter last. Test the breaker with a fake clock.
8. **API + invariant test.** Concurrent mixed load, duplicate keys, then assert Σ money unchanged.
9. **Postgres.** Run the same tests on real Postgres. Then try opposing transfers A↔B — *watch* the deadlock appear, then add lock ordering.

## Part 3 — Questions you should be able to answer

* What exactly can go wrong between the debit and the credit in a cross-shard transfer? What state is the system in? How is it repaired?
* Why can't you just use one transaction across two shards? What would two-phase commit cost you?
* My consumer crashes after crediting but before marking the transaction `COMPLETED`. What happens on redelivery?
* Two workers receive the same event simultaneously. Walk through it.
* Why is the outbox written in the same transaction as the debit? What breaks if it isn't?
* Where is exactly-once actually achieved here, and where is it only at-least-once + idempotency?
* The client times out and retries with the same key but a different amount. What happens, and why?
* How would you add a 5th shard? What do you have to migrate?
* One merchant account receives 80% of traffic. What happens to throughput? What would you do?
* Why does `SELECT ... FOR UPDATE` not work as a basis for the test suite on SQLite, and how does the design avoid depending on it?
* What would you measure before claiming "X transactions per second"? What does your benchmark *not* tell you?
* Which guarantees did you test against real Postgres, and which only on SQLite?
