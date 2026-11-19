# Learning notes: event-ledger

## 1. The problem: two systems, one change

A service changes its database and must tell other services (search, notifications, analytics, a read model). That's two writes to two systems, and there is no transaction that spans PostgreSQL and a message broker.

| Design | What a crash in the middle does | Measured here (30 crashes) |
|---|---|---|
| commit, then publish | the change happened, nobody hears about it | 30 events missing |
| publish, then commit | everyone hears about a change that never happened | 30 phantom events |
| **transactional outbox** | the event is a row in the same transaction; a relay publishes it later | 0 missing, 0 phantom |

Retrying doesn't rescue dual writes: in commit-then-publish, the command is already recorded, so a retry is a no-op and the event still never goes out.

## 2. The outbox pattern

1. In the business transaction, insert the event into `ledger.outbox`. Atomic with the change: both or neither.
2. A **relay** reads unpublished rows, publishes them, and marks them published.
3. Publish-then-mark means a crash in between publishes again: **at-least-once**. The broker or the consumer must remove duplicates.

Details that matter (all in `relay/`):
- **Find work by `published_at IS NULL`, not by "id > last".** Ids come from a sequence at insert time, but transactions commit in a different order. A cursor skips a transaction that inserted early and committed late; `TestCursorRelaySkipsALateCommit` shows the lost event.
- **Wake on commit with LISTEN/NOTIFY.** `pg_notify` inside the transaction is delivered only if it commits. Measured: median commit-to-stream 8.8 ms with NOTIFY, 289 ms polling every 500 ms.
- **One active relay:** `pg_try_advisory_lock` on a session. If the relay dies, its connection closes and the lock is released, so a standby takes over. It's not a fence: a relay that stalls can still publish after losing the lock. Deduplication makes that harmless.
- **Alternatives:** Debezium or logical replication reads the write-ahead log instead of polling a table (no relay queries, more moving parts).

## 3. JetStream in five minutes

- A **stream** stores messages on subjects (`ledger.events.>`), with a sequence number each. Retention here: keep everything, so projections can be rebuilt.
- A **durable consumer** remembers its position. **Pull** consumers fetch batches; with **explicit ack**, a message not acknowledged within `AckWait` is delivered again, up to `MaxDeliver` times.
- `Ack` (done), `Nak` / `NakWithDelay` (try again later), `Term` (never redeliver), `InProgress` (extend AckWait).
- **Deduplication:** a message with a `Nats-Msg-Id` already seen **within the stream's duplicate window** is dropped. The window is time-limited: with a 1 s window and the relay down 1.5 s, 200 events were stored twice.
- **Advisories:** JetStream announces things like "max deliveries reached" as plain NATS messages on `$JS.EVENT.ADVISORY...`. Nobody listening means it's gone, so this project stores them in a stream.

## 4. Exactly-once processing is idempotent processing

Delivery is at-least-once (redelivery after a missed ack, duplicates past the dedup window). "Exactly-once" is an effect you build at the consumer:

1. In one database transaction: insert the event ID into `views.processed` (primary key) and apply the change. If the ID is already there, skip the change.
2. Commit, **then** ack.

| Crash point | What happens on restart |
|---|---|
| before commit | nothing was written; redelivery applies it |
| after commit, before ack | redelivery finds the ID; skipped (50 skipped in the test) |
| after ack | nothing redelivered |

The two classic mistakes, both planted and caught: **ack before commit** (a crash loses the event: 10 of 10 accounts wrong) and **no de-duplication** (a redelivery applies twice: 10 of 10 wrong).

This only works because the processed table and the read model are in the **same database**. If a consumer's side effect is elsewhere (sending an email, calling an API), it needs an idempotency key on that side instead.

## 5. Ordering

- The stream is ordered by arrival. The relay publishes in outbox id order, but after a failed publish a later event can land first.
- Per-account order is preserved in the ledger: a transfer locks its accounts, so a later transaction on the same account inserts its outbox row (and gets its id) after the earlier one committed.
- The projections don't depend on arrival order: balances are sums, statements are sorted by the ledger sequence (the outbox id, sent in a `Ledger-Seq` header).
- Each event carries every touched account's new **version**. The checker uses them to find gaps (a missing event) per account.

## 6. Poison messages and dead letters

- **Can never succeed** (bad JSON, unknown type): copy to the dead-letter stream with the reason, then `Term`. Retrying can't help, so don't.
- **Fails repeatedly** (bug, dependency down): `NakWithDelay` with a growing delay; after `MaxDeliver` JetStream stops and sends an advisory; the stored advisory triggers the copy to the dead-letter stream.
- **Don't let one bad event block others:** a batch that fails is retried one event at a time, so only the bad one is held back (215 others applied while one failed).
- **Replay** after the fix goes through the same idempotent path, so replaying twice is safe.

## 7. Projections are disposable

A read model is a function of the event history. Replay the stream into a new projection (new consumer name, from the start) and you get the same answer: 9,480 events in 3.1 s here, identical row for row. That's how you fix a projection bug, add a new read model, or recover a lost one. The price: the stream must keep the history (or you need snapshots).

This is **not** full event sourcing: the write side stores current state (accounts) plus the outbox, not only events. In event sourcing the events table *is* the source of truth and state is derived from it.

## 8. Performance lessons

- A per-event transaction with five sequential statements fell 2,159 messages behind (median 3–4 s). Batching transactions alone barely helped. **Pipelining** statements (`pgx.Batch`: claim all IDs in one round trip, then one balance update per account) brought the median to 50 ms.
- I guessed the per-event cost was the WAL flush at each commit; `synchronous_commit=off` only took the median from 3.2 s to 2.2 s. Most of it was round trips. Measure before you explain.
- Updating accounts in sorted order inside a batch prevents deadlocks between two projectors.

## 9. What broke while building

1. The first dual-write comparison counted 20 extra missing events: no relay ran in those modes, so account setup (which uses the outbox) was never published. The comparison has to differ in one thing only.
2. The projector was too slow until statements were pipelined.
3. My guess about why (fsync) was mostly wrong; measuring said round trips.
4. The first chaos run reported zero takeovers and zero duplicates, which couldn't be true: the supervisor swapped log buffers after each process started. The system was fine; the evidence wasn't.

## 10. Explain it without notes

1. Why can't you just commit and then publish? What about publish and then commit?
2. Walk through the outbox: what's in the transaction, what the relay does, where duplicates come from.
3. Why does `WHERE id > last_published` lose events? What does `published_at IS NULL` cost instead?
4. What does JetStream's `Nats-Msg-Id` deduplication guarantee, and what doesn't it?
5. How does the consumer make processing exactly-once? Why must the ack come after the commit?
6. What happens at each crash point in the consumer?
7. How does the standby relay take over, and why isn't the lock enough on its own?
8. Poison message vs a message that keeps failing: how is each handled, and why differently?
9. How do you rebuild a projection, and what does that require of the stream?
10. Is the stream in order? Does it matter here, and how would you get strict per-account order?
11. Why was the per-event projector slow, and what fixed it?
12. What would change with Kafka instead of JetStream?

## 11. Your turn

1. **Clean up the outbox:** delete published rows older than a day, or partition the table by day and drop old partitions (see sql-performance-lab). Keep every test passing.
2. **Strict per-account order:** make the projector reject (and retry later) an event whose version isn't the account's last version + 1. What happens to throughput?
3. **A side effect outside the database:** a consumer that "sends an email" per transfer to a fake HTTP service. Make it exactly-once with an idempotency key on the receiving side.
4. **A JetStream cluster:** run three embedded servers with replicas: 3, kill one during the chaos test, and see what changes.
5. **Change data capture:** replace the relay with logical replication (`pgoutput`) and compare latency and failure modes.
