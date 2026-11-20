# event-ledger

[![ci](https://github.com/builtbyshailendra/event-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/builtbyshailendra/event-ledger/actions/workflows/ci.yml)

A small double-entry ledger in PostgreSQL that publishes every change as an event to NATS JetStream, through a transactional outbox, to consumers that build read models. The point is the guarantees: no change without its event, no event without its change, and no event applied twice, through crashes at every step.

Built pairing with Claude Code, as a learning project. Runs on this laptop only: PostgreSQL 16 (local test cluster, port 5433) and an embedded NATS server (nats-server 2.15, nats.go 1.56, pgx 5.13, Go 1.27).

**What it shows:**
- **No lost or invented events.** With the transactional outbox, killing the relay at any of four points always ends with each committed event in the stream exactly once. The two dual-write designs lost 30 of 30 or invented 30 of 30 events under the same crashes.
- **No double-applied events.** Consumers record each event ID in the same transaction as the change and ack after commit. Killed at three points, they always converge; the two planted consumer bugs (ack first, no de-duplication) corrupt every account.
- **Chaos:** 78,891 events through 87 process kills, 30 database connection cuts and 35 NATS restarts, with the stream and the projection exact.
- **Two lessons with numbers:** JetStream's deduplication is a window (200 events stored twice once it passed), so consumer-side idempotency is the real guarantee. And a relay that tracks "the highest id published" silently loses a transaction that commits late.

Learning notes and explain-it questions: [LEARN.md](LEARN.md). Interview brief: [`../briefs/event-ledger.md`](../briefs/event-ledger.md).

```text
writers ──► PostgreSQL (one transaction) ─────────────────────────────┐
            ledger.accounts / commands / entries   ledger.outbox      │ pg_notify on commit
                                                       │              ▼
                                                     relay (advisory-lock leader, standby)
                                                       │ publish with Nats-Msg-Id = event id
                                                       ▼
                               NATS JetStream: LEDGER stream (kept forever, dedup window)
                                    │ durable pull consumer, explicit ack     │ max deliveries
                                    ▼                                          ▼
                     projector: views.processed + balances + statement   LEDGER_ADVISORIES
                     (one transaction per batch, ack after commit)            │
                                    │ poison / gave up                         ▼
                                    └──────────────────────────────► LEDGER_DLQ ──► replay
```

## Build log

### Step 1: the write side, with an outbox

- `ledger.accounts` (balance can't go negative), `ledger.commands` (one row per accepted command, keyed by the client's idempotency key), `ledger.entries` (double entry: every command's rows sum to zero; deposits come from an `@world` account) and `ledger.outbox`.
- Every command (open, deposit, transfer) runs in one transaction: claim the idempotency key, change state, insert the event into the outbox, `pg_notify` the relay, commit. Either all of it happens or none of it.
- Transfers lock both accounts in id order (`SELECT ... ORDER BY id FOR UPDATE`) so opposite transfers can't deadlock.
- Each event carries every touched account's new version, so a consumer can tell if it missed or repeated one.
- Two wrong modes are kept on purpose for later comparison: **dual write** (commit, then publish) and **publish first** (publish, then commit).

Tests (against a fresh database on the local cluster each time): a refused transfer leaves neither state nor event; a command retried three times applies once; a crash before commit leaves nothing and the retry works; 8 workers × 400 random transfers among 20 accounts (2,743 accepted, 457 refused for funds) at 861 commands/s, with no deadlocks, money conserved, one event per accepted transfer, and every account's versions 1, 2, 3... without gaps.

### Step 2: the relay, and what it takes to publish each event once

`relay/` reads unpublished outbox rows in id order, publishes them to the `LEDGER` stream with the event ID as JetStream's message ID (`Nats-Msg-Id`), waits for the stream's acknowledgments, then marks the acknowledged rows as published.

- **At-least-once by design:** publish, then mark. A crash in between means those rows go out again.
- **Deduplication** by message ID drops the resent copies, but only within the stream's window (2 minutes by default).
- **LISTEN/NOTIFY:** the write transaction calls `pg_notify`, delivered only on commit, so the relay wakes immediately; polling is the fallback.
- **One active relay:** a session-level advisory lock. If the active relay dies, its connection closes, PostgreSQL releases the lock and the standby takes over. The lock isn't a fence (a paused relay could still publish after losing it); deduplication makes that harmless.
- **Crash points** (`crash/`): `LEDGER_CRASH=relay.after-publish:2` makes the process exit with code 137 at the second time it reaches that point, with no cleanup, like `kill -9`. The tests run the relay as a separate process (`cmd/ledger`) and kill it this way.
- **The checker** (`check/`) reads the whole stream back and compares it with `ledger.commands`: missing (committed, never published), phantom (published, never committed), duplicates, and per-account version gaps.

Results, each on a fresh database and NATS server:

| Test | Result |
|---|---|
| 2,000 concurrent transfers with the relay running (LISTEN/NOTIFY) | 1,902 events, each once; commit-to-stream p50 8.8 ms, p99 61 ms |
| Same, polling every 500 ms instead | p50 289 ms, p99 534 ms |
| Relay killed before publishing (2nd batch of 200) | 200 in the stream; restart publishes the rest; 0 duplicates |
| Killed halfway through the batch | 300 in the stream; restart resends, stream drops 100 duplicates |
| Killed after publishing, before marking | 400 in the stream; stream drops 200 resent duplicates |
| Killed after marking | 400 in the stream; nothing resent |
| Two relays, the active one killed mid-stream | standby takes over; 1,419 events, each once |
| Dedup window 1 s, relay down 1.5 s after publishing | **200 events stored twice**: the window had passed |

In every crash case the final stream holds each committed event exactly once, until the deduplication window is exceeded. That last row is why consumers must deduplicate too (step 3).

**Two classic bugs, reproduced:**
- **A relay that remembers "the highest id I published"** (`WHERE id > last`) loses events. Ids are assigned at insert, not at commit. In the test, transaction `slow` inserts first (lower id) and waits; `fast` commits and is published; then `slow` commits, and the cursor relay never looks back: `slow` is missing forever. Scanning `WHERE published_at IS NULL` publishes it.
- **Dual writes.** 300 transfers, a simulated crash at the worst moment in every 10th:

  | Mode | Committed | In the stream | Result |
  |---|---|---|---|
  | commit, then publish | 320 | 290 | **30 changes no one heard about** |
  | publish, then commit | 290 | 320 | **30 events about changes that never happened** |
  | outbox | 290 | 290 | each once |

  A retry doesn't save dual-write mode: the command is already recorded, so the retry is a no-op and the event is still never sent.

### Step 3: idempotent consumers and projections

`projector/` is a JetStream durable pull consumer (explicit acks, `AckWait`, `MaxDeliver`) that maintains `views.balances` and `views.statement`. Delivery is at-least-once, from two sources: redelivery after a missed ack, and real duplicates in the stream once the relay's deduplication window has passed. So:

- Each event ID is inserted into `views.processed` **in the same transaction** as the change; if it's already there, the event is skipped.
- The ack goes out **after** the commit. A crash between the two causes a redelivery, which the processed table turns into a no-op.
- The statement is sorted by the ledger's sequence (the outbox id, carried in a `Ledger-Seq` header), not by arrival, and balances are sums, so the read model doesn't depend on delivery order.
- A batch is applied in one transaction. If it fails, its events are retried one at a time, so one event that keeps failing doesn't hold back the others.
- `check.Projection` compares the projection with the write side: every account's balance and number of events (the account version).

Results:

| Test | Result |
|---|---|
| 3,000 transfers with relay and projector running (100 events per transaction) | ~2,800 events projected as fast as they were written; event-to-projection p50 50 ms, p99 289 ms; backlog peaked at 149 |
| Same, one transaction per event | p50 3.2 s, p99 5.3 s; backlog peaked at 2,159 |
| Projector killed before commit (2nd batch of 50) | 50 applied; restart redelivers and applies the rest; matches the ledger |
| Killed after commit, before ack | 100 applied; restart gets the 50 unacked again and **skips all 50 as duplicates**; matches |
| Killed after ack | 100 applied; nothing redelivered; matches |
| Stream holding 200 real duplicates (relay down past a 1 s window) | projector applied 299, skipped exactly 200; matches |

Planted consumer bugs, each with the crash that exposes it:

| Projector | Crash | Accounts that differ from the ledger |
|---|---|---|
| correct | before commit | 0 |
| **acks before committing** | before commit | **10 of 10**: a batch of 50 was acked, never written, never redelivered |
| correct | after commit | 0 |
| **no de-duplication** | after commit | **10 of 10**: a redelivered batch of 50 applied twice |

**What broke:** the first projector did one transaction per event, about five statements each, and fell 2,159 messages behind (median 4 s). Batching 100 events per transaction helped only a little (1.5 s), because the statements still ran one round trip at a time. Pipelining them with `pgx.Batch` (claim all IDs, then one balance update per account with the batch's total, in account order so two projectors can't deadlock) brought the median to 50 ms. I guessed the remaining one-per-event cost was waiting for the WAL flush on each commit; with `synchronous_commit=off` its median only went from 3.2 s to 2.2 s, so the round trips are most of it.

### Step 4: dead letters, rebuilds, and chaos

**Dead letters.** Two kinds of message can't be processed:
- **Poison** (can never be valid: not JSON, unknown event type): copied to the `LEDGER_DLQ` stream with headers saying why and where from, then terminated (`TermWithReason`) so JetStream stops redelivering it. If the copy fails, it's negatively acknowledged instead, so it isn't lost.
- **Keeps failing** (a bug, or a dependency that stays down): negatively acknowledged with a growing delay. After `MaxDeliver` (5) deliveries JetStream gives up and publishes an advisory. Advisories are plain NATS messages, lost if nobody is listening, so a third stream (`LEDGER_ADVISORIES`) stores them, and the projector copies each such event to the dead-letter stream.
- `ReplayDeadLetters` applies them again once the cause is fixed; it's idempotent like everything else.

**Rebuilds.** Projections are disposable: a projector under a new name replays the stream from the start (the stream keeps everything; no age limit).

Results:

| Test | Result |
|---|---|
| A non-JSON message and an unknown event type in the stream | both dead-lettered on first delivery; the other 216 events projected; projection matches |
| One event fails every time | dead-lettered after 5 deliveries (3.7 s); 215 other events applied meanwhile; only its 2 accounts differ; after the fix, replay applies it and a second replay applies nothing; projection matches |
| Rebuild 9,480 events into a new projection | 3.1 s (about 3,100 events/s); identical to the ledger and to the live copy, statement row for statement row |
| Delete the original read model and its consumer, rebuild in place | matches the ledger |

**Chaos** (`TestChaos`): two relays and two projectors (competing for one durable consumer) run as supervised processes, restarted whenever they exit, while 8 writers make transfers. Every 200–700 ms the test does one of: kill a random process; cut every `ledger-*` database connection with `pg_terminate_backend`; restart the NATS server (down 0.2–0.8 s). Then everything must catch up, and the checks must be exact: each committed event in the stream once, projection equal to the ledger, money conserved, one processed row per event.

`go test ./e2e -run TestChaos -args -chaos-seeds 3 -chaos-duration 30s`:

| Seed | Events | Process kills | Connection cuts | NATS restarts | Relay became active | Duplicates dropped by the stream | Redeliveries skipped by projectors | Result |
|---|---|---|---|---|---|---|---|---|
| 100 | 27,169 | 26 | 12 | 10 | 21 | 439 | 144 | exact |
| 101 | 24,427 | 29 | 9 | 14 | 17 | 40 | 152 | exact |
| 102 | 27,295 | 32 | 9 | 11 | 19 | 236 | 162 | exact |

That's 78,891 events through 87 kills, 30 connection cuts and 35 NATS restarts, with 715 duplicates dropped and 458 redeliveries skipped along the way, and nothing lost or applied twice.

**What broke:** the first chaos run reported 0 takeovers, 0 duplicates and 0 skipped redeliveries, which can't be right with 15 kills. The test was wrong, not the system: the supervisor replaced each process's log buffer after the process had started writing to the original. Fixed; the counts above are real.

## Run it

```bash
go run ./cmd/ledger demo -transfers 5000    # everything in one process, then the checks
go test ./...                               # about 3 minutes; needs PostgreSQL on 127.0.0.1:5433
go test ./e2e -run TestChaos -args -chaos-seeds 3 -chaos-duration 30s
```

The demo leaves the `event_ledger_demo` database on port 5433 for inspection. Example output:

```
4535 accepted, 465 refused for funds; written in 4.983s, projected by 5.205s
commit -> stream:      p50   7.3 ms, p99  44.5 ms
event  -> projection:  p50  58.5 ms, p99 197.9 ms
stream check:     committed 4575, messages 4575, missing 0 [], phantom 0 [], duplicates 0, unpublished 0, version gaps []
projection check: 0 accounts differ from the ledger
```

## What it doesn't do

- **One machine.** One PostgreSQL, one NATS server (not a 3-node JetStream cluster), all on localhost. Network partitions between the pieces weren't tested; restarts and dropped connections were.
- **Crashes are process kills.** The operating system and disks never failed; PostgreSQL and NATS were never killed mid-write (NATS was shut down and restarted).
- **Order is per ledger sequence, not per arrival.** After a failed publish, a later event can reach the stream before an earlier one; the projections don't care (sums, and statements sorted by the ledger sequence). A consumer that needs strict per-account order would have to check the version numbers each event carries.
- **The outbox is never cleaned up.** Real systems delete or partition old published rows.
- **The stream keeps everything** so projections can be rebuilt. At scale that needs snapshots or compaction.
- **The relay's advisory lock isn't a fence.** A paused relay can publish after losing it; deduplication makes that harmless here.
- CI (badge above) was added when the project was published, 20 Nov 2026, after the build.
