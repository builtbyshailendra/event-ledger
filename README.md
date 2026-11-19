# event-ledger

A small double-entry ledger in PostgreSQL that publishes every change as an event to NATS JetStream, through a transactional outbox, to consumers that build read models. The point is the guarantees: no change without its event, no event without its change, and no event applied twice, through crashes at every step.

Built pairing with Claude Code, as a learning project. Runs on this laptop only: PostgreSQL 16 (local test cluster, port 5433) and an embedded NATS server.

## Build log

### Step 1: the write side, with an outbox

- `ledger.accounts` (balance can't go negative), `ledger.commands` (one row per accepted command, keyed by the client's idempotency key), `ledger.entries` (double entry: every command's rows sum to zero; deposits come from an `@world` account) and `ledger.outbox`.
- Every command (open, deposit, transfer) runs in one transaction: claim the idempotency key, change state, insert the event into the outbox, `pg_notify` the relay, commit. Either all of it happens or none of it.
- Transfers lock both accounts in id order (`SELECT ... ORDER BY id FOR UPDATE`) so opposite transfers can't deadlock.
- Each event carries every touched account's new version, so a consumer can tell if it missed or repeated one.
- Two wrong modes are kept on purpose for later comparison: **dual write** (commit, then publish) and **publish first** (publish, then commit).

Tests (against a fresh database on the local cluster each time): a refused transfer leaves neither state nor event; a command retried three times applies once; a crash before commit leaves nothing and the retry works; 8 workers × 400 random transfers among 20 accounts (2,743 accepted, 457 refused for funds) at 861 commands/s, with no deadlocks, money conserved, one event per accepted transfer, and every account's versions 1, 2, 3... without gaps.
