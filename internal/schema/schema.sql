-- Write side: the source of truth. Every command changes state AND inserts
-- its event into the outbox in the same transaction.
CREATE SCHEMA IF NOT EXISTS ledger;

CREATE TABLE IF NOT EXISTS ledger.accounts (
    id        text PRIMARY KEY,
    balance   bigint NOT NULL DEFAULT 0 CHECK (balance >= 0),
    version   bigint NOT NULL DEFAULT 0,  -- +1 per event touching the account
    opened_at timestamptz NOT NULL DEFAULT now()
);

-- One row per accepted command, keyed by the client's idempotency key.
CREATE TABLE IF NOT EXISTS ledger.commands (
    id         text PRIMARY KEY,
    kind       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Double entry: every movement is a set of signed rows that sum to zero
-- (a deposit is balanced against the external "world" account).
CREATE TABLE IF NOT EXISTS ledger.entries (
    id         bigserial PRIMARY KEY,
    command_id text NOT NULL REFERENCES ledger.commands,
    account    text NOT NULL,
    amount     bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger.outbox (
    id           bigserial PRIMARY KEY,           -- the ledger's order
    event_id     text NOT NULL UNIQUE,            -- also the JetStream message ID
    subject      text NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON ledger.outbox (id) WHERE published_at IS NULL;

-- Read side: projections built from the stream by consumers. Each consumer
-- records the events it has applied in the same transaction as the change.
CREATE SCHEMA IF NOT EXISTS views;

CREATE TABLE IF NOT EXISTS views.processed (
    consumer     text NOT NULL,
    event_id     text NOT NULL,
    stream_seq   bigint NOT NULL,
    ledger_seq   bigint NOT NULL,
    occurred_at  timestamptz NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE IF NOT EXISTS views.balances (
    consumer text NOT NULL,
    account  text NOT NULL,
    balance  bigint NOT NULL DEFAULT 0,
    events   bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (consumer, account)
);

CREATE TABLE IF NOT EXISTS views.statement (
    consumer   text NOT NULL,
    account    text NOT NULL,
    event_id   text NOT NULL,
    ledger_seq bigint NOT NULL,   -- sort by this, not by arrival
    amount     bigint NOT NULL,
    PRIMARY KEY (consumer, account, event_id)
);
