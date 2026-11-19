// Package ledger is the write side: accounts, deposits and transfers in
// PostgreSQL, each publishing exactly one event.
//
// The point of the package is how the event leaves. In Outbox mode (the
// correct one) the event is inserted into ledger.outbox in the same
// transaction as the state change, so either both happen or neither does.
// The two "dual write" modes exist to show what goes wrong without it.
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/builtbyshailendra/event-ledger/events"
)

// Mode is how events leave the write side.
type Mode int

const (
	// Outbox: insert the event in the same transaction; a relay publishes it.
	Outbox Mode = iota
	// DualWrite: commit, then publish. A crash in between commits a change
	// that no one ever hears about.
	DualWrite
	// PublishFirst: publish, then commit. A crash in between (or a failed
	// commit) announces a change that never happened.
	PublishFirst
)

func (m Mode) String() string { return [...]string{"outbox", "dual-write", "publish-first"}[m] }

var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrNoAccount         = errors.New("no such account")
	ErrBadAmount         = errors.New("amount must be positive")
	// ErrCrashed is returned when a simulated crash stopped a command
	// midway (see Ledger.Crash).
	ErrCrashed = errors.New("simulated crash")
)

// Ledger runs commands against PostgreSQL.
type Ledger struct {
	pool *pgxpool.Pool
	Mode Mode
	// Publish sends an event directly; used only by the dual-write modes.
	Publish func(ctx context.Context, e events.Event) error
	// Crash, if set, is asked at each named point; returning true makes the
	// command stop right there, as if the process died. An open
	// transaction is rolled back, which is what PostgreSQL does when a
	// client's connection drops.
	Crash func(point, eventID string) bool
}

// New returns a ledger in Outbox mode.
func New(pool *pgxpool.Pool) *Ledger { return &Ledger{pool: pool} }

// Open creates an account. Opening it again is a no-op.
func (l *Ledger) Open(ctx context.Context, account string) error {
	e := events.Event{ID: "open:" + account, Type: events.AccountOpened, Account: account}
	return l.run(ctx, e, func(tx pgx.Tx) (map[string]int64, error) {
		var v int64
		err := tx.QueryRow(ctx, `INSERT INTO ledger.accounts (id, version) VALUES ($1, 1) RETURNING version`, account).Scan(&v)
		return map[string]int64{account: v}, err
	})
}

// Deposit adds money from outside. id is the client's idempotency key.
func (l *Ledger) Deposit(ctx context.Context, id, account string, amount int64) error {
	if amount <= 0 {
		return ErrBadAmount
	}
	e := events.Event{ID: id, Type: events.Deposited, Account: account, Amount: amount}
	return l.run(ctx, e, func(tx pgx.Tx) (map[string]int64, error) {
		var v int64
		err := tx.QueryRow(ctx, `UPDATE ledger.accounts SET balance = balance + $2, version = version + 1 WHERE id = $1 RETURNING version`, account, amount).Scan(&v)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoAccount
		}
		if err != nil {
			return nil, err
		}
		if err := entries(ctx, tx, id, map[string]int64{account: amount, events.World: -amount}); err != nil {
			return nil, err
		}
		return map[string]int64{account: v}, nil
	})
}

// Transfer moves money between accounts. id is the client's idempotency key.
func (l *Ledger) Transfer(ctx context.Context, id, from, to string, amount int64) error {
	if amount <= 0 {
		return ErrBadAmount
	}
	if from == to {
		return fmt.Errorf("transfer to the same account")
	}
	e := events.Event{ID: id, Type: events.Transferred, From: from, To: to, Amount: amount}
	return l.run(ctx, e, func(tx pgx.Tx) (map[string]int64, error) {
		// Lock both rows in a fixed order (by id), so two opposite transfers
		// can't deadlock waiting for each other.
		ids := []string{from, to}
		sort.Strings(ids)
		rows, err := tx.Query(ctx, `SELECT id, balance FROM ledger.accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE`, ids)
		if err != nil {
			return nil, err
		}
		bal := map[string]int64{}
		for rows.Next() {
			var id string
			var b int64
			if err := rows.Scan(&id, &b); err != nil {
				return nil, err
			}
			bal[id] = b
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(bal) != 2 {
			return nil, ErrNoAccount
		}
		if bal[from] < amount {
			return nil, ErrInsufficientFunds
		}
		versions := map[string]int64{}
		for acct, delta := range map[string]int64{from: -amount, to: amount} {
			var v int64
			if err := tx.QueryRow(ctx, `UPDATE ledger.accounts SET balance = balance + $2, version = version + 1 WHERE id = $1 RETURNING version`, acct, delta).Scan(&v); err != nil {
				return nil, err
			}
			versions[acct] = v
		}
		if err := entries(ctx, tx, id, map[string]int64{from: -amount, to: amount}); err != nil {
			return nil, err
		}
		return versions, nil
	})
}

func entries(ctx context.Context, tx pgx.Tx, command string, amounts map[string]int64) error {
	for acct, amt := range amounts {
		if _, err := tx.Exec(ctx, `INSERT INTO ledger.entries (command_id, account, amount) VALUES ($1, $2, $3)`, command, acct, amt); err != nil {
			return err
		}
	}
	return nil
}

func (l *Ledger) crash(point, id string) bool { return l.Crash != nil && l.Crash(point, id) }

// run executes one command: claim its idempotency key, change state, and
// get the event out according to the mode.
func (l *Ledger) run(ctx context.Context, e events.Event, change func(pgx.Tx) (map[string]int64, error)) error {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background()) // no-op after Commit

	// Idempotency: a retried command finds its key taken and does nothing.
	tag, err := tx.Exec(ctx, `INSERT INTO ledger.commands (id, kind) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`, e.ID, string(e.Type))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // already done
	}
	if e.Versions, err = change(tx); err != nil {
		return err
	}
	e.At = time.Now().UTC()
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}

	switch l.Mode {
	case Outbox:
		if _, err := tx.Exec(ctx, `INSERT INTO ledger.outbox (event_id, subject, payload) VALUES ($1, $2, $3)`, e.ID, e.Subject(), payload); err != nil {
			return err
		}
		// Wake the relay. NOTIFY is delivered only if this commits.
		if _, err := tx.Exec(ctx, `SELECT pg_notify('ledger_outbox', '')`); err != nil {
			return err
		}
		if l.crash("before-commit", e.ID) {
			return ErrCrashed
		}
		return tx.Commit(ctx)

	case DualWrite:
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		if l.crash("after-commit", e.ID) {
			return ErrCrashed // committed, never published
		}
		return l.Publish(ctx, e)

	case PublishFirst:
		if err := l.Publish(ctx, e); err != nil {
			return err
		}
		if l.crash("after-publish", e.ID) {
			return ErrCrashed // published, never committed
		}
		return tx.Commit(ctx)
	}
	return fmt.Errorf("unknown mode %d", l.Mode)
}
