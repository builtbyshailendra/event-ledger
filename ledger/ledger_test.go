package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/builtbyshailendra/event-ledger/events"
	"github.com/builtbyshailendra/event-ledger/internal/testenv"
)

var ctx = context.Background()

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func balance(t *testing.T, pool *pgxpool.Pool, acct string) int64 {
	return count(t, pool, `SELECT balance FROM ledger.accounts WHERE id = $1`, acct)
}

func TestStateAndEventCommitTogether(t *testing.T) {
	pool, _ := testenv.DB(t)
	l := New(pool)
	for _, a := range []string{"alice", "bob"} {
		if err := l.Open(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Deposit(ctx, "d1", "alice", 100); err != nil {
		t.Fatal(err)
	}
	if err := l.Transfer(ctx, "t1", "alice", "bob", 30); err != nil {
		t.Fatal(err)
	}
	// Refused: the state change rolls back and so does its event.
	if err := l.Transfer(ctx, "t2", "bob", "alice", 31); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("got %v", err)
	}
	if balance(t, pool, "alice") != 70 || balance(t, pool, "bob") != 30 {
		t.Fatal("wrong balances")
	}
	if n := count(t, pool, `SELECT count(*) FROM ledger.outbox`); n != 4 { // 2 opens, deposit, transfer
		t.Fatalf("outbox has %d events, want 4", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM ledger.outbox WHERE event_id = 't2'`); n != 0 {
		t.Fatal("a refused transfer left an event")
	}
	// Double entry: every command's entries sum to zero.
	if n := count(t, pool, `SELECT count(*) FROM (SELECT command_id FROM ledger.entries GROUP BY command_id HAVING sum(amount) <> 0) x`); n != 0 {
		t.Fatalf("%d unbalanced commands", n)
	}
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM ledger.outbox WHERE event_id = 't1'`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var e events.Event
	_ = json.Unmarshal(payload, &e)
	if e.From != "alice" || e.To != "bob" || e.Amount != 30 || e.Versions["alice"] != 3 || e.Versions["bob"] != 2 {
		t.Fatalf("event %+v", e)
	}
}

func TestRetriedCommandIsANoOp(t *testing.T) {
	pool, _ := testenv.DB(t)
	l := New(pool)
	_ = l.Open(ctx, "alice")
	for range 3 {
		if err := l.Deposit(ctx, "d1", "alice", 100); err != nil {
			t.Fatal(err)
		}
	}
	if b := balance(t, pool, "alice"); b != 100 {
		t.Fatalf("balance %d after 3 retries of one deposit", b)
	}
	if n := count(t, pool, `SELECT count(*) FROM ledger.outbox WHERE event_id = 'd1'`); n != 1 {
		t.Fatalf("%d events", n)
	}
}

func TestCrashBeforeCommitLeavesNothing(t *testing.T) {
	pool, _ := testenv.DB(t)
	l := New(pool)
	_ = l.Open(ctx, "alice")
	l.Crash = func(point, id string) bool { return id == "d1" && point == "before-commit" }
	if err := l.Deposit(ctx, "d1", "alice", 100); !errors.Is(err, ErrCrashed) {
		t.Fatal(err)
	}
	if balance(t, pool, "alice") != 0 || count(t, pool, `SELECT count(*) FROM ledger.outbox WHERE event_id = 'd1'`) != 0 ||
		count(t, pool, `SELECT count(*) FROM ledger.commands WHERE id = 'd1'`) != 0 {
		t.Fatal("a crash before commit left state behind")
	}
	l.Crash = nil
	if err := l.Deposit(ctx, "d1", "alice", 100); err != nil { // the client retries
		t.Fatal(err)
	}
	if balance(t, pool, "alice") != 100 {
		t.Fatal("retry after crash didn't apply")
	}
}

func TestConcurrentTransfersConserveMoney(t *testing.T) {
	pool, _ := testenv.DB(t)
	l := New(pool)
	const accounts, workers, perWorker = 20, 8, 400
	for i := range accounts {
		a := fmt.Sprintf("acct-%02d", i)
		_ = l.Open(ctx, a)
		if err := l.Deposit(ctx, "seed-"+a, a, 1000); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, refused := 0, 0
	errs := map[string]int{}
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for i := range perWorker {
				from, to := rng.IntN(accounts), rng.IntN(accounts)
				if from == to {
					to = (to + 1) % accounts
				}
				err := l.Transfer(ctx, fmt.Sprintf("w%d-%d", w, i), fmt.Sprintf("acct-%02d", from), fmt.Sprintf("acct-%02d", to), int64(1+rng.IntN(300)))
				mu.Lock()
				switch {
				case err == nil:
					accepted++
				case errors.Is(err, ErrInsufficientFunds):
					refused++
				default:
					errs[err.Error()]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if len(errs) > 0 {
		t.Fatalf("unexpected errors (deadlocks?): %v", errs)
	}
	if sum := count(t, pool, `SELECT sum(balance) FROM ledger.accounts`); sum != accounts*1000 {
		t.Fatalf("money not conserved: %d", sum)
	}
	if n := count(t, pool, `SELECT count(*) FROM ledger.outbox WHERE subject = 'ledger.events.transferred'`); n != int64(accepted) {
		t.Fatalf("%d transfer events for %d accepted transfers", n, accepted)
	}
	// Each account's events carry versions 1, 2, 3... with no gaps or repeats.
	gaps := count(t, pool, `
		WITH v AS (SELECT key AS acct, value::bigint AS ver FROM ledger.outbox, jsonb_each_text(payload->'versions'))
		SELECT count(*) FROM (SELECT acct FROM v GROUP BY acct HAVING count(*) <> max(ver) OR count(DISTINCT ver) <> count(*)) x`)
	if gaps != 0 {
		t.Fatalf("%d accounts with version gaps", gaps)
	}
	t.Logf("%d transfers (%d accepted, %d refused for funds) by %d workers in %v: %.0f commands/s",
		workers*perWorker, accepted, refused, workers, elapsed.Round(time.Millisecond), float64(workers*perWorker)/elapsed.Seconds())
}
