package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/builtbyshailendra/event-ledger/bus"
	"github.com/builtbyshailendra/event-ledger/check"
	"github.com/builtbyshailendra/event-ledger/events"
	"github.com/builtbyshailendra/event-ledger/projector"
)

func (e *env) dlq() []check.Msg {
	e.t.Helper()
	msgs, err := check.ReadStream(ctx, e.js, bus.DLQStream)
	if err != nil {
		e.t.Fatal(err)
	}
	return msgs
}

// Two messages no consumer could ever process: not JSON, and an event type
// it doesn't know. Each goes to the dead-letter stream on its first
// delivery, and everything after it is still projected.
func TestPoisonMessagesGoToDeadLetters(t *testing.T) {
	e := newEnv(t, 0)
	e.runRelay()
	pr := e.runProjector("balances", 0)
	accts := e.seed(10)
	e.transfers(e.l, accts, 100, 4, 21)
	for i, payload := range []string{`"not an event"`, `{"id":"refund-1","type":"refund","amount":5}`} {
		if _, err := e.pool.Exec(ctx, `INSERT INTO ledger.outbox (event_id, subject, payload) VALUES ($1, 'ledger.events.unknown', $2)`,
			fmt.Sprintf("poison-%d", i), payload); err != nil {
			t.Fatal(err)
		}
	}
	e.transfers(e.l, accts, 100, 4, 22) // events after the poison
	e.caughtUp("balances", 30*time.Second)
	e.waitFor("two dead letters", 10*time.Second, func() bool { return pr.DeadLettered.Load() == 2 })

	if d := e.projectionDiffs("balances"); len(d) > 0 {
		t.Fatalf("projection differs: %s", check.Summary(d))
	}
	if n := len(e.dlq()); n != 2 {
		t.Fatalf("%d dead letters, want 2", n)
	}
	t.Logf("2 poison messages dead-lettered on first delivery; the other %d events projected", pr.Applied.Load())
}

// One event fails every time it is applied (a stand-in for a bug or a
// dependency that stays down). After MaxDeliver attempts JetStream gives up
// on it and the projector copies it to the dead-letter stream; the events
// after it aren't held back. Once fixed, replaying the dead letters
// completes the projection, and a second replay changes nothing.
func TestFailingEventIsDeadLetteredThenReplayed(t *testing.T) {
	e := newEnv(t, 0)
	e.runRelay()
	accts := e.seed(10)

	var broken atomic.Bool
	broken.Store(true)
	const bad = "tx-23-50"
	pr := &projector.Projector{Pool: e.pool, JS: e.js, Name: "balances", AckWait: time.Second, MaxDeliver: 5,
		Fail: func(ev events.Event) error {
			if ev.ID == bad && broken.Load() {
				return errors.New("simulated failure")
			}
			return nil
		}}
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = pr.Run(pctx) }()

	start := time.Now()
	e.transfers(e.l, accts, 200, 4, 23)
	e.waitFor("the failing event to be dead-lettered", 60*time.Second, func() bool { return pr.DeadLettered.Load() == 1 })
	e.caughtUp("balances", 30*time.Second)
	gaveUp := time.Since(start)

	d := e.projectionDiffs("balances")
	if len(d) != 2 { // the two accounts of the failed transfer
		t.Fatalf("expected exactly the 2 accounts of %s to differ, got %v", bad, d)
	}
	dl := e.dlq()
	if len(dl) != 1 || dl[0].Event.ID != bad {
		t.Fatalf("dead letters: %+v", dl)
	}
	t.Logf("%s failed 5 times and was dead-lettered after %v; meanwhile %d other events were applied; differing: %v",
		bad, gaveUp.Round(100*time.Millisecond), pr.Applied.Load(), d)

	broken.Store(false) // the fix is deployed
	for i, want := range []int{1, 0} {
		n, failed, err := pr.ReplayDeadLetters(ctx)
		if err != nil || len(failed) > 0 || n != want {
			t.Fatalf("replay %d: applied %d (want %d), failed %v, err %v", i+1, n, want, failed, err)
		}
	}
	if d := e.projectionDiffs("balances"); len(d) > 0 {
		t.Fatalf("after replay: %s", check.Summary(d))
	}
}

// Read models are disposable: replaying the stream from the start rebuilds
// them. First a second copy under a new name (a blue/green rebuild, while
// the first keeps serving), then the original deleted and rebuilt in place.
func TestRebuildProjectionFromTheStream(t *testing.T) {
	e := newEnv(t, 0)
	e.runRelay()
	e.runProjector("balances", 0)
	accts := e.seed(50)
	e.transfers(e.l, accts, 10_000, 8, 24)
	e.caughtUp("balances", 120*time.Second)
	total := e.count(`SELECT count(*) FROM ledger.commands`)

	start := time.Now()
	e.runProjector("balances_v2", 0)
	e.caughtUp("balances_v2", 120*time.Second)
	took := time.Since(start)
	if d := e.projectionDiffs("balances_v2"); len(d) > 0 {
		t.Fatalf("rebuilt copy differs: %s", check.Summary(d))
	}
	if n := e.count(`SELECT count(*) FROM (
		SELECT account, event_id, amount FROM views.statement WHERE consumer = 'balances'
		EXCEPT SELECT account, event_id, amount FROM views.statement WHERE consumer = 'balances_v2') x`); n != 0 {
		t.Fatalf("%d statement rows differ between the copies", n)
	}
	t.Logf("rebuilt %d events into a new projection in %v (%.0f events/s); it matches the ledger and the live copy",
		total, took.Round(time.Millisecond), float64(total)/took.Seconds())

	// Lose the original read model entirely, then rebuild it in place.
	if err := e.js.DeleteConsumer(ctx, bus.Stream, "balances"); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{"views.balances", "views.statement", "views.processed"} {
		if _, err := e.pool.Exec(ctx, "DELETE FROM "+tbl+" WHERE consumer = 'balances'"); err != nil {
			t.Fatal(err)
		}
	}
	e.runProjector("balances", 0)
	e.caughtUp("balances", 120*time.Second)
	if d := e.projectionDiffs("balances"); len(d) > 0 {
		t.Fatalf("rebuilt-in-place projection differs: %s", check.Summary(d))
	}
}
