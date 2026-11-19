package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/builtbyshailendra/event-ledger/bus"
	"github.com/builtbyshailendra/event-ledger/check"
	"github.com/builtbyshailendra/event-ledger/projector"
	"github.com/builtbyshailendra/event-ledger/relay"
)

// runRelay runs a relay in this process until the test ends.
func (e *env) runRelay() *relay.Relay {
	r := &relay.Relay{Pool: e.pool, JS: e.js, Poll: 200 * time.Millisecond}
	rctx, cancel := context.WithCancel(ctx)
	e.t.Cleanup(cancel)
	go func() { _ = r.Run(rctx) }()
	return r
}

func (e *env) runProjector(name string, batch int) *projector.Projector {
	p := &projector.Projector{Pool: e.pool, JS: e.js, Name: name, AckWait: time.Second, Batch: batch}
	pctx, cancel := context.WithCancel(ctx)
	e.t.Cleanup(cancel)
	go func() { _ = p.Run(pctx) }()
	return p
}

// idle reports whether a consumer has been delivered everything in the
// stream and has nothing waiting for an ack.
func (e *env) idle(name string) bool {
	c, err := e.js.Consumer(ctx, bus.Stream, name)
	if err != nil {
		return false
	}
	ci, err := c.Info(ctx)
	if err != nil {
		return false
	}
	s, _ := e.js.Stream(ctx, bus.Stream)
	si, err := s.Info(ctx)
	if err != nil {
		return false
	}
	return ci.NumPending == 0 && ci.NumAckPending == 0 && ci.Delivered.Stream == si.State.LastSeq
}

// caughtUp waits until the outbox is drained and the consumer is idle.
func (e *env) caughtUp(name string, timeout time.Duration) {
	e.t.Helper()
	e.waitPublished(timeout)
	e.waitFor(name+" to catch up", timeout, func() bool { return e.idle(name) })
}

func (e *env) projectionDiffs(name string) []string {
	e.t.Helper()
	d, err := check.Projection(ctx, e.pool, name)
	if err != nil {
		e.t.Fatal(err)
	}
	return d
}

func TestProjectionMatchesTheLedger(t *testing.T) {
	for _, batch := range []int{1, 100} {
		t.Run(fmt.Sprintf("%d events per transaction", batch), func(t *testing.T) { projectionUnderLoad(t, batch) })
	}
}

func projectionUnderLoad(t *testing.T, batch int) {
	e := newEnv(t, 0)
	e.runRelay()
	e.runProjector("balances", batch)
	accts := e.seed(20)

	// Sample the consumer's backlog while the load runs.
	var maxLag uint64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			case <-time.After(25 * time.Millisecond):
			}
			if c, err := e.js.Consumer(ctx, bus.Stream, "balances"); err == nil {
				if ci, err := c.Info(ctx); err == nil {
					maxLag = max(maxLag, ci.NumPending+uint64(ci.NumAckPending))
				}
			}
		}
	}()
	start := time.Now()
	e.transfers(e.l, accts, 3000, 8, 11)
	e.caughtUp("balances", 60*time.Second)
	elapsed := time.Since(start)
	close(stop)
	<-sampled

	if d := e.projectionDiffs("balances"); len(d) > 0 {
		t.Fatalf("projection differs from the ledger: %s", check.Summary(d))
	}
	var events int64
	var p50, p99 float64
	if err := e.pool.QueryRow(ctx, `SELECT count(*),
		percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - occurred_at)),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - occurred_at))
		FROM views.processed WHERE consumer = 'balances'`).Scan(&events, &p50, &p99); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d events projected in %v; event-to-projection p50 %.1f ms, p99 %.1f ms; largest backlog %d messages",
		events, elapsed.Round(time.Millisecond), p50*1000, p99*1000, maxLag)
}

// The projector is killed at three points in its second batch of 50:
// after writing but before committing, after committing but before acking,
// and after acking. On restart, unacknowledged messages are redelivered once
// AckWait passes; the processed table turns the replay of an already-applied
// event into a no-op.
func TestProjectorCrashAtEachPoint(t *testing.T) {
	for _, point := range []string{"projector.before-commit", "projector.after-commit", "projector.after-ack"} {
		t.Run(point, func(t *testing.T) {
			e := newEnv(t, 0)
			e.runRelay()
			accts := e.seed(10)
			e.transfers(e.l, accts, 300, 4, 6)
			e.waitPublished(30 * time.Second)

			p := e.start("project", point+":2", "-ack-wait", "1s", "-batch", "50")
			if code := p.wait(30 * time.Second); code != 137 {
				t.Fatalf("projector exit %d\n%s", code, p.out)
			}
			applied := e.count(`SELECT count(*) FROM views.processed`)
			p2 := e.start("project", "", "-ack-wait", "1s", "-batch", "50")
			e.caughtUp("balances", 30*time.Second)
			if d := e.projectionDiffs("balances"); len(d) > 0 {
				t.Fatalf("projection differs: %s\n%s", check.Summary(d), p2.out)
			}
			t.Logf("crashed with %d events applied; after restart the projection matches; %d redeliveries skipped as duplicates",
				applied, lines(p2.out.String(), "duplicate"))
		})
	}
}

// Two planted consumer bugs, each with the crash that exposes it, next to
// the correct projector under the same crash.
func TestPlantedConsumerBugs(t *testing.T) {
	cases := []struct {
		name   string
		crash  string
		flags  []string
		broken bool
	}{
		{"correct, crash before commit", "projector.before-commit:2", nil, false},
		{"ack before commit, crash before commit", "projector.before-commit:2", []string{"-ack-before-commit"}, true},
		{"correct, crash after commit", "projector.after-commit:2", nil, false},
		{"no dedup, crash after commit", "projector.after-commit:2", []string{"-no-dedup"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, 0)
			e.runRelay()
			accts := e.seed(10)
			e.transfers(e.l, accts, 300, 4, 7)
			e.waitPublished(30 * time.Second)

			args := append([]string{"-ack-wait", "1s", "-batch", "50"}, tc.flags...)
			p := e.start("project", tc.crash, args...)
			if code := p.wait(30 * time.Second); code != 137 {
				t.Fatalf("exit %d\n%s", code, p.out)
			}
			e.start("project", "", args...)
			e.caughtUp("balances", 30*time.Second)
			time.Sleep(1500 * time.Millisecond) // past AckWait: any redelivery has happened
			e.waitFor("idle", 10*time.Second, func() bool { return e.idle("balances") })
			d := e.projectionDiffs("balances")
			t.Logf("%d accounts differ from the ledger %v", len(d), d)
			if (len(d) > 0) != tc.broken {
				t.Fatalf("broken=%v expected, got %d differences", tc.broken, len(d))
			}
		})
	}
}

// Past the relay's deduplication window the stream really does hold two
// copies of 200 events. The projector skips the second copy of each.
func TestProjectorAbsorbsDuplicatesPastTheWindow(t *testing.T) {
	e := newEnv(t, time.Second)
	accts := e.seed(5)
	e.transfers(e.l, accts, 290, 2, 8)
	p := e.start("relay", "relay.after-publish:1")
	if code := p.wait(30 * time.Second); code != 137 {
		t.Fatalf("exit %d\n%s", code, p.out)
	}
	time.Sleep(1500 * time.Millisecond)
	e.start("relay", "")
	e.waitPublished(30 * time.Second)
	if rep := e.report(); rep.Duplicates == 0 {
		t.Fatalf("expected duplicates in the stream: %v", rep)
	}
	pr := e.runProjector("balances", 0)
	e.caughtUp("balances", 30*time.Second)
	if d := e.projectionDiffs("balances"); len(d) > 0 {
		t.Fatalf("projection differs: %s", check.Summary(d))
	}
	t.Logf("stream held %d duplicate messages; projector applied %d and skipped %d", e.report().Duplicates, pr.Applied.Load(), pr.Duplicates.Load())
	if pr.Duplicates.Load() != int64(e.report().Duplicates) {
		t.Fatalf("skipped %d, stream has %d duplicates", pr.Duplicates.Load(), e.report().Duplicates)
	}
}
