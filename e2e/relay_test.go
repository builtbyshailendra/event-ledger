package e2e

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/builtbyshailendra/event-ledger/events"
	"github.com/builtbyshailendra/event-ledger/ledger"
	"github.com/builtbyshailendra/event-ledger/relay"
)

var droppedRe = regexp.MustCompile(`stream dropped (\d+) duplicates`)

// dropped sums the duplicates a relay process said the stream dropped.
func dropped(out string) int {
	n := 0
	for _, m := range droppedRe.FindAllStringSubmatch(out, -1) {
		k, _ := strconv.Atoi(m[1])
		n += k
	}
	return n
}

// latency reports the median and 99th percentile of commit-to-published.
func (e *env) latency() (time.Duration, time.Duration) {
	var p50, p99 float64
	if err := e.pool.QueryRow(ctx, `
		SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - created_at)),
		       percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - created_at))
		FROM ledger.outbox`).Scan(&p50, &p99); err != nil {
		e.t.Fatal(err)
	}
	return time.Duration(p50 * float64(time.Second)), time.Duration(p99 * float64(time.Second))
}

func TestRelayPublishesEachEventOnce(t *testing.T) {
	for _, tc := range []struct {
		name     string
		noListen bool
	}{{"LISTEN/NOTIFY", false}, {"polling every 500 ms", true}} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, 0)
			r := &relay.Relay{Pool: e.pool, JS: e.js, NoListen: tc.noListen, Poll: 500 * time.Millisecond}
			rctx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() { _ = r.Run(rctx) }()

			accts := e.seed(20)
			start := time.Now()
			accepted := e.transfers(e.l, accts, 2000, 8, 1)
			e.waitPublished(30 * time.Second)
			elapsed := time.Since(start)
			rep := e.report()
			if !rep.OK() || rep.Messages != rep.Committed {
				t.Fatal(rep)
			}
			p50, p99 := e.latency()
			t.Logf("%d events (%d transfers accepted) committed and published in %v; commit-to-stream p50 %v, p99 %v",
				rep.Committed, accepted, elapsed.Round(time.Millisecond), p50.Round(100*time.Microsecond), p99.Round(100*time.Microsecond))
		})
	}
}

// The relay is killed at each step of a round: before publishing, halfway
// through the batch, after publishing but before marking, and after marking.
// A restarted relay must leave every committed event in the stream exactly
// once.
func TestRelayCrashAtEachPoint(t *testing.T) {
	for _, point := range []string{"relay.before-publish", "relay.mid-publish", "relay.after-publish", "relay.after-mark"} {
		t.Run(point, func(t *testing.T) {
			e := newEnv(t, 0)
			accts := e.seed(10)
			e.transfers(e.l, accts, 480, 4, 2) // 500 events: 3 batches of up to 200

			p := e.start("relay", point+":2") // die in the second batch
			if code := p.wait(30 * time.Second); code != 137 {
				t.Fatalf("relay exit code %d, want 137\n%s", code, p.out)
			}
			before := e.report()

			p2 := e.start("relay", "")
			e.waitPublished(30 * time.Second)
			rep := e.report()
			if !rep.OK() || rep.Messages != rep.Committed {
				t.Fatalf("%v\nrelay output:\n%s", rep, p2.out)
			}
			t.Logf("crash left %d of %d in the stream, %d unmarked; after restart: %d messages, stream dropped %d resent duplicates",
				before.Messages, before.Committed, before.Unpublished, rep.Messages, dropped(p2.out.String()))
		})
	}
}

// JetStream drops a duplicate only within the stream's window. A relay that
// stays down longer than that publishes the same events again, and they are
// stored twice. Consumers must deduplicate too.
func TestDedupWindowEnds(t *testing.T) {
	e := newEnv(t, time.Second)
	accts := e.seed(5)
	e.transfers(e.l, accts, 290, 2, 3)

	p := e.start("relay", "relay.after-publish:1") // publish 200, die before marking
	if code := p.wait(30 * time.Second); code != 137 {
		t.Fatalf("exit %d\n%s", code, p.out)
	}
	time.Sleep(1500 * time.Millisecond) // longer than the window
	p2 := e.start("relay", "")
	e.waitPublished(30 * time.Second)
	rep := e.report()
	if rep.Duplicates == 0 || len(rep.Missing) > 0 {
		t.Fatalf("expected duplicates once the window passed: %v", rep)
	}
	t.Logf("window 1 s, relay down 1.5 s: %d events stored twice (stream dropped %d)", rep.Duplicates, dropped(p2.out.String()))
}

// Two relays run; the advisory lock lets only one publish. Killing the
// active one mid-stream hands over to the standby.
func TestStandbyRelayTakesOver(t *testing.T) {
	e := newEnv(t, 0)
	accts := e.seed(10)
	a := e.start("relay", "")
	b := e.start("relay", "")
	e.waitFor("one relay to become active", 10*time.Second, func() bool {
		return lines(a.out.String()+b.out.String(), "relay: active") == 1
	})
	active, standby := a, b
	if lines(b.out.String(), "relay: active") == 1 {
		active, standby = b, a
	}
	done := make(chan int)
	go func() { done <- e.transfers(e.l, accts, 1500, 4, 4) }()
	e.waitFor("some events to be published", 10*time.Second, func() bool {
		return e.count(`SELECT count(*) FROM ledger.outbox WHERE published_at IS NOT NULL`) > 300
	})
	active.kill()
	e.waitFor("the standby to take over", 10*time.Second, func() bool {
		return lines(standby.out.String(), "relay: active") == 1
	})
	<-done
	e.waitPublished(30 * time.Second)
	rep := e.report()
	if !rep.OK() {
		t.Fatal(rep)
	}
	t.Logf("active relay killed mid-stream, standby took over: %d events, each once", rep.Messages)
}

// A transaction that inserts into the outbox first can commit last. A relay
// that remembers "the highest id I published" never looks back and loses it.
func TestCursorRelaySkipsALateCommit(t *testing.T) {
	for _, cursor := range []bool{true, false} {
		name := "published_at IS NULL"
		if cursor {
			name = "id > last (planted bug)"
		}
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 0)
			e.seed(2)
			e.waitFor("nothing", time.Second, func() bool { return true })
			r := &relay.Relay{Pool: e.pool, JS: e.js, Cursor: cursor, Poll: 100 * time.Millisecond}
			rctx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() { _ = r.Run(rctx) }()
			e.waitPublished(10 * time.Second)

			// "slow" gets its outbox id first, then waits before committing.
			release := make(chan struct{})
			slow := ledger.New(e.pool)
			slow.Crash = func(point, id string) bool {
				if id == "slow" {
					<-release
				}
				return false
			}
			errc := make(chan error)
			go func() { errc <- slow.Deposit(ctx, "slow", "acct-00", 5) }()
			e.waitFor("slow to reach its commit", 5*time.Second, func() bool {
				return e.count(`SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction'`) > 0
			})
			// "fast" gets a higher id and commits first; the relay publishes it.
			if err := e.l.Deposit(ctx, "fast", "acct-01", 5); err != nil {
				t.Fatal(err)
			}
			e.waitPublished(10 * time.Second)
			close(release)
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
			time.Sleep(500 * time.Millisecond) // a few relay rounds
			rep := e.report()
			lost := len(rep.Missing) == 1 && rep.Missing[0] == "slow"
			t.Logf("%v", rep)
			if lost != cursor {
				t.Fatalf("cursor=%v: missing %v", cursor, rep.Missing)
			}
		})
	}
}

// The same crash, three ways of getting the event out. Every 10th command's
// process "dies" at the worst moment for its mode.
func TestDualWritesLoseOrInventEvents(t *testing.T) {
	for _, mode := range []ledger.Mode{ledger.DualWrite, ledger.PublishFirst, ledger.Outbox} {
		t.Run(mode.String(), func(t *testing.T) {
			e := newEnv(t, 0)
			// The relay runs in every mode: account setup always goes
			// through the outbox, so only the transfers differ.
			r := &relay.Relay{Pool: e.pool, JS: e.js}
			rctx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() { _ = r.Run(rctx) }()
			accts := e.seed(10)
			l := ledger.New(e.pool)
			l.Mode = mode
			l.Publish = func(ctx context.Context, ev events.Event) error {
				return publishDirect(ctx, e, ev)
			}
			point := map[ledger.Mode]string{ledger.DualWrite: "after-commit", ledger.PublishFirst: "after-publish", ledger.Outbox: "before-commit"}[mode]
			var crashed int
			l.Crash = func(p, id string) bool {
				if p != point {
					return false
				}
				n, _ := strconv.Atoi(id[len("tx-5-"):])
				if n%10 == 9 {
					crashed++
					return true
				}
				return false
			}
			// Sequential, so the crash count is exact.
			for i := range 300 {
				err := l.Transfer(ctx, fmt.Sprintf("tx-5-%d", i), accts[i%10], accts[(i+3)%10], 7)
				if err != nil && !errors.Is(err, ledger.ErrCrashed) && !errors.Is(err, ledger.ErrInsufficientFunds) {
					t.Fatal(err)
				}
			}
			e.waitPublished(10 * time.Second)
			rep := e.report()
			t.Logf("%d crashes: %v", crashed, rep)
			ok := rep.OK() && rep.Messages == rep.Committed
			if ok != (mode == ledger.Outbox) {
				t.Fatalf("mode %v: ok=%v", mode, ok)
			}
		})
	}
}

func publishDirect(ctx context.Context, e *env, ev events.Event) error {
	payload, err := jsonMarshal(ev)
	if err != nil {
		return err
	}
	_, err = e.js.Publish(ctx, ev.Subject(), payload, jetstreamMsgID(ev.ID))
	return err
}
