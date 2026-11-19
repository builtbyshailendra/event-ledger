package e2e

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/builtbyshailendra/event-ledger/check"
)

var (
	flagChaosSeeds    = flag.Int("chaos-seeds", 2, "chaos runs")
	flagChaosDuration = flag.Duration("chaos-duration", 15*time.Second, "how long each chaos run writes")
)

// supervisor keeps a process running, like systemd or Kubernetes: when it
// exits for any reason, it is started again.
type supervisor struct {
	e    *env
	mu   sync.Mutex
	cur  map[string]*proc
	out  map[string]*syncBuf
	stop bool
	wg   sync.WaitGroup
}

func (s *supervisor) run(name, db, sub string, args ...string) {
	s.out[name] = &syncBuf{}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			s.mu.Lock()
			if s.stop {
				s.mu.Unlock()
				return
			}
			p := s.e.startLogged(s.out[name], db, sub, "", args...) // one log per role across restarts
			s.cur[name] = p
			s.mu.Unlock()
			<-p.done
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func (s *supervisor) kill(name string) {
	s.mu.Lock()
	p := s.cur[name]
	s.mu.Unlock()
	if p != nil {
		p.kill()
	}
}

func (s *supervisor) shutdown() {
	s.mu.Lock()
	s.stop = true
	procs := s.cur
	s.mu.Unlock()
	for _, p := range procs {
		p.kill()
	}
	s.wg.Wait()
}

// TestChaos runs two relays and two projectors (competing for one durable
// consumer) as supervised processes while 8 writers make transfers, and
// keeps breaking things: killing processes, restarting NATS, and cutting
// their database connections. Afterwards every committed event must be in
// the stream once, and the projection must match the ledger.
func TestChaos(t *testing.T) {
	for i := range *flagChaosSeeds {
		seed := uint64(100 + i)
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) { chaos(t, seed) })
	}
}

func chaos(t *testing.T, seed uint64) {
	e := newEnv(t, 0)
	accts := e.seed(20)
	app := func(name string) string { return e.dbURL + "&application_name=ledger-" + name }
	sup := &supervisor{e: e, cur: map[string]*proc{}, out: map[string]*syncBuf{}}
	roles := []string{"relay-a", "relay-b", "project-a", "project-b"}
	for _, r := range roles {
		sub, args := "relay", []string{}
		if r[:7] == "project" {
			sub, args = "project", []string{"-ack-wait", "2s"}
		}
		sup.run(r, app(r), sub, args...)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	committed := 0
	wg.Add(1)
	go func() { // writers
		defer wg.Done()
		for round := 0; ; round++ {
			select {
			case <-stop:
				return
			default:
			}
			committed += e.transfers(e.l, accts, 200, 8, seed*10_000+uint64(round))
		}
	}()

	rng := rand.New(rand.NewPCG(seed, 7))
	kills, natsRestarts, cuts := 0, 0, 0
	end := time.Now().Add(*flagChaosDuration)
	for time.Now().Before(end) {
		time.Sleep(time.Duration(200+rng.IntN(500)) * time.Millisecond)
		switch r := rng.IntN(10); {
		case r < 6:
			sup.kill(roles[rng.IntN(len(roles))])
			kills++
		case r < 8:
			if _, err := e.pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
				WHERE datname = current_database() AND application_name LIKE 'ledger-%'`); err != nil {
				t.Fatal(err)
			}
			cuts++
		default:
			e.restartNATS(time.Duration(200+rng.IntN(600)) * time.Millisecond)
			natsRestarts++
		}
	}
	close(stop)
	wg.Wait()

	e.caughtUp("balances", 90*time.Second)
	time.Sleep(2500 * time.Millisecond) // past AckWait, so any redelivery has landed
	e.waitFor("idle", 30*time.Second, func() bool { return e.idle("balances") })
	sup.shutdown()

	rep := e.report()
	if !rep.OK() || rep.Messages != rep.Committed {
		t.Fatalf("stream: %v", rep)
	}
	if d := e.projectionDiffs("balances"); len(d) > 0 {
		t.Fatalf("projection differs: %s", check.Summary(d))
	}
	if sum := e.count(`SELECT sum(balance) FROM ledger.accounts`); sum != 20*1000 {
		t.Fatalf("money not conserved: %d", sum)
	}
	if n := e.count(`SELECT count(*) FROM views.processed WHERE consumer = 'balances'`); n != int64(rep.Committed) {
		t.Fatalf("%d processed for %d committed", n, rep.Committed)
	}
	relayOut := sup.out["relay-a"].String() + sup.out["relay-b"].String()
	projOut := sup.out["project-a"].String() + sup.out["project-b"].String()
	t.Logf("%v of writes: %d events; %d process kills, %d connection cuts, %d NATS restarts; relays took over %d times, stream dropped %d resent duplicates, projectors skipped %d redeliveries; stream and projection exact",
		*flagChaosDuration, rep.Committed, kills, cuts, natsRestarts, lines(relayOut, "relay: active"), dropped(relayOut), lines(projOut, "duplicate"))
}
