// Package e2e runs the system as separate processes against PostgreSQL and
// an embedded NATS server, kills them at chosen points, and checks what
// survived.
package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/builtbyshailendra/event-ledger/bus"
	"github.com/builtbyshailendra/event-ledger/check"
	"github.com/builtbyshailendra/event-ledger/internal/testenv"
	"github.com/builtbyshailendra/event-ledger/ledger"
)

var (
	ctx    = context.Background()
	binary string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ledger-bin")
	if err != nil {
		panic(err)
	}
	binary = filepath.Join(dir, "ledger.exe")
	if out, err := exec.Command("go", "build", "-o", binary, "../cmd/ledger").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type env struct {
	t       *testing.T
	pool    *pgxpool.Pool
	dbURL   string
	natsDir string
	port    int
	ns      *server.Server
	nc      *nats.Conn
	js      jetstream.JetStream
	l       *ledger.Ledger
}

// newEnv gives a test a fresh database and NATS server. dup is the
// stream's deduplication window (0 = the default 2 minutes).
func newEnv(t *testing.T, dup time.Duration) *env {
	t.Helper()
	e := &env{t: t, natsDir: t.TempDir()}
	e.pool, e.dbURL = testenv.DB(t)
	e.l = ledger.New(e.pool)
	e.startNATS()
	if err := bus.Setup(ctx, e.js, dup); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.nc.Close()
		e.ns.Shutdown()
		e.ns.WaitForShutdown()
	})
	return e
}

func (e *env) natsURL() string { return fmt.Sprintf("nats://127.0.0.1:%d", e.port) }

func (e *env) startNATS() {
	ns, err := bus.Server(e.natsDir, e.port)
	if err != nil {
		e.t.Fatal(err)
	}
	e.ns, e.port = ns, bus.Port(ns)
	if e.nc == nil {
		if e.nc, e.js, err = bus.Connect(e.natsURL(), "test"); err != nil {
			e.t.Fatal(err)
		}
	}
}

// restartNATS stops the server and starts it again on the same port and
// store; clients reconnect on their own.
func (e *env) restartNATS(down time.Duration) {
	e.ns.Shutdown()
	e.ns.WaitForShutdown()
	time.Sleep(down)
	e.startNATS()
}

// proc is a child process running one piece of the system.
type proc struct {
	cmd  *exec.Cmd
	out  *syncBuf
	done chan struct{}
	err  error
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// start runs `ledger <sub>`; crashAt is a LEDGER_CRASH spec or "".
func (e *env) start(sub, crashAt string, args ...string) *proc {
	e.t.Helper()
	cmd := exec.Command(binary, append([]string{sub, "-db", e.dbURL, "-nats", e.natsURL()}, args...)...)
	cmd.Env = append(os.Environ(), "LEDGER_CRASH="+crashAt)
	p := &proc{cmd: cmd, out: &syncBuf{}, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = p.out, p.out
	if err := cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	e.t.Cleanup(p.kill)
	return p
}

// kill ends the process the hard way (TerminateProcess on Windows).
func (p *proc) kill() {
	select {
	case <-p.done:
	default:
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

// wait returns the exit code, or -1 if it is still running after timeout.
func (p *proc) wait(timeout time.Duration) int {
	select {
	case <-p.done:
		var ee *exec.ExitError
		if errors.As(p.err, &ee) {
			return ee.ExitCode()
		}
		return 0
	case <-time.After(timeout):
		return -1
	}
}

func (p *proc) running() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (e *env) count(sql string, args ...any) int64 {
	e.t.Helper()
	var n int64
	if err := e.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// waitFor polls cond until it holds or timeout passes.
func (e *env) waitFor(what string, timeout time.Duration, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) waitPublished(timeout time.Duration) {
	e.t.Helper()
	e.waitFor("the outbox to drain", timeout, func() bool {
		return e.count(`SELECT count(*) FROM ledger.outbox WHERE published_at IS NULL`) == 0
	})
}

func (e *env) report() check.Report {
	e.t.Helper()
	r, err := check.Stream(ctx, e.pool, e.js)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

// seed opens n accounts with 1000 each.
func (e *env) seed(n int) []string {
	e.t.Helper()
	var accts []string
	for i := range n {
		a := fmt.Sprintf("acct-%02d", i)
		if err := e.l.Open(ctx, a); err != nil {
			e.t.Fatal(err)
		}
		if err := e.l.Deposit(ctx, "seed-"+a, a, 1000); err != nil {
			e.t.Fatal(err)
		}
		accts = append(accts, a)
	}
	return accts
}

// transfers runs n random transfers with the given number of workers and
// returns how many were accepted. Refusals for funds are normal.
func (e *env) transfers(l *ledger.Ledger, accts []string, n, workers int, seed uint64) int {
	e.t.Helper()
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	var firstErr error
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(w)))
			for i := w; i < n; i += workers {
				a, b := rng.IntN(len(accts)), rng.IntN(len(accts)-1)
				if b >= a {
					b++
				}
				err := l.Transfer(ctx, fmt.Sprintf("tx-%d-%d", seed, i), accts[a], accts[b], int64(1+rng.IntN(200)))
				mu.Lock()
				switch {
				case err == nil:
					accepted++
				case errors.Is(err, ledger.ErrInsufficientFunds), errors.Is(err, ledger.ErrCrashed):
				default:
					if firstErr == nil {
						firstErr = err
					}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		e.t.Fatal(firstErr)
	}
	return accepted
}

func lines(s, substr string) int {
	n := 0
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, substr) {
			n++
		}
	}
	return n
}
