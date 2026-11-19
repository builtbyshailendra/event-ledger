package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/builtbyshailendra/event-ledger/bus"
	"github.com/builtbyshailendra/event-ledger/check"
	"github.com/builtbyshailendra/event-ledger/internal/schema"
	"github.com/builtbyshailendra/event-ledger/ledger"
	"github.com/builtbyshailendra/event-ledger/projector"
	"github.com/builtbyshailendra/event-ledger/relay"
)

// demo runs the whole system in one process against a fresh database named
// event_ledger_demo (left in place for inspection) and an embedded NATS
// server, then prints what the checks found.
func demo(admin string, transfers int) error {
	ctx := context.Background()
	a, err := pgxpool.New(ctx, admin)
	if err != nil {
		return err
	}
	defer a.Close()
	for _, q := range []string{"DROP DATABASE IF EXISTS event_ledger_demo WITH (FORCE)", "CREATE DATABASE event_ledger_demo"} {
		if _, err := a.Exec(ctx, q); err != nil {
			return err
		}
	}
	dbURL := strings.Replace(admin, "/postgres?", "/event_ledger_demo?", 1)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := schema.Apply(ctx, pool); err != nil {
		return err
	}

	dir, err := os.MkdirTemp("", "ledger-demo-nats")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	ns, err := bus.Server(dir, 0)
	if err != nil {
		return err
	}
	defer ns.Shutdown()
	nc, js, err := bus.Connect(ns.ClientURL(), "demo")
	if err != nil {
		return err
	}
	defer nc.Close()
	if err := bus.Setup(ctx, js, 0); err != nil {
		return err
	}

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = (&relay.Relay{Pool: pool, JS: js}).Run(rctx) }()
	pr := &projector.Projector{Pool: pool, JS: js, Name: "balances"}
	go func() { _ = pr.Run(rctx) }()

	l := ledger.New(pool)
	const accounts = 20
	for i := range accounts {
		acct := fmt.Sprintf("acct-%02d", i)
		if err := l.Open(ctx, acct); err != nil {
			return err
		}
		if err := l.Deposit(ctx, "seed-"+acct, acct, 1000); err != nil {
			return err
		}
	}
	fmt.Printf("database event_ledger_demo, NATS %s; %d transfers from 8 writers...\n", ns.ClientURL(), transfers)
	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, refused := 0, 0
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 42))
			for i := w; i < transfers; i += 8 {
				x, y := rng.IntN(accounts), rng.IntN(accounts-1)
				if y >= x {
					y++
				}
				err := l.Transfer(ctx, fmt.Sprintf("t-%d", i), fmt.Sprintf("acct-%02d", x), fmt.Sprintf("acct-%02d", y), int64(1+rng.IntN(200)))
				mu.Lock()
				if err == nil {
					accepted++
				} else if errors.Is(err, ledger.ErrInsufficientFunds) {
					refused++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	wrote := time.Since(start)

	var commands int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM ledger.commands`).Scan(&commands)
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		var pending, processed int64
		_ = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM ledger.outbox WHERE published_at IS NULL),
			(SELECT count(*) FROM views.processed WHERE consumer = 'balances')`).Scan(&pending, &processed)
		if pending == 0 && processed == commands {
			break
		}
	}
	caught := time.Since(start)

	rep, err := check.Stream(ctx, pool, js)
	if err != nil {
		return err
	}
	diffs, err := check.Projection(ctx, pool, "balances")
	if err != nil {
		return err
	}
	var s50, s99, p50, p99 float64
	_ = pool.QueryRow(ctx, `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - created_at)),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - created_at)) FROM ledger.outbox`).Scan(&s50, &s99)
	_ = pool.QueryRow(ctx, `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - occurred_at)),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - occurred_at)) FROM views.processed`).Scan(&p50, &p99)

	fmt.Printf("%d accepted, %d refused for funds; written in %v, projected by %v\n", accepted, refused, wrote.Round(time.Millisecond), caught.Round(time.Millisecond))
	fmt.Printf("commit -> stream:      p50 %5.1f ms, p99 %5.1f ms\n", s50*1000, s99*1000)
	fmt.Printf("event  -> projection:  p50 %5.1f ms, p99 %5.1f ms\n", p50*1000, p99*1000)
	fmt.Printf("stream check:     %v\n", rep)
	fmt.Printf("projection check: %d accounts differ from the ledger\n", len(diffs))
	rows, err := pool.Query(ctx, `SELECT event_id, ledger_seq, amount FROM views.statement
		WHERE consumer = 'balances' AND account = 'acct-00' ORDER BY ledger_seq DESC LIMIT 5`)
	if err == nil {
		fmt.Println("latest statement lines for acct-00:")
		for rows.Next() {
			var id string
			var seq, amt int64
			_ = rows.Scan(&id, &seq, &amt)
			fmt.Printf("  #%-6d %-8s %+d\n", seq, id, amt)
		}
		rows.Close()
	}
	if !rep.OK() || len(diffs) > 0 {
		return fmt.Errorf("checks failed")
	}
	return nil
}
