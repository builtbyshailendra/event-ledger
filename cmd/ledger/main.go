// Command ledger runs the pieces of the system as separate processes, so
// tests can kill them.
//
//	ledger demo    [-transfers 5000]   everything in one process, then the checks
//	ledger relay   -db URL -nats URL
//	ledger project -db URL -nats URL -name balances
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/builtbyshailendra/event-ledger/bus"
	"github.com/builtbyshailendra/event-ledger/projector"
	"github.com/builtbyshailendra/event-ledger/relay"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ledger demo|relay|project [flags]")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	if cmd == "demo" {
		fs := flag.NewFlagSet("demo", flag.ExitOnError)
		admin := fs.String("admin", "postgres://postgres@127.0.0.1:5433/postgres?sslmode=disable", "maintenance database URL")
		n := fs.Int("transfers", 5000, "number of transfers")
		_ = fs.Parse(args)
		if err := demo(*admin, *n); err != nil {
			fail(err)
		}
		return
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dbURL := fs.String("db", "postgres://postgres@127.0.0.1:5433/event_ledger?sslmode=disable", "PostgreSQL URL")
	natsURL := fs.String("nats", "nats://127.0.0.1:4222", "NATS URL")
	poll := fs.Duration("poll", time.Second, "relay: longest wait between rounds")
	noListen := fs.Bool("no-listen", false, "relay: poll only, no LISTEN/NOTIFY")
	cursor := fs.Bool("cursor", false, "relay: planted bug, track the highest id published")
	name := fs.String("name", "balances", "project: durable consumer and projection name")
	ackWait := fs.Duration("ack-wait", 5*time.Second, "project: redeliver after this long without an ack")
	ackFirst := fs.Bool("ack-before-commit", false, "project: planted bug, ack before writing")
	noDedup := fs.Bool("no-dedup", false, "project: planted bug, apply redeliveries again")
	batch := fs.Int("batch", 100, "project: events per transaction")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	pool, err := pgxpool.New(ctx, *dbURL)
	if err != nil {
		fail(err)
	}
	defer pool.Close()
	nc, js, err := bus.Connect(*natsURL, "ledger-"+cmd)
	if err != nil {
		fail(err)
	}
	defer nc.Close()
	logf := func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) }

	switch cmd {
	case "relay":
		r := &relay.Relay{Pool: pool, JS: js, Poll: *poll, NoListen: *noListen, Cursor: *cursor, Log: logf}
		err = r.Run(ctx)
	case "project":
		p := &projector.Projector{Pool: pool, JS: js, Name: *name, AckWait: *ackWait, Batch: *batch, Log: logf,
			Bugs: projector.Bugs{AckBeforeCommit: *ackFirst, NoDedup: *noDedup}}
		err = p.Run(ctx)
	default:
		fail(fmt.Errorf("unknown command %q", cmd))
	}
	if err != nil && ctx.Err() == nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "ledger:", err)
	os.Exit(1)
}
