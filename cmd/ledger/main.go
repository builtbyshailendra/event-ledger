// Command ledger runs the pieces of the system as separate processes, so
// tests can kill them.
//
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
	"github.com/builtbyshailendra/event-ledger/relay"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ledger relay|project [flags]")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dbURL := fs.String("db", "postgres://postgres@127.0.0.1:5433/event_ledger?sslmode=disable", "PostgreSQL URL")
	natsURL := fs.String("nats", "nats://127.0.0.1:4222", "NATS URL")
	poll := fs.Duration("poll", time.Second, "relay: longest wait between rounds")
	noListen := fs.Bool("no-listen", false, "relay: poll only, no LISTEN/NOTIFY")
	cursor := fs.Bool("cursor", false, "relay: planted bug, track the highest id published")
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
