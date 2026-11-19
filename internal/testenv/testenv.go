// Package testenv gives tests a fresh PostgreSQL database on the local test
// cluster (port 5433) and an embedded NATS server.
package testenv

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/builtbyshailendra/event-ledger/internal/schema"
)

// AdminURL is the maintenance database on the test cluster. Override with
// LEDGER_PG_ADMIN.
func AdminURL() string {
	if u := os.Getenv("LEDGER_PG_ADMIN"); u != "" {
		return u
	}
	return "postgres://postgres@127.0.0.1:5433/postgres?sslmode=disable"
}

var dbCounter atomic.Int64

// DB creates an empty database with the schema applied, and drops it when
// the test ends. It returns the pool and the database URL (for subprocesses).
func DB(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, AdminURL())
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("event_ledger_t%d_%d_%d", os.Getpid(), time.Now().UnixNano()%1e6, dbCounter.Add(1))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	url := fmt.Sprintf("postgres://postgres@127.0.0.1:5433/%s?sslmode=disable", name)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close()
	})
	if err := schema.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool, url
}
