// Package bus sets up NATS JetStream: an embedded server for tests and
// demos, and the two streams the ledger uses.
package bus

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// Stream holds every ledger event, forever: projections are rebuilt by
	// replaying it from the start.
	Stream   = "LEDGER"
	Subjects = "ledger.events.>"
	// DLQStream holds events a consumer gave up on.
	DLQStream   = "LEDGER_DLQ"
	DLQSubjects = "ledger.dlq.>"
)

// Server starts an embedded NATS server with JetStream storing to dir.
// port 0 picks a free port; pass the same dir and port to restart it.
func Server(dir string, port int) (*server.Server, error) {
	if port == 0 {
		port = -1
	}
	ns, err := server.NewServer(&server.Options{
		ServerName: "ledger", Host: "127.0.0.1", Port: port,
		JetStream: true, StoreDir: dir, NoLog: true, NoSigs: true,
	})
	if err != nil {
		return nil, err
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		return nil, fmt.Errorf("nats server not ready")
	}
	return ns, nil
}

// Port returns the port a server listens on.
func Port(ns *server.Server) int { return ns.Addr().(*net.TCPAddr).Port }

// Connect connects with unlimited reconnects, so a client survives a server
// restart.
func Connect(url, name string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url, nats.Name(name), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond))
	if err != nil {
		return nil, nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}

// Setup creates or updates both streams. dup is JetStream's deduplication
// window: a message with the same ID as one stored within that window is
// dropped. It is a window, not a guarantee (see the tests).
func Setup(ctx context.Context, js jetstream.JetStream, dup time.Duration) error {
	if dup == 0 {
		dup = 2 * time.Minute
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: Stream, Subjects: []string{Subjects}, Storage: jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy, Duplicates: dup,
	}); err != nil {
		return err
	}
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: DLQStream, Subjects: []string{DLQSubjects}, Storage: jetstream.FileStorage,
	})
	return err
}
