// Package projector builds read models from the stream.
//
// Delivery from JetStream is at-least-once: a message is redelivered if its
// ack doesn't arrive within AckWait (the consumer crashed, the ack was lost,
// processing was slow), and the stream itself can hold a duplicate once the
// relay's deduplication window has passed. So the projector is idempotent:
// it records each event ID in views.processed in the same transaction as the
// change, and acknowledges only after that transaction commits.
package projector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/builtbyshailendra/event-ledger/bus"
	"github.com/builtbyshailendra/event-ledger/crash"
	"github.com/builtbyshailendra/event-ledger/events"
)

// Bugs are deliberate mistakes for the tests to catch.
type Bugs struct {
	AckBeforeCommit bool // acknowledge first, then write: a crash loses the event
	NoDedup         bool // apply every delivery: a redelivery applies twice
}

// Projector maintains views.balances and views.statement for one consumer
// name. Two projectors with different names build independent copies; that
// is how a projection is rebuilt: a new name, replayed from the start.
type Projector struct {
	Pool       *pgxpool.Pool
	JS         jetstream.JetStream
	Name       string        // durable consumer name and projection key
	AckWait    time.Duration // redelivery after this long without an ack, default 5s
	MaxDeliver int           // give up after this many deliveries, default 5
	Batch      int           // events per transaction, default 100; 1 = one transaction per event
	Bugs       Bugs
	// Fail, if set, can make applying an event fail (a stand-in for a
	// transient error such as a dependency being down).
	Fail func(events.Event) error
	Log  func(format string, args ...any)

	Applied    atomic.Int64
	Duplicates atomic.Int64
}

func (p *Projector) logf(format string, args ...any) {
	if p.Log != nil {
		p.Log(format, args...)
	}
}

// Consumer creates or updates the durable consumer.
func (p *Projector) Consumer(ctx context.Context) (jetstream.Consumer, error) {
	if p.AckWait == 0 {
		p.AckWait = 5 * time.Second
	}
	if p.MaxDeliver == 0 {
		p.MaxDeliver = 5
	}
	return p.JS.CreateOrUpdateConsumer(ctx, bus.Stream, jetstream.ConsumerConfig{
		Durable:       p.Name,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       p.AckWait,
		MaxDeliver:    p.MaxDeliver,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: bus.Subjects,
		MaxAckPending: 1000,
	})
}

// Run consumes until ctx ends.
func (p *Projector) Run(ctx context.Context) error {
	if p.Batch == 0 {
		p.Batch = 100
	}
	var cons jetstream.Consumer
	var err error
	for {
		if cons, err = p.Consumer(ctx); err == nil {
			break
		}
		p.logf("projector: %v; retrying", err)
		if !sleep(ctx, 500*time.Millisecond) {
			return ctx.Err()
		}
	}
	p.logf("projector %s: running", p.Name)
	for ctx.Err() == nil {
		batch, err := cons.Fetch(p.Batch, jetstream.FetchMaxWait(200*time.Millisecond))
		if err != nil { // NATS unreachable: wait and try again
			sleep(ctx, 200*time.Millisecond)
			continue
		}
		var items []item
		for msg := range batch.Messages() {
			if it, ok := p.decode(ctx, msg); ok {
				items = append(items, it)
			}
		}
		if len(items) > 0 {
			p.process(ctx, items)
		}
	}
	return ctx.Err()
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

type item struct {
	msg       jetstream.Msg
	md        *jetstream.MsgMetadata
	ev        events.Event
	ledgerSeq int64
}

// decode parses a message; one that can never be valid is handled as poison.
func (p *Projector) decode(ctx context.Context, msg jetstream.Msg) (item, bool) {
	md, err := msg.Metadata()
	if err != nil {
		p.logf("projector: %v", err)
		return item{}, false
	}
	ev, err := events.Decode(msg.Data())
	if err != nil {
		if err := p.poison(ctx, msg, md, err); err != nil {
			p.logf("projector: %v", err)
		}
		return item{}, false
	}
	ls, _ := strconv.ParseInt(msg.Headers().Get(events.HeaderLedgerSeq), 10, 64)
	return item{msg: msg, md: md, ev: ev, ledgerSeq: ls}, true
}

// process applies a batch in one transaction, then acknowledges it. If the
// transaction fails, the events are retried one at a time, so one event that
// keeps failing doesn't hold back the others.
func (p *Projector) process(ctx context.Context, items []item) {
	if p.Bugs.AckBeforeCommit {
		for _, it := range items {
			_ = it.msg.Ack()
		}
	}
	applied, err := p.apply(ctx, items)
	if err != nil {
		if len(items) > 1 {
			for _, it := range items {
				p.process(ctx, []item{it})
			}
			return
		}
		it := items[0]
		_ = it.msg.NakWithDelay(time.Duration(it.md.NumDelivered) * 200 * time.Millisecond)
		p.logf("projector: event %s (delivery %d): %v", it.ev.ID, it.md.NumDelivered, err)
		return
	}
	crash.Point("projector.after-commit") // applied, not acknowledged
	if !p.Bugs.AckBeforeCommit {
		for _, it := range items {
			_ = it.msg.Ack()
		}
	}
	crash.Point("projector.after-ack")
	for i, ok := range applied {
		if ok {
			p.Applied.Add(1)
		} else {
			p.Duplicates.Add(1)
			p.logf("projector: duplicate %s (stream seq %d, delivery %d) skipped", items[i].ev.ID, items[i].md.Sequence.Stream, items[i].md.NumDelivered)
		}
	}
}

// apply changes the projection and records each event as processed, all in
// one transaction. For each event it reports false if it had been applied
// already. Statements are pipelined (pgx.Batch): the cost is round trips,
// not rows, so a batch of 100 events takes three round trips, not 500.
func (p *Projector) apply(ctx context.Context, items []item) ([]bool, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())

	// 1. Claim every event ID; the ones already there are duplicates.
	claims := &pgx.Batch{}
	for _, it := range items {
		claims.Queue(`INSERT INTO views.processed (consumer, event_id, stream_seq, ledger_seq, occurred_at)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
			p.Name, it.ev.ID, int64(it.md.Sequence.Stream), it.ledgerSeq, it.ev.At)
	}
	applied := make([]bool, len(items))
	br := tx.SendBatch(ctx, claims)
	for i := range items {
		tag, err := br.Exec()
		if err != nil {
			br.Close()
			return nil, err
		}
		applied[i] = tag.RowsAffected() == 1 || p.Bugs.NoDedup
	}
	if err := br.Close(); err != nil {
		return nil, err
	}

	// 2. Apply the new ones: one statement row each, and one balance update
	// per account with the batch's total (in account order, so two
	// projectors updating the same rows can't deadlock).
	changes := &pgx.Batch{}
	delta, count := map[string]int64{}, map[string]int64{}
	for i, it := range items {
		if !applied[i] {
			continue
		}
		if p.Fail != nil {
			if err := p.Fail(it.ev); err != nil {
				return nil, err
			}
		}
		for acct, d := range it.ev.Changes() {
			delta[acct] += d
			count[acct]++
			if it.ev.Type != events.AccountOpened {
				changes.Queue(`INSERT INTO views.statement (consumer, account, event_id, ledger_seq, amount)
					VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, p.Name, acct, it.ev.ID, it.ledgerSeq, d)
			}
		}
	}
	accts := make([]string, 0, len(delta))
	for a := range delta {
		accts = append(accts, a)
	}
	sort.Strings(accts)
	for _, a := range accts {
		changes.Queue(`INSERT INTO views.balances (consumer, account, balance, events) VALUES ($1, $2, $3, $4)
			ON CONFLICT (consumer, account) DO UPDATE SET balance = balances.balance + EXCLUDED.balance, events = balances.events + EXCLUDED.events`,
			p.Name, a, delta[a], count[a])
	}
	if changes.Len() > 0 {
		if err := tx.SendBatch(ctx, changes).Close(); err != nil {
			return nil, err
		}
	}
	crash.Point("projector.before-commit")
	return applied, tx.Commit(ctx)
}

// poison handles a message that can never be processed. For now it is
// terminated so it isn't redelivered; step 5 adds a dead-letter stream.
func (p *Projector) poison(ctx context.Context, msg jetstream.Msg, md *jetstream.MsgMetadata, cause error) error {
	_ = msg.TermWithReason(cause.Error())
	return fmt.Errorf("stream seq %d: %w", md.Sequence.Stream, errors.Join(errPoison, cause))
}

var errPoison = errors.New("poison message terminated")
