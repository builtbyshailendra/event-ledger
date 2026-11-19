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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
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

	Applied      atomic.Int64
	Duplicates   atomic.Int64
	DeadLettered atomic.Int64
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
	go p.runDeadLetters(ctx)
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

// Dead-letter headers.
const (
	HeaderDLQConsumer = "Dlq-Consumer"
	HeaderDLQSeq      = "Dlq-Stream-Seq"
	HeaderDLQReason   = "Dlq-Reason"
	HeaderDLQSubject  = "Dlq-Subject"
)

// DLQSubject is where a consumer's dead letters go.
func DLQSubject(consumer string) string { return "ledger.dlq." + consumer }

// poison handles a message that can never be processed: no retry will make
// it decode. It is copied to the dead-letter stream, then terminated so
// JetStream stops redelivering it. If the copy fails, the message is
// negatively acknowledged instead, so it isn't lost.
func (p *Projector) poison(ctx context.Context, msg jetstream.Msg, md *jetstream.MsgMetadata, cause error) error {
	if err := p.deadLetter(ctx, msg.Subject(), msg.Data(), msg.Headers(), md.Sequence.Stream, cause.Error()); err != nil {
		_ = msg.NakWithDelay(time.Second)
		return fmt.Errorf("stream seq %d: dead-letter failed: %w", md.Sequence.Stream, err)
	}
	_ = msg.TermWithReason(cause.Error())
	p.DeadLettered.Add(1)
	p.logf("projector: stream seq %d dead-lettered: %v", md.Sequence.Stream, cause)
	return nil
}

func (p *Projector) deadLetter(ctx context.Context, subject string, data []byte, hdr nats.Header, seq uint64, reason string) error {
	m := nats.NewMsg(DLQSubject(p.Name))
	m.Data = data
	for k, v := range hdr {
		m.Header[k] = v
	}
	m.Header.Set(HeaderDLQConsumer, p.Name)
	m.Header.Set(HeaderDLQSeq, strconv.FormatUint(seq, 10))
	m.Header.Set(HeaderDLQSubject, subject)
	m.Header.Set(HeaderDLQReason, reason)
	_, err := p.JS.PublishMsg(ctx, m, jetstream.WithMsgID(fmt.Sprintf("dlq-%s-%d", p.Name, seq)))
	return err
}

// runDeadLetters handles events that failed MaxDeliver times. JetStream
// stops delivering them and publishes an advisory, which the advisory stream
// keeps; this copies each such event to the dead-letter stream.
func (p *Projector) runDeadLetters(ctx context.Context) {
	var cons jetstream.Consumer
	var err error
	for {
		cons, err = p.JS.CreateOrUpdateConsumer(ctx, bus.AdvisoryStream, jetstream.ConsumerConfig{
			Durable: "dlq-" + p.Name, AckPolicy: jetstream.AckExplicitPolicy,
			FilterSubject: bus.AdvisorySubject(p.Name), DeliverPolicy: jetstream.DeliverAllPolicy,
		})
		if err == nil {
			break
		}
		if !sleep(ctx, 500*time.Millisecond) {
			return
		}
	}
	stream, err := p.JS.Stream(ctx, bus.Stream)
	for err != nil {
		if !sleep(ctx, 500*time.Millisecond) {
			return
		}
		stream, err = p.JS.Stream(ctx, bus.Stream)
	}
	for ctx.Err() == nil {
		batch, err := cons.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			sleep(ctx, 200*time.Millisecond)
			continue
		}
		for adv := range batch.Messages() {
			var notice struct {
				StreamSeq  uint64 `json:"stream_seq"`
				Deliveries uint64 `json:"deliveries"`
			}
			if err := json.Unmarshal(adv.Data(), &notice); err != nil {
				_ = adv.Term()
				continue
			}
			raw, err := stream.GetMsg(ctx, notice.StreamSeq)
			if err == nil {
				err = p.deadLetter(ctx, raw.Subject, raw.Data, raw.Header, notice.StreamSeq,
					fmt.Sprintf("gave up after %d deliveries", notice.Deliveries))
			}
			if err != nil {
				_ = adv.NakWithDelay(time.Second)
				continue
			}
			_ = adv.Ack()
			p.DeadLettered.Add(1)
			p.logf("projector: stream seq %d dead-lettered after %d deliveries", notice.StreamSeq, notice.Deliveries)
		}
	}
}

// ReplayDeadLetters applies this consumer's dead letters again, for after
// the cause is fixed. It is safe to run more than once: events already
// applied are skipped. It returns how many were applied now, and the ones
// that still fail.
func (p *Projector) ReplayDeadLetters(ctx context.Context) (applied int, failed []string, err error) {
	s, err := p.JS.Stream(ctx, bus.DLQStream)
	if err != nil {
		return 0, nil, err
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(DLQSubject(p.Name)))
	if err != nil {
		return 0, nil, err
	}
	total := 0
	for _, n := range info.State.Subjects {
		total += int(n)
	}
	if total == 0 {
		return 0, nil, nil
	}
	c, err := p.JS.OrderedConsumer(ctx, bus.DLQStream, jetstream.OrderedConsumerConfig{FilterSubjects: []string{DLQSubject(p.Name)}})
	if err != nil {
		return 0, nil, err
	}
	for seen := 0; seen < total; {
		batch, err := c.Fetch(100, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			return applied, failed, err
		}
		got := 0
		for m := range batch.Messages() {
			got++
			seq, _ := strconv.ParseUint(m.Headers().Get(HeaderDLQSeq), 10, 64)
			ev, err := events.Decode(m.Data())
			if err != nil {
				failed = append(failed, fmt.Sprintf("stream seq %d: %v", seq, err))
				continue
			}
			ls, _ := strconv.ParseInt(m.Headers().Get(events.HeaderLedgerSeq), 10, 64)
			it := item{md: &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: seq}}, ev: ev, ledgerSeq: ls}
			ok, err := p.apply(ctx, []item{it})
			switch {
			case err != nil:
				failed = append(failed, fmt.Sprintf("%s: %v", ev.ID, err))
			case ok[0]:
				applied++
			}
		}
		if got == 0 {
			break
		}
		seen += got
	}
	return applied, failed, nil
}
