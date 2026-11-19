// Package check compares what the ledger committed with what reached the
// stream and the projections.
package check

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/builtbyshailendra/event-ledger/bus"
	"github.com/builtbyshailendra/event-ledger/events"
)

// Msg is one message read back from the stream.
type Msg struct {
	StreamSeq uint64
	LedgerSeq int64
	Event     events.Event
	Bad       bool // payload didn't decode
}

// ReadStream reads every message in a stream, in stream order.
func ReadStream(ctx context.Context, js jetstream.JetStream, stream string) ([]Msg, error) {
	s, err := js.Stream(ctx, stream)
	if err != nil {
		return nil, err
	}
	info, err := s.Info(ctx)
	if err != nil {
		return nil, err
	}
	last := info.State.LastSeq
	if info.State.Msgs == 0 {
		return nil, nil
	}
	c, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{})
	if err != nil {
		return nil, err
	}
	var out []Msg
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		batch, err := c.Fetch(1000, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			return nil, err
		}
		for m := range batch.Messages() {
			md, err := m.Metadata()
			if err != nil {
				return nil, err
			}
			x := Msg{StreamSeq: md.Sequence.Stream}
			x.LedgerSeq, _ = strconv.ParseInt(m.Headers().Get(events.HeaderLedgerSeq), 10, 64)
			if x.Event, err = events.Decode(m.Data()); err != nil {
				x.Bad = true
			}
			out = append(out, x)
			if md.Sequence.Stream >= last {
				return out, nil
			}
		}
		if batch.Error() != nil {
			return nil, batch.Error()
		}
	}
	return out, fmt.Errorf("read %d messages, stream ends at %d", len(out), last)
}

// Report compares the committed commands with the stream.
type Report struct {
	Committed   int      // commands in ledger.commands
	Messages    int      // messages in the stream
	Missing     []string // committed, never in the stream: a change no one heard about
	Phantom     []string // in the stream, never committed: an event about nothing
	Duplicates  int      // extra copies of an event in the stream
	Unpublished int      // outbox rows not yet marked published
	VersionGaps []string // accounts whose versions in the stream aren't 1..n
}

// OK says whether the stream holds each committed event exactly once.
func (r Report) OK() bool {
	return len(r.Missing) == 0 && len(r.Phantom) == 0 && r.Duplicates == 0 && len(r.VersionGaps) == 0
}

func (r Report) String() string {
	return fmt.Sprintf("committed %d, messages %d, missing %d %v, phantom %d %v, duplicates %d, unpublished %d, version gaps %v",
		r.Committed, r.Messages, len(r.Missing), head(r.Missing), len(r.Phantom), head(r.Phantom), r.Duplicates, r.Unpublished, head(r.VersionGaps))
}

func head(s []string) []string {
	if len(s) > 5 {
		return append(s[:5:5], "...")
	}
	return s
}

// Stream builds a Report.
func Stream(ctx context.Context, pool *pgxpool.Pool, js jetstream.JetStream) (Report, error) {
	var r Report
	committed := map[string]bool{}
	rows, err := pool.Query(ctx, `SELECT id FROM ledger.commands`)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return r, err
		}
		committed[id] = true
	}
	if err := rows.Err(); err != nil {
		return r, err
	}
	r.Committed = len(committed)
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ledger.outbox WHERE published_at IS NULL`).Scan(&r.Unpublished); err != nil {
		return r, err
	}

	msgs, err := ReadStream(ctx, js, bus.Stream)
	if err != nil {
		return r, err
	}
	r.Messages = len(msgs)
	seen := map[string]int{}
	versions := map[string][]int64{}
	for _, m := range msgs {
		if m.Bad {
			continue
		}
		id := m.Event.ID
		seen[id]++
		if seen[id] > 1 {
			r.Duplicates++
			continue
		}
		if !committed[id] {
			r.Phantom = append(r.Phantom, id)
		}
		for acct, v := range m.Event.Versions {
			versions[acct] = append(versions[acct], v)
		}
	}
	for id := range committed {
		if seen[id] == 0 {
			r.Missing = append(r.Missing, id)
		}
	}
	sort.Strings(r.Missing)
	for acct, vs := range versions {
		sort.Slice(vs, func(i, j int) bool { return vs[i] < vs[j] })
		for i, v := range vs {
			if v != int64(i+1) {
				r.VersionGaps = append(r.VersionGaps, acct)
				break
			}
		}
	}
	sort.Strings(r.VersionGaps)
	return r, nil
}

// Projection compares a consumer's balances with the write side.
func Projection(ctx context.Context, pool *pgxpool.Pool, consumer string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT coalesce(a.id, b.account), coalesce(a.balance, -1), coalesce(b.balance, -1),
		       coalesce(a.version, -1), coalesce(b.events, -1)
		FROM ledger.accounts a
		FULL JOIN (SELECT * FROM views.balances WHERE consumer = $1) b ON b.account = a.id
		WHERE a.balance IS DISTINCT FROM b.balance OR a.version IS DISTINCT FROM b.events
		ORDER BY 1`, consumer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var diffs []string
	for rows.Next() {
		var acct string
		var want, got, wantV, gotV int64
		if err := rows.Scan(&acct, &want, &got, &wantV, &gotV); err != nil {
			return nil, err
		}
		diffs = append(diffs, fmt.Sprintf("%s: ledger %d (%d events), %s %d (%d events)", acct, want, wantV, consumer, got, gotV))
	}
	return diffs, rows.Err()
}

// Summary joins problems for an error message.
func Summary(diffs []string) string { return strings.Join(head(diffs), "; ") }
