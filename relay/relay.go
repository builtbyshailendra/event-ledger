// Package relay moves events from the PostgreSQL outbox to JetStream.
//
// It is at-least-once by construction: it publishes, then marks rows as
// published. A crash between the two publishes those rows again on restart.
// Each message carries the event ID as its JetStream message ID, so the
// stream drops copies that arrive within its deduplication window; consumers
// must still tolerate duplicates, because the window ends.
package relay

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/builtbyshailendra/event-ledger/crash"
	"github.com/builtbyshailendra/event-ledger/events"
)

// lockKey is the advisory lock that elects the one active relay.
const lockKey = 7_100_001

// Relay publishes outbox rows.
type Relay struct {
	Pool  *pgxpool.Pool
	JS    jetstream.JetStream
	Batch int           // rows per round, default 200
	Poll  time.Duration // longest wait between rounds without a notification, default 1s
	// NoListen turns off LISTEN/NOTIFY, leaving only polling.
	NoListen bool
	// Cursor plants a classic bug: remember the highest id published and
	// ask for "id > last" next time. Ids are assigned at insert, not at
	// commit, so a transaction that commits late is skipped forever.
	Cursor bool
	Log    func(format string, args ...any)

	Published  atomic.Int64 // messages the stream accepted
	Duplicates atomic.Int64 // messages the stream reported as duplicates
}

func (r *Relay) logf(format string, args ...any) {
	if r.Log != nil {
		r.Log(format, args...)
	}
}

// Run waits to become the active relay, then publishes until ctx ends or the
// database connection fails (then it returns, and should be restarted).
func (r *Relay) Run(ctx context.Context) error {
	if r.Batch == 0 {
		r.Batch = 200
	}
	if r.Poll == 0 {
		r.Poll = time.Second
	}
	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Leader election: a session-level advisory lock. If this process dies,
	// its connection closes and PostgreSQL releases the lock, so a standby
	// takes over. The lock is not a fence: a paused relay may still publish
	// after losing it, which deduplication makes harmless.
	for {
		var ok bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&ok); err != nil {
			return err
		}
		if ok {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	r.logf("relay: active")
	if !r.NoListen {
		if _, err := conn.Exec(ctx, `LISTEN ledger_outbox`); err != nil {
			return err
		}
	}

	var last int64
	backoff := 50 * time.Millisecond
	for {
		n, err := r.round(ctx, &last)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil && errors.Is(err, errPublish):
			// NATS is unreachable or slow: keep the rows, try again later.
			r.logf("relay: %v; retrying in %v", err, backoff)
			time.Sleep(backoff)
			backoff = min(backoff*2, 2*time.Second)
			continue
		case err != nil:
			return err
		}
		backoff = 50 * time.Millisecond
		if n == r.Batch {
			continue // more waiting
		}
		if r.NoListen {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.Poll):
			}
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, r.Poll)
		_, err = conn.Conn().WaitForNotification(wctx)
		cancel()
		if err != nil && ctx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("relay: lost the lock connection: %w", err)
		}
	}
}

var errPublish = errors.New("publish failed")

type row struct {
	id      int64
	eventID string
	subject string
	payload []byte
}

// round publishes one batch and marks what the stream acknowledged.
func (r *Relay) round(ctx context.Context, last *int64) (int, error) {
	query := `SELECT id, event_id, subject, payload FROM ledger.outbox WHERE published_at IS NULL ORDER BY id LIMIT $1`
	args := []any{r.Batch}
	if r.Cursor {
		query = `SELECT id, event_id, subject, payload FROM ledger.outbox WHERE id > $2 ORDER BY id LIMIT $1`
		args = append(args, *last)
	}
	rs, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	var batch []row
	for rs.Next() {
		var x row
		if err := rs.Scan(&x.id, &x.eventID, &x.subject, &x.payload); err != nil {
			return 0, err
		}
		batch = append(batch, x)
	}
	if err := rs.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}

	crash.Point("relay.before-publish")
	futures := make([]jetstream.PubAckFuture, 0, len(batch))
	for i, x := range batch {
		if i == len(batch)/2 {
			crash.Point("relay.mid-publish") // half the batch is out
		}
		msg := nats.NewMsg(x.subject)
		msg.Data = x.payload
		msg.Header.Set(events.HeaderLedgerSeq, strconv.FormatInt(x.id, 10))
		f, err := r.JS.PublishMsgAsync(msg, jetstream.WithMsgID(x.eventID))
		if err != nil {
			break
		}
		futures = append(futures, f)
	}

	// Mark only what the stream acknowledged. A row whose publish failed
	// stays unpublished and goes out again next round.
	var done []int64
	var failed error
	dups := 0
	timeout := time.After(5 * time.Second)
	for i, f := range futures {
		select {
		case ack := <-f.Ok():
			done = append(done, batch[i].id)
			if ack.Duplicate {
				r.Duplicates.Add(1)
				dups++
			} else {
				r.Published.Add(1)
			}
		case err := <-f.Err():
			failed = err
		case <-timeout:
			failed = fmt.Errorf("timed out waiting for acks")
		}
		if failed != nil {
			break
		}
	}
	crash.Point("relay.after-publish") // in the stream, not yet marked
	if dups > 0 {
		r.logf("relay: stream dropped %d duplicates", dups)
	}

	if len(done) > 0 {
		if _, err := r.Pool.Exec(ctx, `UPDATE ledger.outbox SET published_at = clock_timestamp() WHERE id = ANY($1)`, done); err != nil {
			return 0, err
		}
		*last = max(*last, done[len(done)-1])
	}
	crash.Point("relay.after-mark")
	if failed != nil || len(futures) < len(batch) {
		return len(done), fmt.Errorf("%w: %d of %d acknowledged: %v", errPublish, len(done), len(batch), failed)
	}
	return len(done), nil
}
