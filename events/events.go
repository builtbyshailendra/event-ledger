// Package events defines what the ledger publishes.
package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// Type is the kind of event.
type Type string

const (
	AccountOpened Type = "account.opened"
	Deposited     Type = "deposited"
	Transferred   Type = "transferred"
)

// World is the external account deposits come from, so every movement is a
// balanced double entry.
const World = "@world"

// Event is one fact the ledger committed. ID is unique (the command's
// idempotency key) and doubles as the JetStream message ID.
type Event struct {
	ID       string           `json:"id"`
	Type     Type             `json:"type"`
	At       time.Time        `json:"at"`
	Account  string           `json:"account,omitempty"` // opened, deposited
	From     string           `json:"from,omitempty"`    // transferred
	To       string           `json:"to,omitempty"`
	Amount   int64            `json:"amount,omitempty"`
	Versions map[string]int64 `json:"versions"` // account -> its version after this event
}

// Subject is where the event is published.
func (e Event) Subject() string { return "ledger.events." + string(e.Type) }

// Changes lists the balance change per account (deposits from World are not
// counted as a ledger account).
func (e Event) Changes() map[string]int64 {
	switch e.Type {
	case AccountOpened:
		return map[string]int64{e.Account: 0}
	case Deposited:
		return map[string]int64{e.Account: e.Amount}
	case Transferred:
		return map[string]int64{e.From: -e.Amount, e.To: e.Amount}
	}
	return nil
}

// Decode parses and validates an event. An error here is permanent: no
// number of retries will make the message valid.
func Decode(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return e, fmt.Errorf("decode: %w", err)
	}
	if e.ID == "" {
		return e, fmt.Errorf("event without id")
	}
	switch e.Type {
	case AccountOpened, Deposited, Transferred:
	default:
		return e, fmt.Errorf("unknown event type %q", e.Type)
	}
	if e.Type != AccountOpened && e.Amount <= 0 {
		return e, fmt.Errorf("event %s: amount %d", e.ID, e.Amount)
	}
	return e, nil
}

// Header names the relay sets.
const (
	HeaderLedgerSeq = "Ledger-Seq" // the outbox id: the order the ledger committed in
)
