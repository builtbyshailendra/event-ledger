// Package crash makes a process die at a named point, like kill -9: no
// deferred cleanup, no graceful shutdown. Open database transactions are
// rolled back by PostgreSQL when the connection drops; anything already sent
// to NATS stays sent.
//
// Set LEDGER_CRASH=point:n to exit with status 137 the n-th time the point
// is reached, for example LEDGER_CRASH=relay.after-publish:3.
package crash

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

// ExitCode is what a crashed process exits with (128 + SIGKILL).
const ExitCode = 137

var (
	target string
	at     int64 = 1
	hits   atomic.Int64
)

func init() {
	spec := os.Getenv("LEDGER_CRASH")
	if spec == "" {
		return
	}
	target = spec
	if i := strings.LastIndexByte(spec, ':'); i > 0 {
		if n, err := strconv.ParseInt(spec[i+1:], 10, 64); err == nil {
			target, at = spec[:i], n
		}
	}
}

// Point exits the process if it is the configured crash point and has been
// reached the configured number of times.
func Point(name string) {
	if target == "" || name != target {
		return
	}
	if hits.Add(1) == at {
		fmt.Fprintf(os.Stderr, "CRASH at %s (hit %d)\n", name, at)
		os.Exit(ExitCode)
	}
}
