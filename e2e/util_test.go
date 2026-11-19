package e2e

import (
	"encoding/json"

	"github.com/nats-io/nats.go/jetstream"
)

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

func jetstreamMsgID(id string) jetstream.PublishOpt { return jetstream.WithMsgID(id) }
