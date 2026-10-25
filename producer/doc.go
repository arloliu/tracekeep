// Package producer is the producer SDK an equipment gateway links to publish its SECS/HSMS traffic records on NATS JetStream.
//
// The design is in docs/specs/tracekeep/producer.md and producer-sdk.md.
package producer

import (
	// Keeps the SDK's pinned nats.go requirement in go.mod until the first code imports it.
	_ "github.com/nats-io/nats.go"
)
