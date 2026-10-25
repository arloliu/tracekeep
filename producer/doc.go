// Package producer is the producer SDK an equipment gateway links to publish its SECS/HSMS traffic records on NATS JetStream.
//
// The design is in docs/specs/tracekeep/producer-sdk.md; the package holds no code yet.
package producer

import (
	// Keeps the SDK's pinned requirements in go.mod until the first code imports them.
	_ "github.com/arloliu/go-secs/tracepack"
	_ "github.com/nats-io/nats.go"
)
