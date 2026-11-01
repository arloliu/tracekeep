// Package link lets the module's own packages reach a producer's recorder,
// which no integrator can: secsobs feeds it, and the SDK's tests drive it.
package link

import (
	"github.com/nats-io/nats.go"

	"github.com/arloliu/tracekeep/producer/internal/core"
)

// Recorder returns the recorder of a *producer.Producer; package producer sets it.
var Recorder func(p any) *core.Recorder

// Bus returns the default bus over nc as a producer.Bus, which the bus tests wrap; package producer sets it.
var Bus func(nc *nats.Conn) (any, error)
