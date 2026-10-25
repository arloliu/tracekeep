// Package link lets the module's own packages reach a producer's recorder,
// which no integrator can: secsobs feeds it, and the SDK's tests drive it.
package link

import "github.com/arloliu/tracekeep/producer/internal/core"

// Recorder returns the recorder of a *producer.Producer; package producer sets it.
var Recorder func(p any) *core.Recorder
