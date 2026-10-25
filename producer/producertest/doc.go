// Package producertest provides what testing code that links the producer SDK needs:
// an in-memory bus, the chaos scenarios an integrator's device runs against it,
// a fake response waiter on a clock the test moves,
// and the conformance check a gateway's own waiter runs.
//
// The design is in docs/specs/tracekeep/producer-sdk.md §4.7.
package producertest
