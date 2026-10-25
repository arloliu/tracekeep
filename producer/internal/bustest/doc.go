// Package bustest tests the producer SDK against a real JetStream:
// the embedded nats-server of internal/testbus, which the SDK module itself must not import.
// It is a module of its own, never tagged or required, run with GOWORK=off (docs/specs/tracekeep/tracekeep-decisions.md T-35).
package bustest
