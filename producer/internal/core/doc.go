// Package core holds the producer's capture algorithm, which the producer and its observers share:
// the capture object, seq and clock allocation, the lanes and the observer callback
// (docs/specs/tracekeep/producer.md §4.2).
//
// It is internal so that no integrator can feed records by any route but the observers.
package core
