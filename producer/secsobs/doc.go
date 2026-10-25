// Package secsobs turns the go-secs observer events of one HSMS-SS connection into a producer's records
// (docs/specs/tracekeep/producer-sdk.md §4.7 *Observers*, go-secs tracepack-go.md §5).
//
// An integrator builds one Observer per producer and passes its ConnOptions to the connection it builds
// (through hsmsss.WithConnectionOption) before the connection opens;
// the package exports no other way to feed a record.
// The callbacks feed the capture algorithm directly:
// a wire frame is copied only once the ordinary lane admitted it, and a transport event allocates nothing.
package secsobs
