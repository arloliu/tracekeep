// Package logfeed converts the host log feed (docs/specs/tracekeep/logfeed.md §15): entries a log pipeline parsed from a host program's SML text log,
// one JSON object a message, become reconstructed data records and annotations of log captures.
//
// This part holds no delivery, catalog or bus: decoding an entry and reading its time (§15.3, §15.5 *Time*),
// a file's reorder window with its duplicates and stale entries, the device rules, pairing a header with its body,
// and building the records tracepack's SegmentWriter.Append takes (§15.5 *What becomes what*).
// The converter that drives them under a lease group, batches, flushes and registers is built on it.
package logfeed
