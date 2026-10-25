package core

import "time"

// monoBase is the process's monotonic reference: every mono reading the SDK takes is measured from it.
var monoBase = time.Now()

// Mono returns t's monotonic reading in nanoseconds since monoBase;
// for a t without one it falls back to the wall clock, as time.Time.Sub does.
func Mono(t time.Time) int64 { return int64(t.Sub(monoBase)) }

// Now returns the wall clock and the monotonic reading of the present instant, from one clock read.
func Now() (wall, mono int64) {
	n := time.Now()
	return n.UnixNano(), Mono(n)
}
