package core

import (
	"sync/atomic"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

func BenchmarkAllocate(b *testing.B) {
	var gen atomic.Uint64
	c := NewCapture(CaptureParams{ID: tracepack.UUID{1}, Lifecycle: &gen, Tolerance: eps, ReservedCap: 8, OriginWall: origin})
	var mono int64
	b.ReportAllocs()
	for b.Loop() {
		mono++
		c.Allocate(origin+mono, mono, 1)
	}
}
