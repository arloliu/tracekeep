package secsobs

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

// BenchmarkCallback measures the wire callback, the callback a frame costs go-secs's receive goroutine,
// with 1 and 4 callbacks running at once: its budget in 650-perf-sec.md.
// Its starting state is a recorder with an open capture and an empty ordinary lane, no publisher draining it.
// An iteration delivers one inbound S1F1 frame, stamped before the loop as go-secs stamps it before the callback,
// and dequeues one record from the ordinary lane, returning its budget, so the lane holds at most one record a callback.
// It asserts each iteration that a record was admitted and dequeued as a data record of the frame's length:
// a dropped record leaves the lane short, so the dequeue is also the iteration's check that nothing was dropped.
// After the loop no drop is counted and the lane's budget is whole.
// It runs b.RunParallel with GOMAXPROCS set to the callbacks for the sub-benchmark, so that many goroutines run the callback at once.
func BenchmarkCallback(b *testing.B) {
	for _, callbacks := range []int{1, 4} {
		b.Run(fmt.Sprintf("callbacks=%d", callbacks), func(b *testing.B) {
			defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(callbacks))
			r := core.NewRecorder(core.RecorderParams{ReservedCap: 8, OrdinaryRecords: 4096, OrdinaryBytes: 1 << 20})
			var gen atomic.Uint64
			at := time.Now()
			c := core.NewCapture(core.CaptureParams{
				ID: tracepack.UUID{1}, Lifecycle: &gen, Tolerance: int64(time.Second), ReservedCap: 8,
				OriginWall: at.UnixNano(), OriginMono: core.Mono(at),
			})
			if res := r.Rotate(c, time.Now().Add(time.Second), func() bool { return false }, core.Now); !res.Installed {
				b.Fatalf("Rotate: %+v", res)
			}
			<-r.Reserved()
			obs := &Observer{rec: r}
			f := frame(0, 0x01, 0x00)
			ev := hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, At: at, Frame: f}

			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					obs.wire(ev)
					select {
					case e := <-r.Ordinary():
						r.ReleaseOrdinary(&e)
						if e.Kind != core.EntryObserved || e.RecKind != tracepack.KindData || len(e.Payload) != len(f) {
							b.Errorf("dequeued entry kind %d record kind %d, %d bytes; want a %d-byte data record", e.Kind, e.RecKind, len(e.Payload), len(f))
							return
						}
					default:
						b.Error("a callback's record was not admitted")
						return
					}
				}
			})
			if cs := r.Counters(); cs.Dropped != [core.NumDropReasons]uint64{} {
				b.Fatalf("dropped %v, want none", cs.Dropped)
			}
			if count, bytes := r.OrdinaryDepth(); count != 0 || bytes != 0 {
				b.Fatalf("ordinary lane holds %d records, %d bytes after the loop, want 0", count, bytes)
			}
		})
	}
}
