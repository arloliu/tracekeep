package core

import (
	"fmt"
	"os"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// benchScaleEnv names the largest size the benchmarks run: small (the default when unset), medium or large.
// The service's internal/benchscale reads the same variable; the SDK, its own module, cannot import it.
const benchScaleEnv = "TRACEKEEP_BENCH_SCALE"

// benchSizes returns the sizes to run, smallest first: small, then medium when benchScaleEnv is medium or large,
// then large when it is large. Any other value fails tb.
func benchSizes[T any](tb testing.TB, small, medium, large T) []T {
	tb.Helper()
	switch v := os.Getenv(benchScaleEnv); v {
	case "", "small":
		return []T{small}
	case "medium":
		return []T{small, medium}
	case "large":
		return []T{small, medium, large}
	default:
		tb.Fatalf("%s=%q: want small, medium or large", benchScaleEnv, v)

		return nil
	}
}

// BenchmarkObserve measures the observer callback's path in the recorder, a data record and a transport event apart.
// Its starting state is a recorder with an open capture and an empty ordinary lane.
// An iteration observes one record, its wall origin plus its mono so no clock step is recorded,
// and drains it from the ordinary lane, returning its budget, so every iteration starts from the empty lane.
// It asserts each iteration that the record was admitted and dequeued, with the next seq and its payload or event:
// a dropped record leaves the lane empty, so the dequeue is also the iteration's check that nothing was dropped.
// After the loop no drop is counted and the lane's budget is whole.
// Its sizes are the data frame's payload, 14 B, 1 KiB and 64 KiB (TRACEKEEP_BENCH_SCALE); an event has no payload.
func BenchmarkObserve(b *testing.B) {
	for _, n := range benchSizes(b, 14, 1<<10, 64<<10) {
		b.Run(fmt.Sprintf("kind=data/payload=%d", n), func(b *testing.B) {
			benchObserve(b, frameObs(0, make([]byte, n)))
		})
	}
	b.Run("kind=event", func(b *testing.B) {
		benchObserve(b, &Observation{
			Socket: 1, Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal, Fidelity: tracepack.FidelityNotApplicable,
			Event: Event{Kind: tracepack.EventSocketClose, Cause: tracepack.CausePeerClose, CauseRaw: "go-secs:CauseIOError"},
		})
	})
}

// benchObserve runs BenchmarkObserve's loop, observing o at each iteration's clock reading
// and checking that each dequeued entry carries o's kind, payload length and event.
func benchObserve(b *testing.B, o *Observation) {
	r, _ := newTestRecorder(b, RecorderParams{OrdinaryRecords: 4096})
	var mono int64
	seq := uint64(1) // newTestRecorder's start holds seq 0
	b.ReportAllocs()
	for b.Loop() {
		mono++
		o.Wall, o.Mono = origin+mono, mono
		r.Observe(o)
		select {
		case e := <-r.Ordinary():
			r.ReleaseOrdinary(&e)
			if e.Kind != EntryObserved || e.Seq != seq || e.RecKind != o.Kind || len(e.Payload) != len(o.Payload) || e.Event != o.Event {
				b.Fatalf("dequeued entry kind %d seq %d record kind %d, %d bytes; want record at seq %d", e.Kind, e.Seq, e.RecKind, len(e.Payload), seq)
			}
		default:
			b.Fatalf("record at seq %d was not admitted", seq)
		}
		seq++
	}
	assertNoDrop(b, r)
}

// assertNoDrop checks that r counted no drop and that its ordinary lane's budget is whole.
func assertNoDrop(tb testing.TB, r *Recorder) {
	tb.Helper()
	if c := r.Counters(); c.Dropped != [NumDropReasons]uint64{} {
		tb.Fatalf("dropped %v, want none", c.Dropped)
	}
	if count, bytes := r.OrdinaryDepth(); count != 0 || bytes != 0 {
		tb.Fatalf("ordinary lane holds %d records, %d bytes after the loop, want 0", count, bytes)
	}
}
