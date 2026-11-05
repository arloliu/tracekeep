package ingest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// stageRecorder counts the stages a Timed seam is told of, by name.
type stageRecorder struct {
	mu sync.Mutex
	n  map[string]int
}

func (s *stageRecorder) timed(stage string, _ time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n[stage]++
}

func (s *stageRecorder) count(stage string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.n[stage]
}

// The load harness's seams on a rejected capture's release: the traffic consumer's parking of the originals and the parked consumer's
// copy rows, disposition, admission, flush and three acknowledgement steps are timed apart, the parked ones named parked/<stage>,
// and the parked consumer's Stepping is called as each step begins.
func TestParkedStagesTimedApart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		rec := &stageRecorder{n: map[string]int{}}
		r.g.Timed, r.g.Executor.Timed, r.g.Flusher.Timed, r.g.Coordinator.Timed = rec.timed, rec.timed, rec.timed, rec.timed
		capture := tracepack.UUID{55}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, " ")), nil) // no device name: refused
		r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
		r.step(t)
		if vs := versions(t, c, capture); len(vs) != 2 {
			t.Fatalf("versions %+v, want the start and the record parked", vs)
		}

		pg := r.parkedConsumer(t)
		pg.Timed = rec.timed
		var steps atomic.Int32
		pg.Stepping = func(context.Context) { steps.Add(1) }
		r.g.Executor.Accepts = func([]byte) bool { return true } // the fixed release
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		r.clk.flushInterval()
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if got := copyStates(versions(t, c, capture), pg.Incarnation.ID); !slices.Equal(got, []string{"acked", "acked"}) {
			t.Fatalf("copy rows %v, want both acked", got)
		}
		for stage, want := range map[string]int{
			// The originals, parked by the traffic consumer, each acknowledged in three steps.
			"dispose": 2, "prepare-ack": 2, "double-ack": 2, "ack-copy": 2,
			// The copies: two rows, two dispositions, the start's admission, one batch flushed and acknowledged.
			"parked/copy": 2, "parked/dispose": 2, "parked/admit": 1, "parked/batch": 2, "parked/flush": 1, "parked/write": 1,
			"parked/put": 1, "parked/register": 1, "parked/acknowledge": 1,
			"parked/prepare-ack": 2, "parked/double-ack": 2, "parked/ack-copy": 2,
			"admit": 0, "write": 0, "parked/ack-copy/failed": 0,
		} {
			if got := rec.count(stage); got != want {
				t.Errorf("stage %s timed %d times, want %d", stage, got, want)
			}
		}
		if n := steps.Load(); n != 2 {
			t.Errorf("Stepping called %d times, want once a step", n)
		}
	})
}

// A stage is named apart for a parked copy and for a failure.
func TestStageName(t *testing.T) {
	t.Parallel()
	failed := errors.New("failed")
	for _, tc := range []struct {
		parked bool
		err    error
		want   string
	}{{false, nil, "ack-copy"}, {true, nil, "parked/ack-copy"}, {false, failed, "ack-copy/failed"}, {true, failed, "parked/ack-copy/failed"}} {
		if got := stageName(tc.parked, "ack-copy", tc.err); got != tc.want {
			t.Errorf("stageName(%v, ack-copy, %v) = %q, want %q", tc.parked, tc.err, got, tc.want)
		}
	}
}
