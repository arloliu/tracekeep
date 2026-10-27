package compact

import (
	"bytes"
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
)

// described admits a fresh capture of the recorder instance at origin with a real descriptor, as ingest's admission of its start would.
func (f *fixture) described(t *testing.T, instance tracepack.UUID, origin int64) tracepack.UUID {
	t.Helper()
	ctx := t.Context()
	cfg, err := f.c.ActivateConfig(ctx, []byte("config"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.Register(ctx, "r", []int64{cfg}, time.Hour); err != nil {
		t.Fatal(err)
	}
	name := "eqp"
	mono, tolerance := int64(0), uint64(time.Second)
	m := &tracepack.PackMeta{
		ToolID: tool, Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodRawStream,
		Vantage: tracepack.VantageHost, Recorder: "test", TimeSource: tracepack.TimeSourceCaptureClock,
		CaptureOriginUTCNs: &origin, CaptureOriginMonoNs: &mono, ClockStepToleranceNs: &tolerance,
		LifecycleCoverage: tracepack.LifecycleCoverageSubscribed, QualityEvaluated: true,
		RecorderInstanceID: instance, RecorderInstanceName: &name,
	}
	b, err := tracepack.EncodeDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}
	id := newID(t)
	d := &catalog.Identity{Version: catalog.VersionKey{Capture: id}, Incarnation: 1, StreamSeq: 1, Hour: hour}
	desc := &catalog.Descriptor{Bytes: b, ToolID: tool, RecorderInstanceID: instance, Device: name, OriginNS: origin}
	if _, err := f.c.Admit(ctx, d, desc); err != nil {
		t.Fatal(err)
	}

	return id
}

// closing readies f's compactor for the close pass at now.
func (f *fixture) closing(t *testing.T, now time.Time) {
	t.Helper()
	f.merging(t)
	f.r.Inactivity, f.r.FlushInterval = 2*time.Hour, 5*time.Minute
	f.r.Now = func() time.Time { return now }
}

func (f *fixture) closes(t *testing.T) CloseReport {
	t.Helper()
	rep, err := f.r.Closes(t.Context(), f.lease)
	if err != nil {
		t.Fatal(err)
	}

	return rep
}

// closeObject reads the close segment of capture from the store: its key's bytes and its one record.
func (f *fixture) closeObject(t *testing.T, capture tracepack.UUID) ([]byte, tracepack.Record) {
	t.Helper()
	var keys []string
	err := f.s.List(t.Context(), "staging/", func(info store.ObjectInfo) error {
		if strings.Contains(info.Key, capture.String()) && strings.Contains(info.Key, "9223372036854775807-") {
			keys = append(keys, info.Key)
		}

		return nil
	})
	if err != nil || len(keys) != 1 {
		t.Fatalf("close objects = %v, %v; want one", keys, err)
	}
	obj, err := f.s.Open(t.Context(), keys[0])
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	data := make([]byte, obj.Size())
	if _, err := obj.ReadAt(data, 0); err != nil {
		t.Fatal(err)
	}
	rd, err := tracepack.Open(t.Context(), bytes.NewReader(data), int64(len(data)), tracepack.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var recs []tracepack.Record
	_, err = rd.Iterate(t.Context(), tracepack.Query{Payloads: true}, func(it *tracepack.Item) error {
		recs = append(recs, it.Record)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("the close holds %d records, want 1", len(recs))
	}

	return data, recs[0]
}

// A capture silent for I is closed by inactivity: its intent, then its close written, put and published;
// the close's one record is the stop-unclean boundary at the last seq, at the compactor's clock, and the highest seq stays.
func TestCloseByInactivity(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		now := time.Now().Add(3 * time.Hour).Truncate(time.Millisecond)
		f.closing(t, now)
		capture := f.described(t, newID(t), now.Add(-4*time.Hour).UnixNano())
		f.stage(t, segment(t, capture, []uint64{0, 1, 2}, false))
		rep := f.closes(t)
		if rep.Intended[catalog.CloseInactivity] != 1 || rep.Published != 1 || rep.Failed+rep.Abandoned != 0 {
			t.Fatalf("Closes = %+v, want one closed by inactivity and published", rep)
		}
		got, err := c.Capture(t.Context(), capture)
		if err != nil || got.State != catalog.StateClosed || got.HighestSeq != 2 {
			t.Errorf("capture = %+v, %v; want closed, highest seq 2", got, err)
		}
		_, rec := f.closeObject(t, capture)
		ev, err := tracepack.UnmarshalTransportEvent(rec.Payload)
		if err != nil || rec.Seq != math.MaxInt64 || rec.TSUTCNs != now.UnixNano() || ev.BoundaryKind == nil || *ev.BoundaryKind != tracepack.BoundaryKindStopUnclean {
			t.Errorf("the close record = %+v, %+v, %v", rec, ev, err)
		}
		if pending, err := c.PendingCloses(t.Context()); err != nil || len(pending) != 0 {
			t.Errorf("pending closes = %+v, %v; want none", pending, err)
		}
		if rep := f.closes(t); len(rep.Intended)+rep.Published != 0 {
			t.Errorf("a second pass = %+v, want nothing", rep)
		}
	})
}

// A capture whose successor registered at least Q ago, and that registered nothing since, is closed at the successor's origin.
func TestCloseBySuccessor(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		now := time.Now().Add(20 * time.Minute)
		f.closing(t, now)
		instance := newID(t)
		originNs := time.Now().Add(-time.Hour).UnixNano()
		old := f.described(t, instance, originNs)
		f.stage(t, segment(t, old, []uint64{0, 1}, false))
		next := f.described(t, instance, originNs+int64(time.Minute))
		f.stage(t, segment(t, next, []uint64{0}, false))
		if rep := f.closes(t); rep.Intended[catalog.CloseSuccessor] != 1 || rep.Published != 1 {
			t.Fatalf("Closes = %+v, want one closed by successor", rep)
		}
		if _, rec := f.closeObject(t, old); rec.TSUTCNs != originNs+int64(time.Minute) {
			t.Errorf("the close's time = %d, want the successor's origin %d", rec.TSUTCNs, originNs+int64(time.Minute))
		}
		if got, err := c.Capture(t.Context(), next); err != nil || got.State != catalog.StateOpen {
			t.Errorf("the successor = %+v, %v; want open", got, err)
		}
	})
}

// A crash after the put: the next pass writes the close again to the same bytes and publishes it;
// a put whose result was lost with the object stored whole publishes too.
func TestCloseRetriedToTheSameBytes(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		now := time.Now().Add(3 * time.Hour)
		f.closing(t, now)
		capture := f.described(t, newID(t), now.Add(-4*time.Hour).UnixNano())
		ctx, crash := context.WithCancel(t.Context())
		f.r.hooks[hookClosePut] = func() { crash() }
		if _, err := f.r.Closes(ctx, f.lease); !errors.Is(err, context.Canceled) {
			t.Fatalf("Closes = %v, want the crash", err)
		}
		f.r.hooks[hookClosePut] = nil
		first, _ := f.closeObject(t, capture)
		// The clock moved on; the close is written from its intent alone.
		f.r.Now = func() time.Time { return now.Add(time.Hour) }
		f.r.Store = failingPut{Mem: f.s, visible: true}
		if rep := f.closes(t); rep.Published != 1 {
			t.Fatalf("the retry = %+v, want published", rep)
		}
		f.r.Store = f.s
		if again, _ := f.closeObject(t, capture); !bytes.Equal(again, first) {
			t.Error("the retried close's bytes differ")
		}
	})
}

// A close the horizon refuses is abandoned, its object deleted, and never retried.
func TestCloseAbandonedBeyondTheHorizon(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		now := time.Now().Add(3 * time.Hour)
		f.closing(t, now)
		capture := f.described(t, newID(t), now.Add(-4*time.Hour).UnixNano())
		f.horizon = floorHour(now.UnixNano()) + 1
		if rep := f.closes(t); rep.Abandoned != 1 {
			t.Fatalf("Closes = %+v, want abandoned", rep)
		}
		var left []string
		_ = f.s.List(t.Context(), "staging/", func(info store.ObjectInfo) error { left = append(left, info.Key); return nil })
		if len(left) != 0 {
			t.Errorf("objects = %v, want the abandoned close deleted", left)
		}
		if got, _ := c.Capture(t.Context(), capture); got.State != catalog.StateClosed {
			t.Errorf("capture = %+v, want closed", got)
		}
		if rep := f.closes(t); rep.Published+rep.Abandoned+rep.Failed != 0 {
			t.Errorf("a later pass = %+v, want nothing retried", rep)
		}
	})
}

// capture close: a request the next cycle executes as an intent with the operator's trigger, which ends it; the close pass publishes.
func TestCloseRequest(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		now := time.Now()
		f.closing(t, now)
		capture := f.described(t, newID(t), now.UnixNano())
		f.stage(t, segment(t, capture, []uint64{0}, false))
		id, err := c.RequestCompaction(t.Context(), catalog.RequestClose, capture, nil, "op")
		if err != nil {
			t.Fatal(err)
		}
		stopped := f.described(t, newID(t), now.UnixNano())
		f.stage(t, segment(t, stopped, []uint64{0, 1}, true))
		refused, err := c.RequestCompaction(t.Context(), catalog.RequestClose, stopped, nil, "op")
		if err != nil {
			t.Fatal(err)
		}
		if rep, err := f.r.Requests(t.Context(), f.lease); err != nil || rep.Done != 1 || rep.Failed != 1 {
			t.Fatalf("Requests = %+v, %v; want one done, one failed", rep, err)
		}
		for id, want := range map[tracepack.UUID][2]string{id: {"done", "closed"}, refused: {"failed", "the capture is not open"}} {
			if state, outcome := f.status(t, id); state != want[0] || outcome != want[1] {
				t.Errorf("request = %s, %q; want %v", state, outcome, want)
			}
		}
		if rep := f.closes(t); rep.Published != 1 || len(rep.Intended) != 0 {
			t.Errorf("Closes = %+v, want the operator's close published", rep)
		}
		pending, err := c.PendingRequests(t.Context())
		if err != nil || len(pending) != 0 || !slices.Equal([]string{f.mustState(t, capture)}, []string{catalog.StateClosed}) {
			t.Errorf("pending requests = %+v, %v", pending, err)
		}
	})
}

func (f *fixture) mustState(t *testing.T, capture tracepack.UUID) string {
	t.Helper()
	got, err := f.c.Capture(t.Context(), capture)
	if err != nil {
		t.Fatal(err)
	}

	return got.State
}

// A crash after a close's put leaves its object to the reconciler, which adopts it; the publication then finds it registered.
func TestCloseAdoptedBeforeItsPublication(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		now := time.Now().Add(3 * time.Hour)
		f.closing(t, now)
		capture := f.described(t, newID(t), now.Add(-4*time.Hour).UnixNano())
		f.stage(t, segment(t, capture, []uint64{0, 1}, false))
		ctx, crash := context.WithCancel(t.Context())
		f.r.hooks[hookClosePut] = func() { crash() }
		if _, err := f.r.Closes(ctx, f.lease); !errors.Is(err, context.Canceled) {
			t.Fatalf("Closes = %v, want the crash", err)
		}
		f.r.hooks[hookClosePut] = nil
		f.pass(t)
		if rep := f.pass(t); len(rep.Adopted) != 1 {
			t.Fatalf("pass = %+v, want the close adopted", rep)
		}
		if rep := f.closes(t); rep.Published != 1 {
			t.Fatalf("Closes = %+v, want the adopted close published", rep)
		}
		if got, err := c.Capture(t.Context(), capture); err != nil || got.HighestSeq != 1 || got.State != catalog.StateClosed {
			t.Errorf("capture = %+v, %v; want closed at highest seq 1", got, err)
		}
	})
}

// The close pass counts its closes by trigger and times its task, a pass with a failed close stamping no success.
func TestClosePassMetrics(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		now := time.Now().Add(3 * time.Hour)
		f.closing(t, now)
		s := metrics.NewServer()
		var err error
		if f.r.Metrics, err = metrics.NewCompact(s); err != nil {
			t.Fatal(err)
		}
		f.described(t, newID(t), now.Add(-4*time.Hour).UnixNano())
		if err := f.r.closePass(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		got := scrape(t, s)
		for _, want := range []string{
			`tracekeep_compact_closes_total{trigger="inactivity"} 1`, `tracekeep_compact_cycle_seconds_count{task="close"} 1`,
			`tracekeep_compact_last_success_timestamp{task="close"}`,
		} {
			if !strings.Contains(got, want) {
				t.Errorf("/metrics lacks %s", want)
			}
		}
		g := metrics.NewServer()
		if f.r.Metrics, err = metrics.NewCompact(g); err != nil {
			t.Fatal(err)
		}
		f.described(t, newID(t), now.Add(-4*time.Hour).UnixNano())
		f.r.Store = failingPut{Mem: f.s}
		if err := f.r.closePass(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		if got := scrape(t, g); strings.Contains(got, `last_success_timestamp{task="close"}`) || !strings.Contains(got, `cycle_seconds_count{task="close"} 1`) {
			t.Errorf("a pass with a failed close stamped a success, or was not timed:\n%s", got)
		}
	})
}
