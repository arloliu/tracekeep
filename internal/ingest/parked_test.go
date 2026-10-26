package ingest

import (
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// redelivery is the parked consumer's nak delay in these tests, parking.redelivery_delay.
const redelivery = 90 * time.Second // unlike any step of the backoff

// parkedConsumer is the replica's second ingester, on the parking stream:
// the executor, the flusher, the coordinator and the clock shared, its own consumer, batcher and membership.
func (r *replica) parkedConsumer(t *testing.T) *Ingester {
	t.Helper()
	ctx := t.Context()
	s := r.parked(t)
	inc, err := bus.Incarnate(ctx, r.cat, catalog.RoleParking, s)
	if err != nil {
		t.Fatal(err)
	}
	cons, err := bus.Consumer(ctx, s, inc, bus.ConsumerConfig{AckWait: 30 * time.Second, MaxAckPending: 1000})
	if err != nil {
		t.Fatal(err)
	}
	g := &Ingester{
		Executor: r.g.Executor, Flusher: r.g.Flusher, Coordinator: r.g.Coordinator, Batcher: NewBatcher(time.Second, 1<<20),
		Consumer: cons, Stream: s, Incarnation: inc, Log: slog.New(slog.DiscardHandler), Replica: "r1/parked", Lease: time.Minute,
		FetchBatch: 100, MaxWait: 200 * time.Millisecond, Heartbeat: 10 * time.Second, Now: r.clk.now, Park: true, RedeliveryDelay: redelivery,
	}
	if err := g.Join(ctx); err != nil {
		t.Fatal(err)
	}

	return g
}

func versions(t *testing.T, c *catalog.Catalog, capture tracepack.UUID) []catalog.ParkedVersion {
	t.Helper()
	vs, err := c.ParkedVersions(t.Context(), &capture)
	if err != nil {
		t.Fatal(err)
	}

	return vs
}

// copyStates returns the states of the versions' copy rows on incarnation inc.
func copyStates(vs []catalog.ParkedVersion, inc int64) []string {
	var out []string
	for _, v := range vs {
		for _, c := range v.Copies {
			if c.Incarnation == inc {
				out = append(out, c.State)
			}
		}
	}

	return out
}

// Once its blocker clears, a parked batch is staged from the parking stream:
// registered with each copy as evidence, its versions staged, every copy acknowledged and its copy row acked.
func TestParkedConsumerStages(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		r.g.Flusher.hooks[hookWrite] = func() error { return tracepack.ErrInvalidRecord }
		capture := tracepack.UUID{40}
		for _, m := range traffic(t, capture) {
			r.publish(t, m, nil)
		}
		r.step(t)
		for _, b := range r.g.Batcher.batches {
			for range writeFailureLimit - 1 {
				r.g.refused(batchName(b))
			}
		}
		r.clk.flushInterval()
		r.step(t)
		if vs := versions(t, c, capture); len(vs) != 4 {
			t.Fatalf("%d versions parked, want 4", len(vs))
		}

		r.g.Flusher.hooks[hookWrite] = nil // the Writer is fixed
		pg := r.parkedConsumer(t)
		if err := pg.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		r.clk.flushInterval()
		if err := pg.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		vs := versions(t, c, capture)
		if slices.ContainsFunc(vs, func(v catalog.ParkedVersion) bool { return v.State != catalog.VersionStaged }) {
			t.Errorf("versions %+v, want every one staged", vs)
		}
		if got := copyStates(vs, pg.Incarnation.ID); len(got) != 4 || slices.ContainsFunc(got, func(s string) bool { return s != "acked" }) {
			t.Errorf("copy rows on the parking stream %v, want 4 acked", got)
		}
		if n := msgs(t, r.parked(t)); n != 0 {
			t.Errorf("the parking stream holds %d copies, want none", n)
		}
		if got, _ := c.Capture(t.Context(), capture); got.HighestSeq != 3 {
			t.Errorf("highest seq %d, want the batch registered", got.HighestSeq)
		}
	})
}

// A parked record whose hour passed the horizon is never terminated for its age: its version moves to evicted-hour,
// and the copy stays on the parking stream for an operator.
func TestParkedConsumerEvictsInsteadOfTerminating(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		capture := tracepack.UUID{41}
		r.publish(t, inline(t, capture, dataRecord(1, now-int64(48*time.Hour)), nil), nil)
		r.step(t)
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.step(t)

		pg := r.parkedConsumer(t)
		if err := pg.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		vs := versions(t, c, capture)
		if len(vs) != 1 || vs[0].State != catalog.VersionEvictedHour {
			t.Fatalf("versions %+v, want evicted-hour", vs)
		}
		if n := msgs(t, r.parked(t)); n != 1 {
			t.Errorf("the parking stream holds %d copies, want the copy kept", n)
		}
		es, err := c.AuditEntries(t.Context(), capture)
		if err != nil || slices.ContainsFunc(es, func(e catalog.AuditEntry) bool { return e.Action == ReasonBeyondRetention }) {
			t.Errorf("audit %+v, %v; want no age termination", es, err)
		}
	})
}

// A copy whose version is still parking, its process having crashed between the publish and the advance,
// moves the version to parked when the parked consumer meets it, its own copy row recorded first.
func TestParkedConsumerAdvancesAParkingRow(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{42}
		r.publish(t, inline(t, capture, dataRecord(1, now), nil), nil)
		d, _ := r.fetch(t)
		b, err := r.g.Coordinator.Begin(ctx, d, ReasonUnknownCapture, 0)
		if err != nil {
			t.Fatal(err)
		}
		if conf, err := r.g.Coordinator.Publish(ctx, d, b.State); err != nil || conf != Confirmed {
			t.Fatalf("Publish = %v, %v", conf, err)
		}

		pg := r.parkedConsumer(t)
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		vs := versions(t, c, capture)
		if len(vs) != 1 || vs[0].State != catalog.VersionParked {
			t.Fatalf("versions %+v, want parked", vs)
		}
		if got := copyStates(vs, pg.Incarnation.ID); !slices.Equal(got, []string{"waiting"}) {
			t.Errorf("copy rows on the parking stream %v, want the copy's, waiting", got)
		}
	})
}

// A copy of a terminated version is acknowledged under the termination, its audit row first, and its copy row acked.
func TestParkedConsumerAcknowledgesUnderTheTermination(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{43}
		r.publish(t, inline(t, capture, dataRecord(1, now-int64(48*time.Hour)), nil), nil)
		r.step(t)
		if n, err := c.TerminateParked(ctx, capture, nil, "op", "p1"); err != nil || n != 1 {
			t.Fatalf("TerminateParked = %d, %v", n, err)
		}

		pg := r.parkedConsumer(t)
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if n := msgs(t, r.parked(t)); n != 0 {
			t.Errorf("the parking stream holds %d copies, want the copy acknowledged", n)
		}
		if got := copyStates(versions(t, c, capture), pg.Incarnation.ID); !slices.Equal(got, []string{"acked"}) {
			t.Errorf("copy rows on the parking stream %v, want acked", got)
		}
		es, err := c.AuditEntries(ctx, capture)
		if err != nil || !slices.ContainsFunc(es, func(e catalog.AuditEntry) bool { return e.Action == ReasonTerminated && e.ActionID == "p1" }) {
			t.Errorf("audit %+v, %v; want the acknowledgement under p1", es, err)
		}
	})
}

// The parked consumer returns what still waits with its own long delay, and never parks again what is already parked.
func TestParkedConsumerDelays(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		pg := r.parkedConsumer(t)
		d := delivered(transported(t, inline(t, tracepack.UUID{44}, dataRecord(1, now), nil)))
		d.Msg.Parked = true
		for _, dec := range []Decision{{Action: ActPark, Reason: ReasonUnknownCapture}, {Action: ActNak, Reason: ReasonIncompatible}} {
			h := &msg{}
			pg.act(t.Context(), Entry{D: d, Msg: h}, dec, 9)
			if got := h.done(); !slices.Equal(got, []string{"nak " + redelivery.String()}) {
				t.Errorf("%v: %v, want a nak with the redelivery delay", dec, got)
			}
		}
	})
}
