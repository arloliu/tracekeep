package ingest

import (
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/producer/busconfig"
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
		original := inline(t, capture, dataRecord(1, now-int64(48*time.Hour)), nil)
		r.publish(t, original, nil)
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
		underP1 := func() int {
			es, err := c.AuditEntries(ctx, capture)
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, e := range es {
				if e.Action == ReasonTerminated && e.ActionID == "p1" {
					n++
				}
			}

			return n
		}
		if n := underP1(); n != 1 {
			t.Errorf("%d acknowledgements under p1, want 1", n)
		}

		// A copy of the version published later (the gate's "a day later": after the first was acknowledged, its subject empty),
		// as a delayed publish would store it, is acknowledged under the same termination with its own audit row.
		traffic, err := tracepack.BusSubject(prefix, "T1", capture)
		if err != nil {
			t.Fatal(err)
		}
		subject, err := parkSubject(busconfig.DefaultParkPrefix, traffic, original.Raw().Version)
		if err != nil {
			t.Fatal(err)
		}
		// The scenario's delay: the late copy comes after the parking stream's duplicate window, here shortened to 100 ms.
		s := r.parked(t)
		cfg := s.CachedInfo().Config
		cfg.Duplicates = 100 * time.Millisecond
		if _, err := r.js.UpdateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * cfg.Duplicates)
		if ack := r.publishTo(t, subject, original); ack.Duplicate {
			t.Fatal("the late copy was taken for a duplicate; the window has not passed")
		}
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if n := msgs(t, r.parked(t)); n != 0 {
			t.Errorf("the parking stream holds %d copies, want the late copy acknowledged", n)
		}
		if n := underP1(); n != 2 {
			t.Errorf("%d acknowledgements under p1 after the late copy, want 2", n)
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

// A parked copy's version evicted while its batch's put was in flight is never staged by the registration that follows:
// the key is refused, the version stays evicted-hour and the copy stays on the parking stream, unacknowledged.
func TestParkedConsumerFlushMeetsAnEviction(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{45}
		r.publish(t, inline(t, capture, dataRecord(1, now), nil), nil)
		d, m := r.fetch(t)
		b, err := r.g.Coordinator.Begin(ctx, d, ReasonUnknownCapture, 0)
		if err != nil {
			t.Fatal(err)
		}
		if conf, err := r.g.Coordinator.Publish(ctx, d, b.State); err != nil || conf != Confirmed {
			t.Fatalf("Publish = %v, %v", conf, err)
		}
		if err := r.g.Coordinator.Acknowledge(ctx, d, m); err != nil {
			t.Fatal(err)
		}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.step(t) // the capture admitted

		pg := r.parkedConsumer(t)
		if err := pg.Step(ctx); err != nil { // the copy advanced to parked and batched
			t.Fatal(err)
		}
		pg.Flusher.hooks[hookAfterPut] = func() error {
			pg.Flusher.hooks[hookAfterPut] = nil
			_, err := c.EvictParked(ctx, d.versionKey()) // retention evicts the hour while the put is in flight
			return err
		}
		r.clk.flushInterval()
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if vs := versions(t, c, capture); len(vs) != 1 || vs[0].State != catalog.VersionEvictedHour {
			t.Fatalf("versions %+v, want evicted-hour", vs)
		}
		if n := msgs(t, r.parked(t)); n != 1 {
			t.Errorf("the parking stream holds %d copies, want the copy unacknowledged", n)
		}
		if got := copyStates(versions(t, c, capture), pg.Incarnation.ID); slices.Contains(got, "acked") {
			t.Errorf("copy rows %v, want none acked", got)
		}
	})
}

// parking.enabled turned off on a running service, by a restart without it (gate: the parking phase's case):
// a new indefinite deferral is nak'd, not parked, while the waiting and evicted-hour versions already parked
// stay listable and terminable, and a copy delivered after its termination is acknowledged under it.
func TestParkingTurnedOff(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		evicted, waiting, after := tracepack.UUID{46}, tracepack.UUID{47}, tracepack.UUID{48}
		r.publish(t, inline(t, evicted, dataRecord(1, now-int64(48*time.Hour)), nil), nil)
		r.step(t)
		r.publish(t, inline(t, evicted, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.step(t)
		pg := r.parkedConsumer(t)
		if err := pg.Step(ctx); err != nil { // the evicted capture's copy: admitted meanwhile, its hour beyond the horizon
			t.Fatal(err)
		}
		r.publish(t, inline(t, waiting, dataRecord(1, now-int64(48*time.Hour)), nil), nil)
		r.step(t)

		r.g.Park, pg.Park = false, false // the restart without parking
		r.publish(t, inline(t, after, dataRecord(1, now-int64(48*time.Hour)), nil), nil)
		r.step(t)
		if vs := versions(t, c, after); len(vs) != 0 {
			t.Errorf("versions %+v of a deferral after parking was turned off, want none parked", vs)
		}

		for capture, want := range map[tracepack.UUID]catalog.VersionState{evicted: catalog.VersionEvictedHour, waiting: catalog.VersionParked} {
			if vs := versions(t, c, capture); len(vs) != 1 || vs[0].State != want {
				t.Errorf("versions %+v, want one listed %s", vs, want)
			}
			if n, err := c.TerminateParked(ctx, capture, nil, "op", "p-"+capture.String()); err != nil || n != 1 {
				t.Errorf("TerminateParked = %d, %v; want the version terminated", n, err)
			}
		}
		if err := pg.Step(ctx); err != nil { // the waiting version's copy, delivered for the first time since its termination
			t.Fatal(err)
		}
		if got := copyStates(versions(t, c, waiting), pg.Incarnation.ID); !slices.Equal(got, []string{"acked"}) {
			t.Errorf("the terminated copy's rows %v, want it acknowledged under the termination", got)
		}
	})
}

// rejected → open with a parked copy, then staged (the gate's case): a capture whose descriptor this consumer refuses is rejected
// and its records parked; a fixed release, which the test stands in for by widening the accepted descriptors,
// accepts the parked start in a rejected → open transaction, and the copies are staged and acknowledged.
func TestRejectedCaptureOpensFromItsParkedCopy(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{49}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, " ")), nil) // no device name: refused
		r.step(t)
		if got, err := c.Capture(ctx, capture); err != nil || got.State != catalog.StateRejected {
			t.Fatalf("capture %+v, %v; want rejected", got, err)
		}
		r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
		r.step(t)
		if vs := versions(t, c, capture); len(vs) != 2 {
			t.Fatalf("versions %+v, want the start and the record parked", vs)
		}

		pg := r.parkedConsumer(t)
		r.g.Executor.accepts = func([]byte) bool { return true } // the fixed release
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if got, err := c.Capture(ctx, capture); err != nil || got.State != catalog.StateOpen {
			t.Fatalf("capture %+v, %v; want rejected → open from the parked start", got, err)
		}
		r.clk.flushInterval()
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		vs := versions(t, c, capture)
		if len(vs) != 2 || slices.ContainsFunc(vs, func(v catalog.ParkedVersion) bool { return v.State != catalog.VersionStaged }) {
			t.Errorf("versions %+v, want both staged", vs)
		}
		if n := msgs(t, r.parked(t)); n != 0 {
			t.Errorf("the parking stream holds %d copies, want none", n)
		}
	})
}

// The traffic and parked consumers processing at once over their shared executor, flusher and coordinator (barrier-controlled):
// the traffic consumer's flush is held between its put and its registration while the parked consumer admits, stages and acknowledges
// a parked capture through the same flusher; released, the traffic flush completes, and both captures end registered with nothing left.
func TestConsumersProcessAtOnce(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		parkedCapture, live := tracepack.UUID{53}, tracepack.UUID{54}
		// The parked capture: its batch refused by the Writer to the write-failure limit, then parked.
		r.g.Flusher.hooks[hookWrite] = func() error { return tracepack.ErrInvalidRecord }
		for _, m := range traffic(t, parkedCapture) {
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
		r.g.Flusher.hooks[hookWrite] = nil // the Writer is fixed
		if vs := versions(t, c, parkedCapture); len(vs) != 4 {
			t.Fatalf("%d versions parked, want 4", len(vs))
		}
		cl := newClaims() // claim-check, so the stream reflects the confirmed acknowledgements once a step returns
		for _, m := range confirmedTraffic(t, cl, live) {
			r.publish(t, m, cl.objs[m.Env.MsgID])
		}
		r.step(t) // the live capture batched
		pg := r.parkedConsumer(t)

		held, release := make(chan struct{}), make(chan struct{})
		var first atomic.Bool // only the first put holds; a sync.Once would make the second wait for the first
		r.g.Flusher.hooks[hookAfterPut] = func() error {
			if first.CompareAndSwap(false, true) {
				close(held)
				<-release
			}
			return nil
		}
		r.clk.flushInterval()
		trafficDone := make(chan error, 1)
		go func() { trafficDone <- r.g.Step(ctx) }() // flushes the due batches, held after the first put
		<-held
		if err := pg.Step(ctx); err != nil { // the parked copies batched
			t.Fatal(err)
		}
		r.clk.flushInterval()
		if err := pg.Step(ctx); err != nil { // its flush runs through the shared flusher while the other is held
			t.Fatal(err)
		}
		if vs := versions(t, c, parkedCapture); slices.ContainsFunc(vs, func(v catalog.ParkedVersion) bool { return v.State != catalog.VersionStaged }) {
			t.Errorf("parked versions %+v, want every one staged while the traffic flush is held", vs)
		}
		close(release)
		if err := <-trafficDone; err != nil {
			t.Fatal(err)
		}
		r.g.Flusher.hooks[hookAfterPut] = nil
		r.step(t)
		for _, capture := range []tracepack.UUID{parkedCapture, live} {
			if got, err := c.Capture(ctx, capture); err != nil || !got.HasHighestSeq {
				t.Errorf("capture %s: %+v, %v; want its records registered", capture, got, err)
			}
		}
		if n, m := r.pending(t), msgs(t, r.parked(t)); n != 0 || m != 0 {
			t.Errorf("%d traffic and %d parked messages left, want none", n, m)
		}
	})
}

// Checklist 5's claim-bearing case: a claim-check version parked, its hour evicted (an evicted-hour version, its object kept),
// then terminated; once its object is deleted, as survivor cleanup does when its predicate holds, a late message of the same version
// is acknowledged under the termination without the object, with its own audit row.
func TestLateClaimMessageAfterItsObjectIsGone(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{55}
		cl := newClaims()
		late := claimCheck(t, cl, capture, dataRecord(1, now-int64(48*time.Hour)), nil)
		r.publish(t, late, cl.objs[late.Env.MsgID])
		r.step(t) // parked: expired, its capture unknown
		// The start by claim-check too: its acknowledgement is confirmed, so the stream reflects it once a step returns.
		start := claimCheck(t, cl, capture, startRecord(t, now), descriptorOf(t, "eqp"))
		r.publish(t, start, cl.objs[start.Env.MsgID])
		r.step(t)
		pg := r.parkedConsumer(t)
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if vs := versions(t, c, capture); len(vs) != 1 || vs[0].State != catalog.VersionEvictedHour {
			t.Fatalf("versions %+v, want the claim-check version evicted-hour", vs)
		}
		if n, err := c.TerminateParked(ctx, capture, nil, "op", "p5"); err != nil || n != 1 {
			t.Fatalf("TerminateParked = %d, %v", n, err)
		}
		if err := r.claims.Delete(ctx, late.Env.MsgID); err != nil { // the survivor's object deleted
			t.Fatal(err)
		}

		cfg := r.stream.CachedInfo().Config
		cfg.Duplicates = 100 * time.Millisecond
		if _, err := r.js.UpdateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * cfg.Duplicates) // the scenario's delay: the late message comes after the window
		subject, err := tracepack.BusSubject(prefix, "T1", capture)
		if err != nil {
			t.Fatal(err)
		}
		if ack := r.publishTo(t, subject, late); ack.Duplicate {
			t.Fatal("the late message was taken for a duplicate")
		}
		r.step(t)
		es, err := c.AuditEntries(ctx, capture)
		if err != nil || !slices.ContainsFunc(es, func(e catalog.AuditEntry) bool { return e.Action == ReasonTerminated && e.ActionID == "p5" }) {
			t.Errorf("audit %+v, %v; want the late message acknowledged under p5", es, err)
		}
		r.clk.flushInterval() // the admitted start's batch flushed and acknowledged
		r.step(t)
		if n := r.pending(t); n != 0 {
			t.Errorf("the traffic stream holds %d messages, want the late message acknowledged without its object", n)
		}
	})
}
