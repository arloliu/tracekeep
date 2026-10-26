package ingest

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

func (r *replica) parked(t *testing.T) jetstream.Stream {
	t.Helper()
	s, err := r.js.Stream(t.Context(), busconfig.DefaultParkedStream)
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func (r *replica) step(t *testing.T) {
	t.Helper()
	if err := r.g.Step(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// An expired record of an unknown capture is parked, which creates the capture's pending row;
// the capture's start then admits it in place (ingest.md §6.1).
func TestIngesterParksThenAdmitsInPlace(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{20}
		r.publish(t, inline(t, capture, dataRecord(1, now-int64(48*time.Hour)), nil), nil)
		r.step(t)
		if got, err := c.Capture(ctx, capture); err != nil || got.State != catalog.StatePending {
			t.Fatalf("capture %+v, %v; want pending", got, err)
		}
		if n := r.pending(t); n != 0 {
			t.Errorf("the traffic stream holds %d messages, want the original acknowledged", n)
		}
		if n := msgs(t, r.parked(t)); n != 1 {
			t.Errorf("the parking stream holds %d copies, want 1", n)
		}
		vs, err := c.ParkedVersions(ctx, &capture)
		if err != nil || len(vs) != 1 || vs[0].State != catalog.VersionParked || vs[0].Reason != ReasonDeferredAged {
			t.Fatalf("versions %+v, %v; want parked as deferred-expired", vs, err)
		}

		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.step(t)
		if got, err := c.Capture(ctx, capture); err != nil || got.State != catalog.StateOpen {
			t.Errorf("capture %+v, %v; want the pending row admitted in place", got, err)
		}
	})
}

// With parking off, an indefinite deferral is nak'd like any other and nothing reaches the parking stream.
func TestIngesterWithoutParkingNaks(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		r.g.Park = false
		capture := tracepack.UUID{21}
		r.publish(t, inline(t, capture, dataRecord(1, now-int64(48*time.Hour)), nil), nil)
		r.step(t)
		if n := r.pending(t); n != 1 {
			t.Errorf("the traffic stream holds %d messages, want the original returned", n)
		}
		if n := msgs(t, r.parked(t)); n != 0 {
			t.Errorf("the parking stream holds %d copies, want none", n)
		}
		if vs, err := c.ParkedVersions(t.Context(), &capture); err != nil || len(vs) != 0 {
			t.Errorf("versions %+v, %v; want none", vs, err)
		}
	})
}

// failingAck refuses the acknowledgement its server never confirmed.
type failingAck struct{ jetstream.Msg }

func (failingAck) DoubleAck(context.Context) error { return errors.New("no confirmation") }

// A start an order covers, with a descriptor that does not even decode, is acknowledged under the termination
// before anything frames it, with an audit row naming the order's action and no admission side effect;
// an acknowledgement the server did not confirm leaves the message to be acknowledged again, with its own audit row.
func TestIngesterAcknowledgesUnderTheTermination(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{22}
		if _, err := c.TerminateCapture(ctx, capture, r.g.Incarnation.ID, 100, "op", "c1"); err != nil {
			t.Fatal(err)
		}
		start := inline(t, capture, startRecord(t, now), nil)
		start.Env.Descriptor = "not base64!"
		r.publish(t, start, nil)

		acked := func() int {
			es, err := c.AuditEntries(ctx, capture)
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, e := range es {
				if e.Action == ReasonTerminated && e.ActionID == "c1" {
					n++
				}
			}

			return n
		}
		d, m := r.fetch(t)
		d, dec, err := r.g.Executor.Dispose(ctx, d.Msg)
		if err != nil || dec.Action != ActAck || dec.Reason != ReasonTerminated {
			t.Fatalf("Dispose = %+v, %v; want acknowledged under the termination", dec, err)
		}
		r.g.act(ctx, Entry{D: d, Msg: failingAck{m}}, dec, 1)
		if n := acked(); n != 1 {
			t.Errorf("%d acknowledgement audit rows, want 1", n)
		}
		if err := m.NakWithDelay(0); err != nil {
			t.Fatal(err)
		}
		r.step(t)
		if n := acked(); n != 2 {
			t.Errorf("%d acknowledgement audit rows after the redelivery, want 2", n)
		}
		if n := r.pending(t); n != 0 {
			t.Errorf("the traffic stream holds %d messages, want the start acknowledged", n)
		}
		if got, err := c.Capture(ctx, capture); err != nil || got.State != catalog.StatePending {
			t.Errorf("capture %+v, %v; want the placeholder untouched", got, err)
		}
	})
}

// A batch the Writer refused at the write-failure limit is parked, each original acknowledged once its copy is stored.
func TestIngesterParksAWriteFailure(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		r.g.Flusher.hooks[hookWrite] = func() error { return tracepack.ErrInvalidRecord }
		capture := tracepack.UUID{23}
		for _, m := range traffic(t, capture) {
			r.publish(t, m, nil)
		}
		r.step(t)
		for _, b := range r.g.Batcher.batches { // the batch was refused twice before
			for range writeFailureLimit - 1 {
				r.g.refused(batchName(b))
			}
		}
		r.clk.flushInterval()
		r.step(t)
		if n := r.pending(t); n != 0 {
			t.Errorf("the traffic stream holds %d messages, want every original acknowledged", n)
		}
		if n := msgs(t, r.parked(t)); n != 4 {
			t.Errorf("the parking stream holds %d copies, want 4", n)
		}
		vs, err := c.ParkedVersions(ctx, &capture)
		if err != nil || len(vs) != 4 || slices.ContainsFunc(vs, func(v catalog.ParkedVersion) bool {
			return v.State != catalog.VersionParked || v.Reason != ReasonWriteFailure
		}) {
			t.Errorf("versions %+v, %v; want 4 parked as write failures", vs, err)
		}
	})
}

// answering answers every publish with ack and err.
type answering struct {
	ack *jetstream.PubAck
	err error
}

func (a answering) PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return a.ack, a.err
}

// The ingester acts on what parking found: a suppressed copy waits out the duplicate window, an unconfirmed one backs off,
// a resolved version is acknowledged (under the termination with its audit row), a version past the horizon is terminated.
func TestIngesterActsOnParking(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		p := newPipeline(t, c)
		g := p.ingester(slog.New(slog.DiscardHandler))
		g.Park = true
		covered := tracepack.UUID{31}
		if _, err := c.TerminateCapture(ctx, covered, 1, 100, "op", "c1"); err != nil {
			t.Fatal(err)
		}
		admitted := tracepack.UUID{32}
		p.deliver(t, traffic(t, admitted)[:1])
		for _, tc := range []struct {
			name    string
			capture tracepack.UUID
			bus     Publisher
			horizon int64
			want    []string
		}{
			{"suppressed", tracepack.UUID{30}, answering{ack: &jetstream.PubAck{Sequence: 1, Duplicate: true}}, 0, []string{"nak " + suppressedNakDelay.String()}},
			{"unconfirmed", tracepack.UUID{33}, answering{err: context.DeadlineExceeded}, 0, []string{"nak " + nakDelay(3).String()}},
			{"terminated", covered, nil, 0, []string{"double-ack"}},
			{"past the horizon", admitted, nil, HourOf(now) + 1, []string{"term"}},
		} {
			g.Coordinator = &Coordinator{Catalog: c, Bus: tc.bus, ParkPrefix: busconfig.DefaultParkPrefix}
			g.Executor.Horizon = func() int64 { return tc.horizon * nsPerHour }
			h := &msg{}
			d := delivered(transported(t, inline(t, tc.capture, dataRecord(5, now), nil)))
			g.park(ctx, Entry{D: d, Msg: h}, ReasonUnknownCapture, 3)
			if got := h.done(); !slices.Equal(got, tc.want) {
				t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
			}
		}
		es, err := c.AuditEntries(ctx, covered)
		if err != nil || !slices.ContainsFunc(es, func(e catalog.AuditEntry) bool { return e.Action == ReasonTerminated && e.ActionID == "c1" }) {
			t.Errorf("audit %+v, %v; want the acknowledgement under c1", es, err)
		}
		es, err = c.AuditEntries(ctx, admitted)
		if err != nil || !slices.ContainsFunc(es, func(e catalog.AuditEntry) bool { return e.Action == ReasonBeyondRetention }) {
			t.Errorf("audit %+v, %v; want the beyond-retention termination", es, err)
		}
	})
}

// An order whose cut-off the traffic stream's first sequence passed is spent at the next renewal: no message it covers can exist.
func TestIngesterSpendsPassedOrders(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{24}
		r.publish(t, inline(t, capture, dataRecord(1, now), nil), nil)
		if _, err := c.TerminateCapture(ctx, capture, r.g.Incarnation.ID, 1, "op", "c1"); err != nil {
			t.Fatal(err)
		}
		r.step(t) // the covered message is acknowledged under the order
		if orders, _ := c.TerminationOrders(ctx, capture); len(orders) != 1 || orders[0].State != "active" {
			t.Fatalf("orders %+v, want active while its message may still be redelivered", orders)
		}
		r.publish(t, inline(t, tracepack.UUID{25}, dataRecord(1, now), nil), nil) // the stream's first sequence moves past the cut-off
		r.step(t)
		if orders, _ := c.TerminationOrders(ctx, capture); len(orders) != 1 || orders[0].State != "spent" {
			t.Errorf("orders %+v, want spent once the first sequence passed the cut-off", orders)
		}
	})
}

// A new message of a version resolved before is accounted for by its own copy row before its acknowledgement:
// waiting while the server has not confirmed it, acked once it has.
func TestAcknowledgementRecordsTheMessage(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{26}
		m := inline(t, capture, dataRecord(1, now), nil)
		earlier := delivered(m)
		earlier.Msg.Incarnation, earlier.Msg.StreamSeq = r.g.Incarnation.ID, 100
		if _, err := c.BeginParking(ctx, earlier.identity(), ReasonUnknownCapture, "", 0); err != nil {
			t.Fatal(err)
		}
		if _, err := c.TerminateParked(ctx, capture, nil, "op", "p1"); err != nil {
			t.Fatal(err)
		}
		r.publish(t, m, nil)
		d, held := r.fetch(t)
		d, dec, err := r.g.Executor.Dispose(ctx, d.Msg)
		if err != nil || dec.Action != ActAck || dec.Reason != ReasonTerminated {
			t.Fatalf("Dispose = %+v, %v", dec, err)
		}
		state := func() string {
			for _, v := range versions(t, c, capture) {
				for _, cp := range v.Copies {
					if cp.StreamSeq == d.Msg.StreamSeq {
						return cp.State
					}
				}
			}

			return "none"
		}
		r.g.act(ctx, Entry{D: d, Msg: failingAck{held}}, dec, 1)
		if s := state(); s != "waiting" {
			t.Errorf("the message's copy row after an unconfirmed acknowledgement = %s, want waiting", s)
		}
		if err := held.NakWithDelay(0); err != nil {
			t.Fatal(err)
		}
		r.step(t)
		if s := state(); s != "acked" {
			t.Errorf("the message's copy row after the redelivery = %s, want acked", s)
		}
	})
}

// A terminated pending start, then a start of another version above the order's cut-off, published later (the gate's case):
// the first is acknowledged under the termination with no admission effect, the second admits the capture,
// and the first version delivered again is acknowledged under the termination again.
func TestTerminatedStartThenAnotherVersion(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"inline", "claim-check with its object missing"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			terminatedStartThenAnotherVersion(t, kind != "inline")
		})
	}
}

func terminatedStartThenAnotherVersion(t *testing.T, claim bool) {
	t.Helper()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		// The versions share a message id, the capture and seq's: each is published after the traffic stream's duplicate window.
		cfg := r.stream.CachedInfo().Config
		cfg.Duplicates = 100 * time.Millisecond
		if _, err := r.js.UpdateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		later := func(m Message) {
			time.Sleep(2 * cfg.Duplicates) // the scenario's delay
			subject, err := tracepack.BusSubject(prefix, "T1", m.Raw().Version.Capture)
			if err != nil {
				t.Fatal(err)
			}
			if ack := r.publishTo(t, subject, m); ack.Duplicate {
				t.Fatal("taken for a duplicate; the window has not passed")
			}
		}
		capture := tracepack.UUID{23}
		first := inline(t, capture, startRecord(t, now), nil)
		first.Env.Descriptor = "not base64!"
		if claim {
			first = claimCheck(t, newClaims(), capture, startRecord(t, now), descriptorOf(t, "eqp")) // its object never reaches the bucket
		}
		r.publish(t, first, nil)
		if _, err := c.TerminateCapture(ctx, capture, r.g.Incarnation.ID, 1, "op", "c1"); err != nil {
			t.Fatal(err)
		}
		r.step(t)
		if got, err := c.Capture(ctx, capture); err != nil || got.State != catalog.StatePending {
			t.Fatalf("capture %+v, %v; want no admission effect", got, err)
		}

		later(inline(t, capture, startRecord(t, now+1), descriptorOf(t, "eqp"))) // another version, above the cut-off
		r.step(t)
		if got, err := c.Capture(ctx, capture); err != nil || got.State != catalog.StateOpen {
			t.Fatalf("capture %+v, %v; want the other version's start admitting it", got, err)
		}

		later(first)
		r.step(t)
		r.clk.flushInterval() // the admitted start's batch is flushed and acknowledged
		r.step(t)
		es, err := c.AuditEntries(ctx, capture)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range es {
			if e.Action == ReasonTerminated && e.ActionID == "c1" {
				n++
			}
		}
		if n != 2 || r.pending(t) != 0 {
			t.Errorf("%d acknowledgements under c1, %d messages pending; want the first version acknowledged twice, nothing left", n, r.pending(t))
		}
	})
}
