package ingest

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/testbus"
)

// The phase 1 gate's parking schedules (docs/ROADMAP.md, phase 1 and its implementation checklist).

func (r *replica) admin() *admin.Admin {
	return &admin.Admin{Catalog: r.cat, Bus: r.js, Config: bus.Config{SubjectPrefix: prefix, ClaimBucket: bucket}, Actor: "op"}
}

// restore replaces the stream named name by its snapshot, as an operator restoring a backup does.
func (r *replica) restore(t *testing.T, name string, snapshot func()) {
	t.Helper()
	cfg, data := testbus.Snapshot(t, r.nc, name)
	snapshot()
	if err := r.js.DeleteStream(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	r.srv.Restore(t, cfg, data)
}

func consumerNames(t *testing.T, s jetstream.Stream) []string {
	t.Helper()
	var names []string
	for n := range s.ConsumerNames(t.Context()).Name() {
		names = append(names, n)
	}

	return names
}

// Checklist 1, the traffic stream: restored from a snapshot holding the retired incarnation's consumer with a delivery pending,
// the stream is refused until bus adopt-stream ran, which deletes that consumer;
// the old handle's acknowledgement then removes nothing, and the pending messages reach the new incarnation's consumer.
func TestRestoredTrafficStream(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{50}
		// Claim-check records: their acknowledgements are confirmed, so the stream reflects them once a step returns.
		cl := newClaims()
		for _, m := range []Message{
			claimCheck(t, cl, capture, startRecord(t, now), descriptorOf(t, "eqp")),
			claimCheck(t, cl, capture, dataRecord(1, now+1), nil), claimCheck(t, cl, capture, dataRecord(2, now+2), nil),
		} {
			r.publish(t, m, cl.objs[m.Env.MsgID])
		}
		_, held := r.fetch(t) // deliveries the old consumer holds when the snapshot is taken
		_, held2 := r.fetch(t)
		r.restore(t, r.stream.CachedInfo().Config.Name, func() {
			r.publish(t, traffic(t, capture)[3], nil)                                       // accepted after the snapshot: the restore rewinds it away
			if _, err := bus.Incarnate(ctx, c, catalog.RoleTraffic, r.stream); err != nil { // a renewal observes the later sequence
				t.Fatal(err)
			}
		})
		s, err := r.js.Stream(ctx, r.stream.CachedInfo().Config.Name)
		if err != nil {
			t.Fatal(err)
		}
		if got := consumerNames(t, s); !slices.Equal(got, []string{bus.ConsumerName(r.g.Incarnation)}) {
			t.Fatalf("consumers %v; want the retired incarnation's back from the snapshot", got)
		}
		if _, err := bus.Incarnate(ctx, c, catalog.RoleTraffic, s); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Fatalf("Incarnate of the restored stream = %v, want ErrIncarnationChanged", err)
		}

		if _, err := r.admin().AdoptStream(ctx, catalog.RoleTraffic); !errors.Is(err, catalog.ErrLeaseLive) {
			t.Fatalf("AdoptStream while the replica holds its lease = %v", err)
		}
		if err := c.Leave(ctx, r.g.Replica); err != nil { // every process stopped
			t.Fatal(err)
		}
		inc, err := r.admin().AdoptStream(ctx, catalog.RoleTraffic)
		if err != nil {
			t.Fatal(err)
		}
		if got := consumerNames(t, s); len(got) != 0 {
			t.Fatalf("consumers %v after the adoption, want none", got)
		}
		cons, err := bus.Consumer(ctx, s, inc, bus.ConsumerConfig{AckWait: 30 * time.Second, MaxAckPending: 1000})
		if err != nil {
			t.Fatalf("the new incarnation's consumer = %v", err)
		}

		staleHandler(t, r, r.g.Consumer, held, held2, cons, 3)
		r.g.Consumer, r.g.Stream, r.g.Incarnation = cons, s, inc
		if err := r.g.Join(ctx); err != nil {
			t.Fatal(err)
		}
		r.step(t)
		r.clk.flushInterval()
		r.step(t)
		if got, err := c.Capture(ctx, capture); err != nil || got.State != catalog.StateOpen || got.HighestSeq != 2 {
			t.Errorf("capture %+v, %v; want the pending start and records ingested", got, err)
		}
		if n := r.pending(t); n != 0 {
			t.Errorf("the stream holds %d messages, want none", n)
		}
	})
}

// A traffic stream deleted and created again while a handler holds old deliveries (the gate's case): the stream is refused until adopted;
// the handler's late acknowledgement and nak name the retired consumer and remove nothing from the new stream;
// a reused sequence inherits nothing, an order of the retired incarnation covering none of the new stream's messages.
func TestRecreatedTrafficStream(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		old, renewed := tracepack.UUID{51}, tracepack.UUID{52}
		for _, m := range traffic(t, old)[:2] {
			r.publish(t, m, nil)
		}
		_, held := r.fetch(t)
		_, held2 := r.fetch(t)
		// An order of the retired incarnation, cut off above every sequence the new stream will reuse.
		if _, err := c.TerminateCapture(ctx, renewed, r.g.Incarnation.ID, 100, "op", "c-old"); err != nil {
			t.Fatal(err)
		}
		cfg := r.stream.CachedInfo().Config
		if err := r.js.DeleteStream(ctx, cfg.Name); err != nil {
			t.Fatal(err)
		}
		s, err := r.js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bus.Incarnate(ctx, c, catalog.RoleTraffic, s); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Fatalf("Incarnate of the recreated stream = %v, want ErrIncarnationChanged", err)
		}
		if err := c.Leave(ctx, r.g.Replica); err != nil { // every process stopped
			t.Fatal(err)
		}
		inc, err := r.admin().AdoptStream(ctx, catalog.RoleTraffic)
		if err != nil {
			t.Fatal(err)
		}
		r.stream = s
		for _, m := range traffic(t, renewed)[:2] { // the new stream's sequences 1 and 2, below the old order's cut-off
			r.publish(t, m, nil)
		}
		cons, err := bus.Consumer(ctx, s, inc, bus.ConsumerConfig{AckWait: 30 * time.Second, MaxAckPending: 1000})
		if err != nil {
			t.Fatal(err)
		}
		staleHandler(t, r, r.g.Consumer, held, held2, cons, 2)

		r.g.Consumer, r.g.Stream, r.g.Incarnation = cons, s, inc
		if err := r.g.Join(ctx); err != nil {
			t.Fatal(err)
		}
		r.step(t)
		r.clk.flushInterval()
		r.step(t)
		if got, err := c.Capture(ctx, renewed); err != nil || got.State != catalog.StateOpen || got.HighestSeq != 1 {
			t.Errorf("capture %+v, %v; want its records ingested as new traffic", got, err)
		}
		if es, err := c.AuditEntries(ctx, renewed); err != nil || slices.ContainsFunc(es, func(e catalog.AuditEntry) bool { return e.Action == ReasonTerminated }) {
			t.Errorf("audit %+v, %v; want no acknowledgement under the old order", es, err)
		}
		if n := r.pending(t); n != 0 {
			t.Errorf("the stream holds %d messages, want none", n)
		}
	})
}

// staleHandler is a process that missed the stop: its buffered acknowledgement and nak, and its pull, name the retired consumer,
// and leave the new consumer's pending messages pending and none held.
func staleHandler(t *testing.T, r *replica, retired jetstream.Consumer, acked, nakd jetstream.Msg, cons jetstream.Consumer, pending uint64) {
	t.Helper()
	if err := acked.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := nakd.Nak(); err != nil {
		t.Fatal(err)
	}
	if batch, err := retired.Fetch(1, jetstream.FetchMaxWait(200*time.Millisecond)); err == nil {
		for range batch.Messages() {
			t.Error("the retired consumer delivered a message")
		}
	}
	if err := r.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	info, err := cons.Info(t.Context())
	if err != nil || info.NumPending != pending || info.NumAckPending != 0 {
		t.Fatalf("the new consumer: %+v, %v; want the %d messages pending and none held", info, err, pending)
	}
}

// Checklist 1, the parking stream: restored with the retired parked consumer, adopted, and its copy delivered to the new one,
// which records its own copy row on the new incarnation.
func TestRestoredParkingStream(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{51}
		r.publish(t, inline(t, capture, dataRecord(1, now-int64(48*time.Hour)), nil), nil)
		r.publish(t, inline(t, capture, dataRecord(2, now-int64(48*time.Hour)), nil), nil)
		r.step(t)
		pg := r.parkedConsumer(t)
		retired := pg.Consumer
		batch, err := retired.Fetch(2, jetstream.FetchMaxWait(5*time.Second)) // copies a parked handler holds when the snapshot is taken
		if err != nil {
			t.Fatal(err)
		}
		var held []jetstream.Msg
		for m := range batch.Messages() {
			held = append(held, m)
		}
		if len(held) != 2 {
			t.Fatalf("held %d copies, want 2", len(held))
		}
		parked := r.parked(t)
		r.restore(t, parked.CachedInfo().Config.Name, func() {})
		if _, err := bus.Incarnate(ctx, c, catalog.RoleParking, r.parked(t)); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Fatalf("Incarnate of the restored parking stream = %v, want ErrIncarnationChanged", err)
		}
		for _, replica := range []string{r.g.Replica, pg.Replica} {
			if err := c.Leave(ctx, replica); err != nil {
				t.Fatal(err)
			}
		}
		inc, err := r.admin().AdoptStream(ctx, catalog.RoleParking)
		if err != nil {
			t.Fatal(err)
		}
		s := r.parked(t)
		if got := consumerNames(t, s); len(got) != 0 {
			t.Fatalf("consumers %v after the adoption, want none", got)
		}
		cons, err := bus.Consumer(ctx, s, inc, bus.ConsumerConfig{AckWait: 30 * time.Second, MaxAckPending: 1000})
		if err != nil {
			t.Fatal(err)
		}
		staleHandler(t, r, retired, held[0], held[1], cons, 2) // checklist 1's stale handler, on the parking stream
		pg.Consumer, pg.Stream, pg.Incarnation = cons, s, inc
		if err := pg.Join(ctx); err != nil {
			t.Fatal(err)
		}
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if got := copyStates(versions(t, c, capture), inc.ID); !slices.Equal(got, []string{"waiting", "waiting"}) {
			t.Errorf("copy rows on the new incarnation %v, want both copies', waiting", got)
		}
	})
}

// Checklist 5, the combined lifecycle under one capture terminate:
// V1 parked, V2 a deferred original never parked below the cut-off, V3 parked then evicted-hour, V4 published after the cut-off.
// V1, V2 and V3 end terminated and acknowledged under the termination; V4 is new traffic, handled normally.
func TestTerminationLifecycle(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{52}
		old := now - int64(48*time.Hour)
		r.publish(t, inline(t, capture, dataRecord(1, old), nil), nil)
		r.step(t) // V1 parked, the capture pending
		r.publish(t, inline(t, capture, dataRecord(2, now), nil), nil)
		_, v2 := r.fetch(t) // V2 held, never parked
		r.publish(t, inline(t, capture, dataRecord(3, old), nil), nil)
		r.step(t) // V3 parked
		v3 := versions(t, c, capture)[1].Version
		if s, err := c.EvictParked(ctx, &v3); err != nil || s != catalog.VersionEvictedHour {
			t.Fatalf("EvictParked = %v, %v", s, err)
		}
		term, err := r.admin().CaptureTerminate(ctx, capture)
		if err != nil {
			t.Fatal(err)
		}
		r.publish(t, inline(t, capture, dataRecord(4, now), nil), nil) // V4, after the cut-off
		if err := v2.NakWithDelay(0); err != nil {
			t.Fatal(err)
		}
		r.step(t)
		pg := r.parkedConsumer(t)
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}

		vs := versions(t, c, capture)
		if len(vs) != 4 {
			t.Fatalf("%d versions, want 4", len(vs))
		}
		for i, v := range vs[:3] {
			if v.State != catalog.VersionTerminated || v.TerminatedBy != term.ActionID {
				t.Errorf("V%d = %s by %q, want terminated by %s", i+1, v.State, v.TerminatedBy, term.ActionID)
			}
		}
		if vs[3].State != catalog.VersionParked {
			t.Errorf("V4 = %s, want parked as new traffic of a pending capture", vs[3].State)
		}
		if n := r.pending(t); n != 0 {
			t.Errorf("the traffic stream holds %d messages, want none", n)
		}
		if n := msgs(t, r.parked(t)); n != 1 {
			t.Errorf("the parking stream holds %d copies, want V4's only", n)
		}
		es, err := c.AuditEntries(ctx, capture)
		if err != nil {
			t.Fatal(err)
		}
		acks := 0
		for _, e := range es {
			if e.Action == ReasonTerminated && e.ActionID == term.ActionID {
				acks++
			}
		}
		if acks != 3 {
			t.Errorf("%d acknowledgements under the termination, want V2's original and the V1 and V3 copies", acks)
		}
	})
}

// stage stages d's version as a parked record registered with its evidence.
func stage(t *testing.T, c *catalog.Catalog, d *Delivery) {
	t.Helper()
	ctx := t.Context()
	if _, err := c.BeginParking(ctx, d.identity(), ReasonUnknownCapture, "", 0); err != nil {
		t.Fatal(err)
	}
	id := newPackID()
	seq := d.Raw.Version.Seq
	p := &catalog.Pack{
		ID: id, Capture: d.Raw.Version.Capture, ToolID: "T1", Hour: HourOf(d.Raw.TS), Role: catalog.RoleSegment, Key: "staging/" + id.String(),
		Size: 1, SeqStart: seq, FormatMinor: 1, Ranges: []catalog.Range{{SeqFrom: seq, SeqTo: seq, TSFrom: d.Raw.TS, TSTo: d.Raw.TS}},
	}
	ev := &catalog.Evidence{Deliveries: []catalog.Identity{*d.identity()}}
	if out, err := c.RegisterPack(ctx, p, catalog.Registration{Evidence: ev}); err != nil || out != catalog.Registered {
		t.Fatalf("RegisterPack = %v, %v", out, err)
	}
}

// Checklist 2: a start already staged, terminated or covered by an order is acknowledged from the raw message alone,
// whether its descriptor does not decode or its claim object is missing, and admits nothing;
// a message whose version cannot be read is terminated as malformed and fabricates no version and no capture.
func TestResolutionInputs(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		cl := newClaims()
		x := executor(t, c, cl)
		n := byte(60)
		starts := map[string]func(capture tracepack.UUID) Message{
			"undecodable descriptor": func(capture tracepack.UUID) Message {
				m := inline(t, capture, startRecord(t, now), nil)
				m.Env.Descriptor = "not base64!"
				return m
			},
			"no descriptor header": func(capture tracepack.UUID) Message {
				return inline(t, capture, startRecord(t, now), nil) // a start whose message carries no descriptor (checklist 2)
			},
			"claim object missing": func(capture tracepack.UUID) Message {
				m := claimCheck(t, cl, capture, startRecord(t, now), descriptorOf(t, "eqp"))
				cl.mu.Lock()
				delete(cl.objs, m.Env.MsgID)
				cl.mu.Unlock()
				return m
			},
		}
		resolutions := map[string]struct {
			resolve func(capture tracepack.UUID, d *Delivery)
			reason  string
		}{
			"staged": {func(_ tracepack.UUID, d *Delivery) { stage(t, c, d) }, ReasonAlreadyStaged},
			"terminated": {func(capture tracepack.UUID, d *Delivery) {
				if _, err := c.BeginParking(ctx, d.identity(), ReasonUnknownCapture, "", 0); err != nil {
					t.Fatal(err)
				}
				if _, err := c.TerminateParked(ctx, capture, nil, "op", "p"); err != nil {
					t.Fatal(err)
				}
			}, ReasonTerminated},
			"covered by an order": {func(capture tracepack.UUID, _ *Delivery) {
				if _, err := c.TerminateCapture(ctx, capture, 1, 100, "op", "o"+capture.String()); err != nil {
					t.Fatal(err)
				}
			}, ReasonTerminated},
		}
		for sname, start := range starts {
			for rname, res := range resolutions {
				n++
				capture := tracepack.UUID{n}
				m := start(capture)
				res.resolve(capture, delivered(m))
				if _, dec := dispose(t, x, m); dec.Action != ActAck || dec.Reason != res.reason {
					t.Errorf("%s start, %s: %+v, want acknowledged as %s", sname, rname, dec, res.reason)
				}
				if got, err := c.Capture(ctx, capture); err != nil || got.State != catalog.StatePending || got.Descriptor != nil {
					t.Errorf("%s start, %s: capture %+v, %v; want the placeholder untouched", sname, rname, got, err)
				}
			}
		}

		malformed := map[string]func(m *Message){
			"claim reference without digest":    func(m *Message) { m.Env.PayloadRef = bucket + "/" + m.Env.MsgID },
			"malformed digest":                  func(m *Message) { m.Env.PayloadRef, m.Env.PayloadDigest = bucket+"/"+m.Env.MsgID, "SHA-256=xyz" },
			"digest without reference":          func(m *Message) { m.Env.PayloadDigest = tracepack.BusPayloadDigest([]byte("x")) },
			"malformed Nats-Msg-Id":             func(m *Message) { m.Env.MsgID = "x" },
			"body shorter than a record header": func(m *Message) { m.Body = m.Body[:43] },
		}
		for name, change := range malformed {
			n++
			capture := tracepack.UUID{n}
			m := inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp"))
			change(&m)
			if _, dec := dispose(t, x, m); dec.Action != ActTerminate || dec.Reason != ReasonMalformed {
				t.Errorf("%s: %+v, want terminated as malformed", name, dec)
			}
			if _, err := c.Capture(ctx, capture); !errors.Is(err, catalog.ErrUnknownCapture) {
				t.Errorf("%s: a capture row was written (%v)", name, err)
			}
			if vs := versions(t, c, capture); len(vs) != 0 {
				t.Errorf("%s: versions %+v fabricated", name, vs)
			}
		}

		// A record with no payload is a body of exactly one record header: it passes the length check and is read as any other.
		empty := dataRecord(1, now)
		empty.Payload = nil
		m := inline(t, tracepack.UUID{n + 1}, empty, nil)
		if len(m.Body) != headerLen {
			t.Fatalf("body of %d bytes, want %d", len(m.Body), headerLen)
		}
		if _, dec := dispose(t, x, m); dec.Action != ActNak || dec.Reason != ReasonUnknownCapture {
			t.Errorf("a body of exactly 44 bytes = %+v, want read as a record of an unknown capture", dec)
		}
	})
}

// Checklist 4: a message of a version terminated before is acknowledged under the termination even above an order's cut-off,
// while an unresolved version above the cut-off is new traffic.
func TestTerminatedVersionAboveTheCutOff(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		x := executor(t, c, newClaims())
		capture := tracepack.UUID{80}
		parked := inline(t, capture, dataRecord(1, now), nil)
		if _, err := c.BeginParking(ctx, delivered(parked).identity(), ReasonUnknownCapture, "", 0); err != nil {
			t.Fatal(err)
		}
		if _, err := c.TerminateCapture(ctx, capture, 1, 100, "op", "c1"); err != nil {
			t.Fatal(err)
		}
		late := parked
		late.StreamSeq = 300
		if _, dec := dispose(t, x, late); dec.Action != ActAck || dec.Reason != ReasonTerminated {
			t.Errorf("a terminated version above the cut-off = %+v, want acknowledged under the termination", dec)
		}
		other := inline(t, capture, dataRecord(2, now), nil)
		other.StreamSeq = 301
		if _, dec := dispose(t, x, other); dec.Action != ActPark || dec.Reason != ReasonUnknownCapture {
			t.Errorf("an unresolved version above the cut-off = %+v, want handled as new traffic", dec)
		}
	})
}
