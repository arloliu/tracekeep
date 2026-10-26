package ingest

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// The parking protocol's convergence schedules (docs/ROADMAP.md phase 1 gate and checklist item 6),
// each asserted on what the streams store, not only on the answers.

// refusing answers its first publishes with errors, storing nothing, then publishes.
type refusing struct {
	jetstream.JetStream
	errs []error
}

func (p *refusing) PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if len(p.errs) > 0 {
		err := p.errs[0]
		p.errs = p.errs[1:]

		return nil, err
	}

	return p.JetStream.PublishMsg(ctx, m, opts...)
}

func apiError(code jetstream.ErrorCode, desc string) error {
	return fmt.Errorf("nats: %w", &jetstream.APIError{Code: 400, ErrorCode: code, Description: desc})
}

// Checklist 6: zero and malformed 10071 descriptions, 10164 and a timeout confirm nothing:
// the original stays on the traffic stream and nothing is stored, until a publish is confirmed and the protocol converges.
func TestConfirmationFailuresConverge(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		parked := r.parked(t)
		failures := []error{
			apiError(10071, "wrong last sequence: 0"), apiError(10071, "wrong last sequence: x"),
			apiError(10164, "wrong last sequence"), context.DeadlineExceeded,
		}
		k := &Coordinator{Catalog: c, Bus: &refusing{JetStream: r.js, errs: slices.Clone(failures)}, ParkPrefix: busconfig.DefaultParkPrefix}
		capture := tracepack.UUID{90}
		r.publish(t, inline(t, capture, dataRecord(1, now), nil), nil)
		for i := range failures {
			d, m := r.fetch(t)
			p, err := k.Park(ctx, Entry{D: d, Msg: m}, ReasonUnknownCapture, 0)
			if err != nil || p.Confirmation != Unconfirmed {
				t.Fatalf("attempt %d (%v) = %+v, %v; want unconfirmed", i+1, failures[i], p, err)
			}
			if n := msgs(t, parked); n != 0 || r.pending(t) != 1 {
				t.Fatalf("attempt %d: %d copies stored, %d originals; want none and the original kept", i+1, n, r.pending(t))
			}
			if err := m.NakWithDelay(0); err != nil {
				t.Fatal(err)
			}
		}
		d, m := r.fetch(t)
		if p, err := k.Park(ctx, Entry{D: d, Msg: m}, ReasonUnknownCapture, 0); err != nil || p.Confirmation != Confirmed {
			t.Fatalf("the last attempt = %+v, %v; want confirmed", p, err)
		}
		if n := msgs(t, parked); n != 1 || r.pending(t) != 0 {
			t.Errorf("%d copies stored, %d originals; want one copy and the original acknowledged", n, r.pending(t))
		}
	})
}

// A publish the duplicate window suppressed stored nothing, and the same publish after the window stores the copy:
// on a standalone stream and on a cluster, whose check orders differ.
func TestSuppressedPublishConverges(t *testing.T) {
	t.Parallel()
	for _, clustered := range []bool{false, true} {
		t.Run(map[bool]string{false: "standalone", true: "clustered"}[clustered], func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			js, parked := parkingBus(t, clustered)
			// A short window, so the scenario can outwait it.
			cfg := parked.CachedInfo().Config
			cfg.Duplicates = 500 * time.Millisecond
			if _, err := js.UpdateStream(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			k := &Coordinator{Bus: js, ParkPrefix: busconfig.DefaultParkPrefix}
			capture := tracepack.UUID{91}
			a := delivered(transported(t, inline(t, capture, dataRecord(1, now), nil)))
			b := delivered(transported(t, otherVersion(t, capture, 1)))
			if conf, err := k.Publish(ctx, a, catalog.VersionParking); err != nil || conf != Confirmed {
				t.Fatalf("Publish of the first version = %v, %v", conf, err)
			}
			first := stored(t, parked, mustSubject(t, a))
			if err := parked.DeleteMsg(ctx, first.Sequence); err != nil { // its copy acknowledged and removed
				t.Fatal(err)
			}
			if conf, err := k.Publish(ctx, b, catalog.VersionParking); err != nil || conf != Suppressed {
				t.Fatalf("Publish of the second version inside the window = %v, %v; want suppressed", conf, err)
			}
			if stored(t, parked, mustSubject(t, b)) != nil {
				t.Fatal("a suppressed publish stored a copy")
			}
			time.Sleep(4 * cfg.Duplicates) // the scenario: the original waits out the window before its next delivery
			if conf, err := k.Publish(ctx, b, catalog.VersionParking); err != nil || conf != Confirmed {
				t.Fatalf("Publish after the window = %v, %v; want confirmed", conf, err)
			}
			if cp := stored(t, parked, mustSubject(t, b)); cp == nil || string(cp.Data) != string(b.Msg.Body) {
				t.Errorf("stored copy %+v, want the second version's", cp)
			}
		})
	}
}

func mustSubject(t *testing.T, d *Delivery) string {
	t.Helper()
	s, err := parkSubject(busconfig.DefaultParkPrefix, d.Msg.Subject, d.Raw.Version)
	if err != nil {
		t.Fatal(err)
	}

	return s
}

// Two versions of one seq parked inside one duplicate window, inline and claim-check (the gate's case):
// the first copy is stored and its original acknowledged, the second publish stores nothing and its original stays unacknowledged;
// neither is lost.
func TestTwoVersionsInOneWindow(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"inline", "claim-check"} {
		for _, clustered := range []bool{false, true} {
			t.Run(kind+"/"+map[bool]string{false: "standalone", true: "clustered"}[clustered], func(t *testing.T) {
				t.Parallel()
				twoVersionsInOneWindow(t, kind == "claim-check", clustered)
			})
		}
	}
}

func twoVersionsInOneWindow(t *testing.T, claim, clustered bool) {
	t.Helper()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		js, parked := parkingBus(t, clustered)
		k := &Coordinator{Catalog: c, Bus: js, ParkPrefix: busconfig.DefaultParkPrefix}
		cl := newClaims()
		capture := tracepack.UUID{92}
		other := dataRecord(1, now)
		other.Payload = append(slices.Clone(other.Payload), 0xff)
		message := func(r tracepack.Record) Message {
			if claim {
				return claimCheck(t, cl, capture, r, nil)
			}
			return inline(t, capture, r, nil)
		}
		reason := ReasonUnknownCapture
		if claim {
			reason = ReasonClaimMissing
		}
		a := delivered(transported(t, message(dataRecord(1, now))))
		b := delivered(transported(t, message(other)))
		b.Msg.StreamSeq++ // another message of the traffic stream
		ha, hb := &msg{}, &msg{}
		pa, err := k.Park(ctx, Entry{D: a, Msg: ha}, reason, 0)
		if err != nil || pa.Confirmation != Confirmed || !slices.Equal(ha.done(), []string{"double-ack"}) {
			t.Fatalf("Park of the first version = %+v, %v, %v", pa, err, ha.done())
		}
		pb, err := k.Park(ctx, Entry{D: b, Msg: hb}, reason, 0)
		if err != nil || pb.Confirmation != Suppressed || len(hb.done()) != 0 {
			t.Fatalf("Park of the second version = %+v, %v, %v; want suppressed, unacknowledged", pb, err, hb.done())
		}
		if stored(t, parked, mustSubject(t, a)) == nil || stored(t, parked, mustSubject(t, b)) != nil || msgs(t, parked) != 1 {
			t.Errorf("the parking stream holds %d copies; want the first version's only", msgs(t, parked))
		}
		vs := versions(t, c, capture)
		if len(vs) != 2 || vs[0].State == vs[1].State {
			t.Errorf("versions %+v; want one parked, the other still parking", vs)
		}
	})
}

// The protocol's crash points on the original's side: after the PubAck and before the advance,
// and after the advance and before the original's acknowledgement. The redelivered original is confirmed by the stored copy,
// which stays the only one, and acknowledged.
func TestParkingCrashPoints(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		k := r.g.Coordinator
		parked := r.parked(t)

		r.publish(t, inline(t, tracepack.UUID{93}, dataRecord(1, now), nil), nil)
		d, m := r.fetch(t)
		b, err := k.Begin(ctx, d, ReasonUnknownCapture, 0)
		if err != nil {
			t.Fatal(err)
		}
		if conf, err := k.Publish(ctx, d, b.State); err != nil || conf != Confirmed {
			t.Fatalf("Publish = %v, %v", conf, err)
		}
		if err := m.NakWithDelay(0); err != nil { // the process stops between the PubAck and the advance
			t.Fatal(err)
		}
		d, m = r.fetch(t)
		if p, err := k.Park(ctx, Entry{D: d, Msg: m}, ReasonUnknownCapture, 0); err != nil || p.Confirmation != Confirmed {
			t.Fatalf("Park at the redelivery = %+v, %v", p, err)
		}

		r.publish(t, inline(t, tracepack.UUID{94}, dataRecord(1, now), nil), nil)
		d, m = r.fetch(t)
		if _, err := k.Park(ctx, Entry{D: d, Msg: failingAck{m}}, ReasonUnknownCapture, 0); err == nil {
			t.Fatal("Park whose acknowledgement failed returned no error")
		}
		if vs := versions(t, c, tracepack.UUID{94}); len(vs) != 1 || vs[0].State != catalog.VersionParked {
			t.Fatalf("versions %+v; want parked before the acknowledgement", vs)
		}
		if err := m.NakWithDelay(0); err != nil {
			t.Fatal(err)
		}
		d, m = r.fetch(t)
		if p, err := k.Park(ctx, Entry{D: d, Msg: m}, ReasonUnknownCapture, 0); err != nil || p.Confirmation != Confirmed {
			t.Fatalf("Park at the redelivery = %+v, %v", p, err)
		}
		if n := msgs(t, parked); n != 2 || r.pending(t) != 0 {
			t.Errorf("%d copies, %d originals; want one copy each and the originals acknowledged", n, r.pending(t))
		}
	})
}

// The crash point on the parked consumer's side: after the staging commit and before the copy's acknowledgement.
// The redelivered copy is acknowledged from the staged row.
func TestParkedConsumerCrashAfterStaging(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{95}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.step(t)
		r.publish(t, inline(t, capture, dataRecord(1, now), nil), nil)
		d, m := r.fetch(t)
		if p, err := r.g.Coordinator.Park(ctx, Entry{D: d, Msg: m}, ReasonWriteFailure, 0); err != nil || p.Confirmation != Confirmed {
			t.Fatalf("Park = %+v, %v", p, err)
		}
		pg := r.parkedConsumer(t)
		pg.RedeliveryDelay = time.Millisecond
		pg.Flusher.hooks[hookAfterRegister] = func() error {
			pg.Flusher.hooks[hookAfterRegister] = nil
			return errCrash
		}
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		r.clk.flushInterval()
		if err := pg.Step(ctx); err != nil { // registered, then the crash before the acknowledgement
			t.Fatal(err)
		}
		if vs := versions(t, c, capture); len(vs) != 1 || vs[0].State != catalog.VersionStaged {
			t.Fatalf("versions %+v; want staged by the registration", vs)
		}
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if n := msgs(t, r.parked(t)); n != 0 {
			t.Errorf("the parking stream holds %d copies, want the copy acknowledged from the staged row", n)
		}
		if got := copyStates(versions(t, c, capture), pg.Incarnation.ID); !slices.Equal(got, []string{"acked"}) {
			t.Errorf("copy rows %v, want acked", got)
		}
	})
}

// A handler stalled past the ack wait (the gate's case): the original is redelivered to another handler, which parks it whole,
// its copy stored and the original acknowledged, and the parked consumer stages the copy; the stalled handler, waking past the
// duplicate window, publishes a second copy only once the first was acknowledged and its subject emptied, and that copy is
// acknowledged from the staged row, its claim object gone; the stalled handler's late acknowledgement brings nothing back.
func TestLateSecondCopy(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{96}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		r.step(t)
		r.clk.flushInterval()
		r.step(t) // the start staged and acknowledged before the ack wait is shortened
		cons, err := bus.Consumer(ctx, r.stream, r.g.Incarnation, bus.ConsumerConfig{AckWait: 300 * time.Millisecond, MaxAckPending: 1000})
		if err != nil {
			t.Fatal(err)
		}
		r.g.Consumer = cons
		cl := newClaims()
		m := claimCheck(t, cl, capture, dataRecord(1, now), nil)
		r.publish(t, m, cl.objs[m.Env.MsgID])
		parked := r.parked(t)
		cfg := parked.CachedInfo().Config
		cfg.Duplicates = 500 * time.Millisecond // a short window, so the scenario can outwait it
		if _, err := r.js.UpdateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		k := r.g.Coordinator

		stalled, stalledMsg := r.fetch(t) // handler A begins parking, then stalls
		b, err := k.Begin(ctx, stalled, ReasonWriteFailure, 0)
		if err != nil {
			t.Fatal(err)
		}
		again, againMsg := r.fetch(t) // the ack wait passed: handler B receives the redelivery and parks it whole
		if md, _ := againMsg.Metadata(); md == nil || md.NumDelivered != 2 {
			t.Fatalf("second delivery %+v, want the redelivery", md)
		}
		if pk, err := k.Park(ctx, Entry{D: again, Msg: againMsg}, ReasonWriteFailure, 0); err != nil || pk.Confirmation != Confirmed {
			t.Fatalf("Park of the redelivery = %+v, %v", pk, err)
		}
		pg := r.parkedConsumer(t)
		for range 2 {
			if err := pg.Step(ctx); err != nil {
				t.Fatal(err)
			}
			r.clk.flushInterval()
		}
		if vs := versions(t, c, capture); len(vs) != 1 || vs[0].State != catalog.VersionStaged || msgs(t, r.parked(t)) != 0 {
			t.Fatalf("versions %+v, %d copies; want staged and the first copy acknowledged", vs, msgs(t, r.parked(t)))
		}

		// The stalled handler wakes past the window: its publish finds the subject empty and stores a second copy.
		time.Sleep(4 * cfg.Duplicates) // the scenario's delay
		if conf, err := k.Publish(ctx, stalled, b.State); err != nil || conf != Confirmed {
			t.Fatalf("the late publish = %v, %v", conf, err)
		}
		if err := r.claims.Delete(ctx, m.Env.MsgID); err != nil {
			t.Fatal(err)
		}
		if err := pg.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if n := msgs(t, r.parked(t)); n != 0 {
			t.Errorf("the parking stream holds %d copies, want the late copy acknowledged as already staged", n)
		}
		if err := stalledMsg.Ack(); err != nil {
			t.Fatal(err)
		}
		if err := r.nc.Flush(); err != nil {
			t.Fatal(err)
		}
		if n := r.pending(t); n != 0 {
			t.Errorf("the traffic stream holds %d messages after the stalled handler's late acknowledgement, want none", n)
		}
	})
}
