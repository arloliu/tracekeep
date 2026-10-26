package ingest

import (
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/classify"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

const now = int64(100 * time.Hour)

const maxFrameLen = 1 << 20

func testConfig() Config {
	return Config{Classifier: classify.NewNamed(classify.Name, maxFrameLen), Name: classify.Name, MaxFrameLen: maxFrameLen}
}

// executor runs on c with one configuration, executed by a leased member, so admissions bind it.
func executor(t *testing.T, c *catalog.Catalog, cl *claims) *Executor {
	t.Helper()
	cfg, err := c.ActivateConfig(t.Context(), []byte("phase-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Register(t.Context(), "r", []int64{cfg}, time.Minute); err != nil {
		t.Fatal(err)
	}

	return &Executor{
		Catalog: c, Claims: cl, Incarnation: 1,
		Horizon: func() int64 { return now - int64(24*time.Hour) },
		Configs: map[int64]Config{cfg: testConfig()},
	}
}

func dispose(t *testing.T, x *Executor, m Message) (*Delivery, Decision) {
	t.Helper()
	d, dec, err := x.Dispose(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}

	return d, dec
}

func state(t *testing.T, c *catalog.Catalog, id tracepack.UUID) string {
	t.Helper()
	got, err := c.Capture(t.Context(), id)
	if err != nil {
		return "none"
	}

	return got.State
}

// A start admits its capture, after which its records are ingested; before it they wait.
func TestDisposeAdmitsOnTheStart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		x := executor(t, c, newClaims())
		capture := tracepack.UUID{1}
		if _, dec := dispose(t, x, inline(t, capture, dataRecord(1, now), nil)); dec.Action != ActNak || dec.Reason != ReasonUnknownCapture {
			t.Fatalf("a record before its start = %+v, want a nak for the unknown capture", dec)
		}
		d, dec := dispose(t, x, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		if dec.Action != ActBatch || state(t, c, capture) != catalog.StateOpen || d.ConfigID == 0 {
			t.Fatalf("the start = %+v, capture %s, config %d; want batched, open and bound", dec, state(t, c, capture), d.ConfigID)
		}
		d, dec = dispose(t, x, inline(t, capture, dataRecord(2, now), nil))
		if dec.Action != ActBatch || d.Record.Record.DecodeStatus == tracepack.DecodeStatus(0) {
			t.Errorf("a record after the start = %+v, decode status %v; want batched and classified", dec, d.Record.Record.DecodeStatus)
		}
	})
}

// A start whose descriptor this service does not accept rejects its capture durably and stays deferred;
// its records wait whatever their age.
func TestDisposeRejectsAnUnacceptedStart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		x := executor(t, c, newClaims())
		capture := tracepack.UUID{2}
		m := inline(t, capture, startRecord(t, now), descriptorOf(t, " "))
		if _, dec := dispose(t, x, m); dec.Action != ActPark || dec.Reason != ReasonCaptureRejected || !dec.Alert {
			t.Fatalf("an unaccepted start = %+v, want parked with the capture-rejected alert", dec)
		}
		if s := state(t, c, capture); s != catalog.StateRejected {
			t.Fatalf("capture %s, want rejected", s)
		}
		old := inline(t, capture, dataRecord(1, now-int64(48*time.Hour)), nil)
		if _, dec := dispose(t, x, old); dec.Action != ActPark || dec.Reason != ReasonDeferredAged {
			t.Errorf("an expired record of the rejected capture = %+v, want parked", dec)
		}
	})
}

// A start whose descriptor does not decode at all is rejected with the header as it came.
func TestDisposeRejectsAnUndecodableStart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		x := executor(t, c, newClaims())
		capture := tracepack.UUID{3}
		m := inline(t, capture, startRecord(t, now), nil)
		m.Env.Descriptor = "not base64!"
		if _, dec := dispose(t, x, m); dec.Action != ActPark || dec.Reason != ReasonCaptureRejected {
			t.Fatalf("an undecodable start = %+v, want rejected and parked", dec)
		}
		got, _ := c.Capture(t.Context(), capture)
		if got.State != catalog.StateRejected || string(got.Descriptor) != "not base64!" {
			t.Errorf("capture %+v, want rejected keeping the header", got)
		}
	})
}

// A claim-check start of an unknown capture is fetched and checked before anything else decides,
// since only its payload shows that it is a start.
func TestDisposeFetchesAClaimCheckStart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		cl := newClaims()
		x := executor(t, c, cl)
		capture := tracepack.UUID{4}
		d, dec := dispose(t, x, claimCheck(t, cl, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		if dec.Action != ActBatch || state(t, c, capture) != catalog.StateOpen || d.Claim == nil {
			t.Fatalf("a claim-check start = %+v, capture %s; want admitted and batched with its payload", dec, state(t, c, capture))
		}
		missing := claimCheck(t, cl, capture, dataRecord(1, now), nil)
		cl.mu.Lock()
		delete(cl.objs, missing.Env.MsgID)
		cl.mu.Unlock()
		if _, dec := dispose(t, x, missing); dec.Action != ActNak || dec.Reason != ReasonClaimMissing {
			t.Errorf("a claim-check record whose object is gone, with no receipt = %+v, want a nak", dec)
		}
	})
}

// An expired start of an unknown capture is admitted first, then terminated as beyond retention with an audit row.
func TestDisposeExpiredStart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		x := executor(t, c, newClaims())
		capture := tracepack.UUID{5}
		d, dec := dispose(t, x, inline(t, capture, startRecord(t, now-int64(48*time.Hour)), descriptorOf(t, "eqp")))
		if dec.Action != ActTerminate || dec.Reason != ReasonBeyondRetention || !dec.Audit || state(t, c, capture) != catalog.StateOpen {
			t.Fatalf("an expired start = %+v, capture %s; want admitted, then terminated with an audit row", dec, state(t, c, capture))
		}
		if err := x.AuditTermination(t.Context(), d, dec.Reason); err != nil {
			t.Fatal(err)
		}
	})
}

// A start of an admitted capture with other descriptor bytes is a conflict: alert and nak, never overwritten.
func TestDisposeConflictingStart(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		x := executor(t, c, newClaims())
		capture := tracepack.UUID{6}
		dispose(t, x, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		if _, dec := dispose(t, x, inline(t, capture, startRecord(t, now), descriptorOf(t, "other"))); dec.Action != ActNak || dec.Reason != ReasonConflict {
			t.Errorf("a start with other bytes = %+v, want a descriptor conflict", dec)
		}
	})
}

// A record bound to a configuration this replica does not execute is deferred, never processed with a fallback.
func TestDisposeIncompatibleConfiguration(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		x := executor(t, c, newClaims())
		capture := tracepack.UUID{7}
		dispose(t, x, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		x.Configs = map[int64]Config{}
		if _, dec := dispose(t, x, inline(t, capture, dataRecord(1, now), nil)); dec.Action != ActNak || dec.Reason != ReasonIncompatible {
			t.Errorf("a record of an unexecuted configuration = %+v, want deferred", dec)
		}
	})
}

// An admission or a rejection that loses to a termination committed after the delivery's resolution was read
// returns terminated: the start is acknowledged under the termination, its capture neither admitted nor rejected (T-32).
func TestAdmissionLosesToATermination(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		device string
		step   Step
	}{{"admission", "eqp", StepAdmit}, {"rejection", " ", StepReject}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				x := executor(t, c, newClaims())
				capture := tracepack.UUID{11}
				d := &Delivery{Msg: inline(t, capture, startRecord(t, now), descriptorOf(t, tc.device)), Incarnation: x.Incarnation}
				d.Raw, d.Facts = ReadRaw(&d.Msg.Env, d.Msg.Body, x.Horizon())
				for _, s := range []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor} {
					if err := x.run(t.Context(), d, s); err != nil {
						t.Fatal(err)
					}
				}
				if dec := Decide(&d.Facts); dec.Step != tc.step {
					t.Fatalf("Decide = %+v, want step %d", dec, tc.step)
				}
				if _, err := c.TerminateCapture(t.Context(), capture, x.Incarnation, 100, "op", "c1"); err != nil {
					t.Fatal(err)
				}
				if err := x.run(t.Context(), d, tc.step); err != nil {
					t.Fatal(err)
				}
				if dec := Decide(&d.Facts); dec.Action != ActAck || dec.Reason != ReasonTerminated {
					t.Errorf("Decide = %+v, want acknowledged under the termination", dec)
				}
				if s := state(t, c, capture); s != catalog.StatePending {
					t.Errorf("capture %s, want the placeholder only", s)
				}
			})
		})
	}
}

// A record of an unknown capture waits with a nak while the catalog has no row for it,
// and is parked once an earlier parking left the capture pending;
// a claim-check record whose object is missing is parked once its nak delay reached the last step.
func TestDisposeParksIndefiniteDeferrals(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		cl := newClaims()
		x := executor(t, c, cl)
		capture := tracepack.UUID{12}
		first := inline(t, capture, dataRecord(1, now), nil)
		if _, dec := dispose(t, x, first); dec.Action != ActNak || dec.Reason != ReasonUnknownCapture {
			t.Fatalf("a record of a capture with no row = %+v, want a nak", dec)
		}
		d := delivered(first)
		if _, err := c.BeginParking(ctx, d.identity(), ReasonUnknownCapture, "", 0); err != nil {
			t.Fatal(err)
		}
		if _, dec := dispose(t, x, inline(t, capture, dataRecord(2, now), nil)); dec.Action != ActPark || dec.Reason != ReasonUnknownCapture {
			t.Errorf("a record of a pending capture = %+v, want parked", dec)
		}

		missing := claimCheck(t, cl, tracepack.UUID{13}, dataRecord(1, now), nil)
		cl.mu.Lock()
		delete(cl.objs, missing.Env.MsgID)
		cl.mu.Unlock()
		for delivered, want := range map[uint64]Action{1: ActNak, 5: ActPark} {
			m := missing
			m.Delivered = delivered
			if _, dec := dispose(t, x, m); dec.Action != want || dec.Reason != ReasonClaimMissing {
				t.Errorf("delivery %d of a record whose object is missing = %+v, want %v", delivered, dec, want)
			}
		}
	})
}

// An order's application that finds the version staged meanwhile leaves it staged: the delivery is acknowledged as already staged.
func TestApplyOrderFindsTheVersionStaged(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		x := executor(t, c, newClaims())
		capture := tracepack.UUID{14}
		if _, dec := dispose(t, x, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp"))); dec.Action != ActBatch {
			t.Fatalf("start = %+v", dec)
		}
		m := inline(t, capture, dataRecord(1, now), nil)
		late := delivered(m)
		late.Msg.StreamSeq = 200 // above the cut-off: parked, then staged
		if _, err := c.BeginParking(ctx, late.identity(), ReasonWriteFailure, "", 0); err != nil {
			t.Fatal(err)
		}
		id := tracepack.UUID{0x70, 14}
		p := &catalog.Pack{
			ID: id, Capture: capture, ToolID: "T1", Hour: HourOf(now), Role: catalog.RoleSegment, Key: "staging/" + id.String(), Size: 1,
			SeqStart: 1, FormatMinor: 1, Ranges: []catalog.Range{{SeqFrom: 1, SeqTo: 1, TSFrom: now, TSTo: now}},
		}
		ev := &catalog.Evidence{Deliveries: []catalog.Identity{*late.identity()}}
		if out, err := c.RegisterPack(ctx, p, catalog.Registration{Evidence: ev}); err != nil || out != catalog.Registered {
			t.Fatalf("RegisterPack = %v, %v", out, err)
		}
		if _, err := c.TerminateCapture(ctx, capture, x.Incarnation, 100, "op", "c1"); err != nil {
			t.Fatal(err)
		}
		d := delivered(m) // stream seq 2, below the cut-off
		if err := x.run(ctx, d, StepApplyOrder); err != nil || d.Facts.Resolution != ResolvedStaged {
			t.Errorf("order application = %v, %v; want the version found staged", d.Facts.Resolution, err)
		}
	})
}
