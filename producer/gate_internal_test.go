package producer

import (
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/producer/internal/core"
)

// startsAfter drains the reserved lane and returns the starts of captures other than c.
func startsAfter(p *Producer, c *core.Capture) []core.Entry {
	var starts []core.Entry
	for {
		select {
		case e := <-p.rec.Reserved():
			if e.Kind == core.EntryStart && e.Cap != c {
				starts = append(starts, e)
			}
		default:
			return starts
		}
	}
}

// End runs while a fault rotation is between its two lifecycle-lock sections: the fault rotation then installs nothing.
func TestEndInsideAFaultRotationsGap(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	nextReserved(t, p)
	c.Fault()
	p.faultGap = func() { p.End(t.Context()) }

	p.handleFault(c)
	if cur := p.rec.Current(); cur != nil {
		t.Errorf("a fault rotation installed %v after End", cur.ID)
	}
	if s := startsAfter(p, c); len(s) != 0 {
		t.Errorf("%d starts queued after End, want none", len(s))
	}
}

// A HotReload rotates while a fault rotation of the same capture is between its two lifecycle-lock sections:
// the fault rotation then finds its capture no longer current and installs nothing more.
func TestFaultRotationRacingAHotReload(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	nextReserved(t, p)
	c.Fault()
	reloaded := testDescriptor()
	reloaded.DeviceName = "eqp-reloaded"
	p.faultGap = func() {
		if err := p.Rotate(t.Context(), reloaded); err != nil {
			t.Errorf("HotReload: %v", err)
		}
	}

	p.handleFault(c)
	starts := startsAfter(p, c)
	if len(starts) != 1 || p.rec.Current() != starts[0].Cap {
		t.Fatalf("%d starts after the fault, current %v; want the HotReload's capture alone", len(starts), p.rec.Current())
	}
	if prev := previousOf(t, starts[0].Cap); prev == nil || *prev != c.ID {
		t.Errorf("previous_capture_id = %v, want the faulted capture", prev)
	}
}

// A late record of an abandoned capture that reached the publisher before End's drain request is held with its capture;
// whatever then ends the capture, End's termination or the publisher stopping at Close, drops it as abandoned.
func TestHeldRecordOfAnAbandonedCaptureIsDroppedAsAbandoned(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		end  func(pb *publisher, c *core.Capture)
	}{
		{"End's termination", func(pb *publisher, c *core.Capture) {
			pb.end([]*core.Capture{c})
			pb.takeEnds()
			pb.finishEnds()
		}},
		{"Close", func(pb *publisher, _ *core.Capture) { pb.stop() }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := newTestProducer(t, Tunables{})
			if err := p.Begin(t.Context(), testDescriptor()); err != nil {
				t.Fatal(err)
			}
			c := p.rec.Current()
			pb := newPublisher(p, newRecBus())
			wall, mono := core.Now()
			p.rec.Observe(&core.Observation{Wall: wall, Mono: mono, Socket: 1, Kind: tracepack.KindData, Payload: []byte{1}})
			p.rec.Abandon(&p.gen) // End passed its deadline while the callback held the lock

			pb.add(<-p.rec.Ordinary()) // the record, before End's request
			tt.end(pb, c)
			if !c.Terminated() {
				t.Fatalf("the abandoned capture is not terminated")
			}
			// Close also drops the capture's unpublished start, which is abandoned too.
			if pb.dropped[core.DropAbandoned] == 0 || pb.dropped[core.DropPublishFailed] != 0 {
				t.Errorf("dropped abandoned %d, publish_failed %d; want every loss as abandoned",
					pb.dropped[core.DropAbandoned], pb.dropped[core.DropPublishFailed])
			}
		})
	}
}

// Two callbacks step the clock in opposite directions while the first is stalled before queuing its step:
// the publisher holds back the second step, a reserved record, and the record above it until the first step is queued.
func TestOpposingStepsHoldBackTheReservedLane(t *testing.T) {
	t.Parallel()
	bus := newRecBus()
	passes := make(chan struct{}, 64)
	hooks := publisherHooks{afterPass: func() {
		select {
		case passes <- struct{}{}:
		default:
		}
	}}
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, WithBus(bus), func(o *options) { o.hooks = hooks })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	c := p.rec.Current()
	bus.waitFor(t, 1) // the start

	// Callback A steps forward and stalls before queuing its step (seq 1) and its record (seq 2);
	// callback B, the wall back on the old anchor, steps backward (seq 3) and queues its record (seq 4).
	wall, mono := core.Now()
	a, _ := c.Allocate(wall+3*int64(time.Second), mono, 1)
	p.rec.Observe(&core.Observation{Wall: wall, Mono: mono + 1, Socket: 1, Kind: tracepack.KindData, Payload: []byte{1}})
	for range 5 {
		<-passes
	}
	if ids := bus.published(); len(ids) != 1 {
		t.Fatalf("published %v while the forward step is unqueued, want only the start", ids)
	}

	p.rec.PushReserved(core.Entry{Cap: c, Kind: core.EntryClockStep, Seq: a.StepSeq, Drift: 3 * int64(time.Second)})
	c.Retire(a.StepSeq)
	ids := bus.waitFor(t, 4)
	want := []string{c.ID.String() + ".0", c.ID.String() + ".1", c.ID.String() + ".3", c.ID.String() + ".4"}
	if !slices.Equal(ids, want) {
		t.Errorf("published %v, want %v", ids, want)
	}
}
