package producer

import (
	"testing"

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
