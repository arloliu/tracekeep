package core

import (
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// newForwardRecorder returns a recorder whose capture has seen socket 5 as epoch 1, with its frame drained.
func newForwardRecorder(t *testing.T) (*Recorder, *Capture) {
	t.Helper()
	r, c := newTestRecorder(t, RecorderParams{})
	o := frameObs(1, []byte{1})
	o.Socket = 5
	r.Observe(o)
	popOrdinary(t, r)

	return r, c
}

func TestForwardedTimeoutEmitsT3(t *testing.T) {
	t.Parallel()
	r, _ := newForwardRecorder(t)
	r.Pending.SetSocket(5)
	tok := r.Pending.Register(2, 77, "c")
	r.Pending.Bind(2, 77, 1, 3, 5)

	tok.Awaiting(45 * time.Second)
	tok.TimedOut(45 * time.Second)

	var e Entry
	select {
	case e = <-r.Ordinary():
	default:
		t.Fatalf("no T3 record")
	}
	want := Event{
		Kind: tracepack.EventTimerExpiry, Timer: tracepack.TimerT3,
		SessionID: 2, Stream: 1, Function: 3, SystemBytes: 77, Timeout: 45 * time.Second,
	}
	if e.RecKind != tracepack.KindTransportEvent || e.Epoch != 1 || e.Event != want {
		t.Fatalf("entry = %+v, want T3 %+v in epoch 1", e, want)
	}
}

func TestForwardedTimeoutEmitsNothingWhen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(r *Recorder) *PendingEntry
		reason TimeoutDropReason
	}{
		{"unbound", func(r *Recorder) *PendingEntry {
			return r.Pending.Register(2, 77, "c")
		}, TimeoutUnbound},
		{"ambiguous", func(r *Recorder) *PendingEntry {
			r.Pending.Register(2, 77, "c")
			tok := r.Pending.Register(2, 77, "c")
			r.Pending.Bind(2, 77, 1, 3, 5)

			return tok
		}, TimeoutAmbiguous},
		{"reconnected before the send", func(r *Recorder) *PendingEntry {
			tok := r.Pending.Register(2, 77, "c")
			r.Pending.SetSocket(6)
			r.Pending.Bind(2, 77, 1, 3, 6)

			return tok
		}, TimeoutAmbiguous},
		{"unattributed", func(r *Recorder) *PendingEntry {
			r.Pending.SetSocket(9)
			tok := r.Pending.Register(2, 77, "c")
			r.Pending.Bind(2, 77, 1, 3, 9) // a socket the current capture never saw

			return tok
		}, TimeoutUnattributed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, c := newForwardRecorder(t)
			r.Pending.SetSocket(5)
			tok := tt.setup(r)

			tok.TimedOut(time.Second)
			if n := len(r.Ordinary()); n != 0 {
				t.Fatalf("%d records, want none", n)
			}
			if n := r.Pending.Counters().TimeoutsDropped[tt.reason]; n != 1 {
				t.Fatalf("dropped timeouts for the reason %d, want 1", n)
			}
			// No seq was taken: the next record follows the drained frame.
			a, _ := c.Allocate(origin+2, 2, 5)
			if a.Seq != 2 {
				t.Fatalf("next seq %d, want 2", a.Seq)
			}
		})
	}
}

func TestReplyBeforeTimeoutEmitsNothing(t *testing.T) {
	t.Parallel()
	r, _ := newForwardRecorder(t)
	r.Pending.SetSocket(5)
	tok := r.Pending.Register(2, 77, "c")
	r.Pending.Bind(2, 77, 1, 3, 5)

	tok.Replied()
	tok.TimedOut(time.Second)
	if n := len(r.Ordinary()); n != 0 {
		t.Fatalf("%d records after a reply, want none", n)
	}
}

func TestForwardedTimeoutDoesNotAllocate(t *testing.T) {
	r, _ := newForwardRecorder(t)
	r.Pending.SetSocket(5)
	toks := make([]*PendingEntry, 0, 101)
	for i := range 101 {
		toks = append(toks, r.Pending.Register(2, uint32(i), "c")) //nolint:gosec // i < 101
		r.Pending.Bind(2, uint32(i), 1, 3, 5)                      //nolint:gosec // i < 101
	}
	i := 0
	n := testing.AllocsPerRun(100, func() {
		toks[i].TimedOut(time.Second)
		i++
		e := <-r.Ordinary()
		r.ReleaseOrdinary(&e)
	})
	if n != 0 {
		t.Fatalf("a timeout allocates %v times", n)
	}
}

// Bind runs in the wire observer, whose only allocation is the frame copy.
func TestBindDoesNotAllocate(t *testing.T) {
	pt, _ := newTestPending(t, 0)
	pt.Register(0x0102, 0x0a0b0c0d, "c")
	n := testing.AllocsPerRun(100, func() {
		pt.Bind(0x0102, 0x0a0b0c0d, 1, 3, 5)
		pt.Bind(0x0102, 0x0a0b0c0e, 1, 3, 5) // no entry
	})
	if n != 0 {
		t.Fatalf("Bind allocates %v times", n)
	}
}

// Only the first outbound frame binds: a later frame with the same key names no other stream or function.
func TestBindKeepsTheFirstFrame(t *testing.T) {
	t.Parallel()
	r, _ := newForwardRecorder(t)
	r.Pending.SetSocket(5)
	tok := r.Pending.Register(2, 77, "c")
	r.Pending.Bind(2, 77, 1, 3, 5)
	r.Pending.Bind(2, 77, 6, 11, 5)

	tok.TimedOut(time.Second)
	e := <-r.Ordinary()
	if e.Event.Stream != 1 || e.Event.Function != 3 {
		t.Fatalf("T3 names S%dF%d, want S1F3", e.Event.Stream, e.Event.Function)
	}
}
