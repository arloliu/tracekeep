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
	pt.SetSocket(5)
	for i := range 101 {
		pt.Register(0x0102, uint32(i), "c") //nolint:gosec // i < 101
	}
	i := 0
	n := testing.AllocsPerRun(100, func() {
		pt.Bind(0x0102, uint32(i), 1, 3, 5)  //nolint:gosec // i < 101
		pt.Bind(0x0102, 0x0a0b0c0e, 1, 3, 5) // no entry
		i++
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

// An outbound frame observed on the old socket after a reconnect still leaves the entry ambiguous.
func TestDelayedFrameOfTheOldSocketIsAmbiguous(t *testing.T) {
	t.Parallel()
	r, _ := newForwardRecorder(t)
	r.Pending.SetSocket(5)
	tok := r.Pending.Register(2, 77, "c")
	r.Pending.SetSocket(6)
	r.Pending.Bind(2, 77, 1, 3, 5)

	tok.TimedOut(time.Second)
	if n := len(r.Ordinary()); n != 0 {
		t.Fatalf("%d records, want none", n)
	}
	if n := r.Pending.Counters().TimeoutsDropped[TimeoutAmbiguous]; n != 1 {
		t.Fatalf("ambiguous timeouts %d, want 1", n)
	}
}

// A reconnected request is ambiguous before Bind records its socket,
// so a timeout held right after the binding, on a socket the capture maps, is dropped as ambiguous.
func TestTimeoutAfterAReconnectedBindIsAmbiguous(t *testing.T) {
	t.Parallel()
	r, _ := newForwardRecorder(t)
	o := frameObs(1, []byte{1})
	o.Socket = 6
	r.Observe(o)
	popOrdinary(t, r)
	r.Pending.SetSocket(5)
	tok := r.Pending.Register(2, 77, "c")
	r.Pending.SetSocket(6)
	held := 0
	r.Pending.bindHooks[0] = func() {
		held++
		if !tok.Ambiguous() || tok.Socket() != 0 {
			t.Errorf("before the binding: ambiguous %v, socket %d; want ambiguous and unbound", tok.Ambiguous(), tok.Socket())
		}
	}
	r.Pending.bindHooks[1] = func() {
		held++
		tok.TimedOut(time.Second)
	}
	r.Pending.Bind(2, 77, 1, 3, 6)

	if held != 2 {
		t.Fatalf("Bind reached %d of its hooks, want 2", held)
	}
	if n := len(r.Ordinary()); n != 0 {
		t.Fatalf("%d records from a timeout after the binding, want none", n)
	}
	if n := r.Pending.Counters().TimeoutsDropped[TimeoutAmbiguous]; n != 1 {
		t.Fatalf("ambiguous timeouts %d, want 1: %+v", n, r.Pending.Counters().TimeoutsDropped)
	}
}

// The first lookup of a fresh table, which a go-secs request with no registration makes, allocates nothing.
func TestFirstBindDoesNotAllocate(t *testing.T) {
	tables := make([]*PendingTable, 101) // AllocsPerRun's warm-up run takes one
	for i := range tables {
		tables[i], _ = newTestPending(t, 0)
	}
	i := 0
	n := testing.AllocsPerRun(100, func() {
		tables[i].Bind(1, 1, 1, 3, 5)
		i++
	})
	if n != 0 {
		t.Fatalf("the first Bind of a table allocates %v times", n)
	}
}

// T3 takes the epoch the current capture's map holds for the bound socket, not a fixed one.
func TestForwardedTimeoutTakesTheSocketsEpoch(t *testing.T) {
	t.Parallel()
	r, _ := newTestRecorder(t, RecorderParams{})
	for _, s := range []uint64{4, 5} {
		o := frameObs(1, []byte{1})
		o.Socket = s
		r.Observe(o)
		popOrdinary(t, r)
	}
	r.Pending.SetSocket(5)
	tok := r.Pending.Register(2, 77, "c")
	r.Pending.Bind(2, 77, 1, 3, 5)

	tok.TimedOut(time.Second)
	if e := <-r.Ordinary(); e.Epoch != 2 {
		t.Fatalf("T3 in epoch %d, want socket 5's epoch 2", e.Epoch)
	}
}
