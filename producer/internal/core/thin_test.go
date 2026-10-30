package core

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// thinningCapture builds a capture under rule and makes it pt's current one.
func thinningCapture(pt *PendingTable, rule *Sampling) {
	var gen atomic.Uint64
	c := NewCapture(CaptureParams{ID: tracepack.UUID{1}, Lifecycle: &gen, Tolerance: int64(time.Second), ReservedCap: 8, Sampling: rule})
	pt.current = func() *Capture { return c }
}

func poll(sb uint32) Request {
	return Request{SessionID: 1, SystemBytes: sb, Stream: 1, Function: 3, Named: true, Marked: true, Channel: "fdc"}
}

// A marked S1F3 takes its decision at registration from the current capture's rule:
// every Nth keeps its bodies, counted from the capture's first, which keeps them (producer.md §4.6).
func TestRegistrationTakesTheRuleOfEveryNth(t *testing.T) {
	t.Parallel()
	pt, _ := newTestPending(t, 0)
	thinningCapture(pt, &Sampling{KeepEveryN: 3, KeepInterval: int64(time.Hour)})
	got := make([]bool, 0, 7)
	for sb := range uint32(7) {
		got = append(got, pt.RegisterRequest(poll(sb)).ThinSelected())
	}
	want := []bool{false, true, true, false, true, true, false}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("thin-selected = %v, want %v", got, want)
		}
	}
}

// A poll keeps its bodies once the interval since the last kept one has passed, whatever the count.
func TestRegistrationKeepsAfterTheInterval(t *testing.T) {
	t.Parallel()
	pt, clk := newTestPending(t, 0)
	thinningCapture(pt, &Sampling{KeepEveryN: 1000, KeepInterval: int64(time.Minute)})
	if pt.RegisterRequest(poll(1)).ThinSelected() {
		t.Fatal("the capture's first poll is thin-selected")
	}
	clk.advance(59 * time.Second)
	if !pt.RegisterRequest(poll(2)).ThinSelected() {
		t.Fatal("a poll inside the interval kept its bodies")
	}
	clk.advance(time.Second)
	if pt.RegisterRequest(poll(3)).ThinSelected() {
		t.Fatal("a poll at the interval is thin-selected")
	}
	clk.advance(30 * time.Second)
	if !pt.RegisterRequest(poll(4)).ThinSelected() {
		t.Fatal("the interval did not restart at the last kept poll")
	}
}

// Only a marked S1F3 counts or thins; an unmarked one, another message, a registration without a header,
// a capture without a rule and no capture at all keep their bodies, and a new capture's first poll keeps them.
func TestRegistrationKeepsWhatTheRuleDoesNotSelect(t *testing.T) {
	t.Parallel()
	pt, _ := newTestPending(t, 0)
	thinningCapture(pt, &Sampling{KeepEveryN: 2, KeepInterval: int64(time.Hour)})
	if pt.RegisterRequest(poll(1)).ThinSelected() {
		t.Fatal("the first poll is thin-selected")
	}
	unmarked := poll(2)
	unmarked.Marked = false
	other := poll(3)
	other.Stream, other.Function = 2, 41
	for _, e := range []*PendingEntry{pt.RegisterRequest(unmarked), pt.RegisterRequest(other), pt.Register(1, 4, "fdc")} {
		if e.ThinSelected() {
			t.Errorf("registration %d is thin-selected", e.SystemBytes)
		}
	}
	// None of them advanced the count: the next marked poll is the second.
	if !pt.RegisterRequest(poll(5)).ThinSelected() {
		t.Error("a registration the rule does not select advanced its count")
	}

	thinningCapture(pt, &Sampling{KeepEveryN: 2, KeepInterval: int64(time.Hour)})
	if pt.RegisterRequest(poll(6)).ThinSelected() {
		t.Error("a new capture's first poll is thin-selected")
	}
	thinningCapture(pt, nil)
	if pt.RegisterRequest(poll(7)).ThinSelected() {
		t.Error("a poll under a capture without a rule is thin-selected")
	}
	pt.current = func() *Capture { return nil }
	if pt.RegisterRequest(poll(8)).ThinSelected() {
		t.Error("a poll with no capture current is thin-selected")
	}
}

// Every registration enters the ambiguity set, marked or not, so a marked poll reusing an unmarked request's key is ambiguous.
func TestEveryRegistrationEntersTheAmbiguitySet(t *testing.T) {
	t.Parallel()
	pt, _ := newTestPending(t, 0)
	thinningCapture(pt, &Sampling{KeepEveryN: 1000, KeepInterval: int64(time.Hour)})
	first := pt.Register(1, 9, "mes")
	first.Sent()
	first.Replied()
	pt.RegisterRequest(poll(1))
	reused := pt.RegisterRequest(poll(9))
	if !first.Ambiguous() || !reused.Ambiguous() {
		t.Errorf("ambiguous = %v, %v; want both", first.Ambiguous(), reused.Ambiguous())
	}
}

// The outbound frame's stream and function are checked against the request's:
// a disagreement is counted and leaves the entry unambiguous, so its T3 is still attributed;
// a registration without a header is never compared.
func TestBindChecksTheHeader(t *testing.T) {
	t.Parallel()
	pt, _ := newTestPending(t, 0)
	pt.SetSocket(7)
	agree := pt.RegisterRequest(poll(1))
	disagree := pt.RegisterRequest(poll(2))
	bare := pt.Register(1, 3, "fdc")
	pt.Bind(1, 1, 1, 3, 7)
	pt.Bind(1, 2, 1, 1, 7)
	pt.Bind(1, 3, 2, 41, 7)
	if agree.HeaderMismatch() || bare.HeaderMismatch() {
		t.Errorf("header mismatch = %v, %v; want neither", agree.HeaderMismatch(), bare.HeaderMismatch())
	}
	if !disagree.HeaderMismatch() || disagree.Ambiguous() || disagree.Socket() != 7 {
		t.Errorf("disagreeing entry: mismatch %v, ambiguous %v, socket %d; want a mismatch, unambiguous, bound to 7",
			disagree.HeaderMismatch(), disagree.Ambiguous(), disagree.Socket())
	}
	if n := pt.Counters().HeaderMismatches; n != 1 {
		t.Errorf("header mismatches = %d, want 1", n)
	}
}
