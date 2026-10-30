package core

import (
	"bytes"
	"slices"
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

// hsmsFrame builds a data frame of session 1: the length prefix, the header and text.
func hsmsFrame(sb uint32, stream, function byte, text ...byte) []byte {
	f := make([]byte, 0, 14+len(text))
	f = append(f, 0, 0, 0, byte(10+len(text)), 0, 1, stream, function, 0, 0, byte(sb>>24), byte(sb>>16), byte(sb>>8), byte(sb))

	return append(f, text...)
}

// sv is an S1F4 text of two status values: a list of a one-byte unsigned integer and a two-character ASCII item.
var sv = []byte{0x01, 0x02, 0xa5, 0x01, 0x07, 0x41, 0x02, 'o', 'k'}

// thinFixture registers a thin-selected poll at System Bytes 2, after a kept one at 1, bound to socket 7,
// under a capture whose rule keeps every 1000th.
func thinFixture(t *testing.T) (*PendingTable, *Capture, *PendingEntry) {
	t.Helper()
	pt, _ := newTestPending(t, 0)
	pt.SetSocket(7)
	var gen atomic.Uint64
	c := NewCapture(CaptureParams{ID: tracepack.UUID{1}, Lifecycle: &gen, Tolerance: int64(time.Second), ReservedCap: 8,
		Sampling: &Sampling{KeepEveryN: 1000, KeepInterval: int64(time.Hour)}})
	pt.current = func() *Capture { return c }
	pt.RegisterRequest(poll(1))
	e := pt.RegisterRequest(poll(2))
	pt.Bind(1, 2, 1, 3, 7)

	return pt, c, e
}

func entryOf(c *Capture, p *PendingEntry, socket uint64, frame []byte) *Entry {
	return &Entry{Cap: c, Kind: EntryObserved, Socket: socket, Pending: p, Payload: frame}
}

// A thin-selected poll and its reply keep their 14 bytes of length and header and lose their text, marked unobserved.
func TestThinZeroFillsFromByte14(t *testing.T) {
	t.Parallel()
	pt, c, p := thinFixture(t)
	for _, f := range [][]byte{hsmsFrame(2, 0x81, 3, sv...), hsmsFrame(2, 0x01, 4, sv...)} {
		orig := append([]byte(nil), f...)
		e := entryOf(c, p, 7, f)
		if !pt.Thin(e) {
			t.Fatalf("S1F%d of a thin-selected poll was kept", orig[7])
		}
		if !slices.Equal(e.Payload[:14], orig[:14]) || slices.ContainsFunc(e.Payload[14:], func(b byte) bool { return b != 0 }) ||
			len(e.Payload) != len(orig) {
			t.Errorf("S1F%d = % x, want % x then zeros", orig[7], e.Payload, orig[:14])
		}
		if e.Quality&tracepack.QualityUnobserved == 0 {
			t.Errorf("S1F%d is not marked unobserved", orig[7])
		}
	}
	if n := pt.Counters().Thinned; n != 2 {
		t.Errorf("thinned = %d, want 2", n)
	}
}

// Each of the publisher's checks keeps the record in full when it fails (producer.md §4.6).
func TestThinKeepsWhatFailsACheck(t *testing.T) {
	t.Parallel()
	reply := func() []byte { return hsmsFrame(2, 0x01, 4, sv...) }
	cases := []struct {
		name  string
		setup func(pt *PendingTable, c *Capture, p *PendingEntry) *Entry
	}{
		{"no entry", func(_ *PendingTable, c *Capture, _ *PendingEntry) *Entry { return entryOf(c, nil, 7, reply()) }},
		{"kept by the rule", func(pt *PendingTable, c *Capture, _ *PendingEntry) *Entry {
			return entryOf(c, pt.Lookup(1, 1), 7, hsmsFrame(1, 0x01, 4, sv...))
		}},
		{"a reused key", func(pt *PendingTable, c *Capture, p *PendingEntry) *Entry {
			pt.Register(1, 2, "mes")
			return entryOf(c, p, 7, reply())
		}},
		{"a header disagreement", func(pt *PendingTable, c *Capture, _ *PendingEntry) *Entry {
			p := pt.RegisterRequest(poll(3))
			pt.Bind(1, 3, 1, 1, 7)
			return entryOf(c, p, 7, hsmsFrame(3, 0x01, 4, sv...))
		}},
		{"a reply before the binding", func(pt *PendingTable, c *Capture, _ *PendingEntry) *Entry {
			return entryOf(c, pt.RegisterRequest(poll(4)), 7, hsmsFrame(4, 0x01, 4, sv...))
		}},
		{"a reply on another socket", func(_ *PendingTable, c *Capture, p *PendingEntry) *Entry { return entryOf(c, p, 8, reply()) }},
		{"a reconnect before the send", func(pt *PendingTable, c *Capture, _ *PendingEntry) *Entry {
			p := pt.RegisterRequest(poll(5))
			pt.SetSocket(8)
			pt.Bind(1, 5, 1, 3, 8)
			return entryOf(c, p, 8, hsmsFrame(5, 0x01, 4, sv...))
		}},
		{"a rotation to a capture without the rule", func(_ *PendingTable, _ *Capture, p *PendingEntry) *Entry {
			var gen atomic.Uint64
			return entryOf(NewCapture(CaptureParams{ID: tracepack.UUID{2}, Lifecycle: &gen, ReservedCap: 8}), p, 7, reply())
		}},
		{"a message the capture does not declare", func(_ *PendingTable, c *Capture, p *PendingEntry) *Entry {
			return entryOf(c, p, 7, hsmsFrame(2, 0x09, 7, sv...))
		}},
		{"the ambiguity set capped", func(pt *PendingTable, c *Capture, p *PendingEntry) *Entry {
			pt.capped.Store(true)
			return entryOf(c, p, 7, reply())
		}},
		{"an empty S1F4", func(_ *PendingTable, c *Capture, p *PendingEntry) *Entry {
			return entryOf(c, p, 7, hsmsFrame(2, 0x01, 4))
		}},
		{"an S1F4 with a zero-length item", func(_ *PendingTable, c *Capture, p *PendingEntry) *Entry {
			return entryOf(c, p, 7, hsmsFrame(2, 0x01, 4, 0x01, 0x02, 0xa5, 0x01, 0x07, 0x41, 0x00))
		}},
		{"an S1F4 that is an empty list", func(_ *PendingTable, c *Capture, p *PendingEntry) *Entry {
			return entryOf(c, p, 7, hsmsFrame(2, 0x01, 4, 0x01, 0x00))
		}},
		{"an S1F4 that does not parse", func(_ *PendingTable, c *Capture, p *PendingEntry) *Entry {
			return entryOf(c, p, 7, hsmsFrame(2, 0x01, 4, 0xa5, 0x05, 0x07))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pt, c, p := thinFixture(t)
			e := tc.setup(pt, c, p)
			orig := append([]byte(nil), e.Payload...)
			if pt.Thin(e) || !slices.Equal(e.Payload, orig) || e.Quality != 0 {
				t.Errorf("record thinned: % x, quality %v", e.Payload, e.Quality)
			}
		})
	}
}

// A mark made after Thin has seen the binding still vetoes it: the marks are read after the binding (producer.md §4.6).
func TestThinReadsTheMarksAfterTheBinding(t *testing.T) {
	t.Parallel()
	pt, c, p := thinFixture(t)
	pt.thinHooks = func() { pt.Register(1, 2, "mes") } // a reuse of the key, between the two reads
	e := entryOf(c, p, 7, hsmsFrame(2, 0x01, 4, sv...))
	if pt.Thin(e) {
		t.Error("a reply was thinned although its entry was marked ambiguous before the marks were read")
	}
}

// The cap vetoes thinning from the registration that finds the ambiguity set full, not when its last slot fills:
// a full set still remembers every key it holds.
func TestThinStopsWhenARegistrationFindsTheSetFull(t *testing.T) {
	t.Parallel()
	pt, _ := newTestPending(t, 3)
	pt.SetSocket(7)
	var gen atomic.Uint64
	c := NewCapture(CaptureParams{ID: tracepack.UUID{1}, Lifecycle: &gen, ReservedCap: 8, Sampling: &Sampling{KeepEveryN: 1000, KeepInterval: int64(time.Hour)}})
	pt.current = func() *Capture { return c }
	pt.RegisterRequest(poll(1))
	p := pt.RegisterRequest(poll(2))
	pt.Bind(1, 2, 1, 3, 7)
	pt.RegisterRequest(poll(3)) // the last slot
	if !pt.Thin(entryOf(c, p, 7, hsmsFrame(2, 0x01, 4, sv...))) || pt.Counters().Capped {
		t.Fatal("a full set that refused no key stopped thinning")
	}
	pt.Register(1, 4, "mes") // a key the set cannot hold
	if pt.Thin(entryOf(c, p, 7, hsmsFrame(2, 0x01, 4, sv...))) || !pt.Counters().Capped {
		t.Error("a queued reply was thinned after a registration found the set full")
	}
}

func TestAbnormalReplies(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		text     []byte
		abnormal bool
	}{
		{"a list of two", sv, false},
		{"nested lists", []byte{0x01, 0x02, 0x01, 0x01, 0xa5, 0x01, 0x07, 0x41, 0x02, 'o', 'k'}, false},
		{"a two-byte length", []byte{0x01, 0x01, 0x42, 0x00, 0x03, 'a', 'b', 'c'}, false},
		{"a three-byte length", []byte{0x43, 0x00, 0x00, 0x01, 'a'}, false},
		{"empty", nil, true},
		{"an empty list", []byte{0x01, 0x00}, true},
		{"a zero-length item", []byte{0x01, 0x02, 0xa5, 0x01, 0x07, 0x41, 0x00}, true},
		{"a list missing its element", []byte{0x01, 0x01}, true},
		{"a list missing one of two", []byte{0x01, 0x02, 0xa5, 0x01, 0x07}, true},
		{"a nested list cut short", []byte{0x01, 0x01, 0x01, 0x02, 0xa5, 0x01, 0x07}, true},
		{"a primitive cut short", []byte{0xa5, 0x05, 0x07}, true},
		{"a length cut short", []byte{0x42, 0x00}, true},
		{"no length bytes", []byte{0xa4, 0x07}, true},
		{"trailing bytes", []byte{0xa5, 0x01, 0x07, 0x00}, true},
		{"a list claiming more elements than its bytes hold", []byte{0x01, 0x03, 0xa5, 0x01, 0x07}, true},
		{"lists whose claimed elements would overflow a 32-bit count", bytes.Repeat([]byte{0x03, 0xff, 0xff, 0xff}, 129), true},
	}
	for _, tc := range cases {
		if got := abnormal(tc.text); got != tc.abnormal {
			t.Errorf("%s: abnormal(% x) = %v, want %v", tc.name, tc.text, got, tc.abnormal)
		}
	}
}

// A reply observed at or past its entry's expiry is no reply to it and is kept in full, before any sweep has expired the entry.
func TestThinKeepsAReplyPastTheExpiry(t *testing.T) {
	t.Parallel()
	pt, clk := newTestPending(t, 0)
	pt.SetSocket(7)
	var gen atomic.Uint64
	c := NewCapture(CaptureParams{ID: tracepack.UUID{1}, Lifecycle: &gen, ReservedCap: 8, Sampling: &Sampling{KeepEveryN: 1000, KeepInterval: int64(time.Hour)}})
	pt.current = func() *Capture { return c }
	pt.RegisterRequest(poll(1))
	p := pt.RegisterRequest(poll(2))
	pt.Bind(1, 2, 1, 3, 7)
	p.Awaiting(time.Second) // expires at 1 s plus the 5 s grace
	early := entryOf(c, p, 7, hsmsFrame(2, 0x01, 4, sv...))
	early.Mono = clk.now()
	clk.advance(6 * time.Second)
	late := entryOf(c, pt.Lookup(1, 2), 7, hsmsFrame(2, 0x01, 4, sv...))
	late.Mono = clk.now()
	if late.Pending != p {
		t.Fatal("the unswept entry is no longer found by its key")
	}
	if pt.Thin(late) {
		t.Error("a reply observed at the entry's expiry was thinned")
	}
	if !pt.Thin(early) {
		t.Error("a reply observed before the expiry was kept: the control case does not thin")
	}
}
