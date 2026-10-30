package core

import "github.com/arloliu/go-secs/tracepack"

// Offsets in a frame, after its 4-byte length prefix, of the HSMS header's stream and function, and where the message text starts.
const (
	frameStreamOff   = 6
	frameFunctionOff = 7
	frameTextOff     = 14
	// listFormat is the SECS-II format code of a list (SEMI E5).
	listFormat = 0
)

// Thin zero-fills e's message text and marks it unobserved when the publisher's checks of producer.md §4.6 all hold,
// in their order, and reports whether it did; e is the lane's own copy, dequeued by the publisher.
func (t *PendingTable) Thin(e *Entry) bool {
	p := e.Pending
	if p == nil || len(e.Payload) < frameTextOff || !p.thin || e.Mono >= p.deadline.Load() {
		return false
	}
	// The binding is read before the marks: Bind sets them before it publishes the binding,
	// so a binding seen here comes with every mark made before it.
	if bound := p.bound.Load(); e.Socket == 0 || bound != e.Socket {
		return false
	}
	t.thinHook()
	if p.ambiguous.Load() || p.mismatch.Load() {
		return false
	}
	stream, function := e.Payload[frameStreamOff]&0x7f, e.Payload[frameFunctionOff]
	if !e.Cap.Declares(stream, function) || t.capped.Load() {
		return false
	}
	text := e.Payload[frameTextOff:]
	if function == 4 && abnormal(text) {
		return false
	}
	clear(text)
	e.Quality |= tracepack.QualityUnobserved
	t.thinned.Add(1)

	return true
}

// abnormal reports an S1F4 text that is empty, holds a zero-length item, or is not exactly one complete SECS-II item (V-27):
// such a reply is kept in full whatever the decision.
// A list's elements follow its header inline, so one pass over the item headers, counting the items still owed, sees every item.
func abnormal(text []byte) bool {
	for owed := 1; owed > 0; owed-- {
		if len(text) == 0 {
			return true
		}
		hdr := text[0]
		lenBytes := int(hdr & 3)
		if lenBytes == 0 || len(text) < 1+lenBytes {
			return true
		}
		n := 0
		for _, b := range text[1 : 1+lenBytes] {
			n = n<<8 | int(b)
		}
		if n == 0 {
			return true
		}
		text = text[1+lenBytes:]
		if hdr>>2 == listFormat {
			// Its n elements follow, each at least two bytes, as does every other item still owed;
			// the bound also keeps owed within the text's length, so it cannot overflow.
			if n > len(text)/2-(owed-1) {
				return true
			}
			owed += n
			continue
		}
		if len(text) < n {
			return true
		}
		text = text[n:]
	}

	return len(text) != 0
}
