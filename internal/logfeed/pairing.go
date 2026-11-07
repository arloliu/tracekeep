package logfeed

// ItemKind is what a file's released entries become (logfeed.md §15.5 *What becomes what*).
type ItemKind int

const (
	// Pair is a header and its body: a data record when the body parses as one message, else an unparsed-entry.
	Pair ItemKind = iota + 1
	// LoneHeader is a header no body took within the pairing window: an unparsed-entry holding its fields.
	LoneHeader
	// LoneBody is a body no header was pending for: an unparsed-entry, never a record.
	LoneBody
	// Encrypted is an encrypted body, with the header it took if any: an unparsed-entry holding the ciphertext.
	Encrypted
	// Unclassified is an entry the pipeline could not classify: an unrecognised-line holding its raw lines.
	Unclassified
	// Unrecognised is an entry whose kind's fields are malformed: an unrecognised-line holding the message as received.
	Unrecognised
)

// Item is one or two entries of a file paired; Header or Body is nil where the kind has none.
type Item struct {
	Kind         ItemKind
	Header, Body *Entry
}

// Entries are the entries the item holds, which are acknowledged once its record or annotation is registered.
func (it *Item) Entries() []*Entry {
	var out []*Entry
	for _, e := range []*Entry{it.Header, it.Body} {
		if e != nil {
			out = append(out, e)
		}
	}

	return out
}

// Pairer pairs one file's released entries in offset order (logfeed.md §15.5 *Pairing*):
// a header waits as the pending header of its SxFy for at most window entries of its file,
// a body takes the pending header of its SxFy, and of its TID when both carry one,
// and a second header of an SxFy before its body ends the first.
// Items come out in the file's order, so the entries ahead of a pending header wait with it,
// and the items returned are always the entries given up to some offset.
type Pairer struct {
	window int
	queue  []*slot
}

type slot struct {
	item Item
	// open marks a header still waiting for its body; age counts the entries of the file given after it.
	open bool
	age  int
}

// NewPairer is a pairer whose headers wait window entries.
func NewPairer(window int) *Pairer { return &Pairer{window: window} }

// Add gives the pairer the next released entry, or a malformed one as Unrecognised, and returns the items now complete, in order.
// A header is open for the window entries after it: the last of them may still be its body, and after it the header ends without one.
func (p *Pairer) Add(e *Entry, malformed bool) []Item {
	for _, s := range p.queue {
		if s.open {
			s.age++
		}
	}
	p.place(e, malformed)
	for _, s := range p.queue {
		if s.open && s.age >= p.window {
			s.open = false
		}
	}

	return p.complete()
}

// place puts e in the queue, or pairs it with the pending header it belongs to.
func (p *Pairer) place(e *Entry, malformed bool) {
	switch {
	case malformed:
		p.queue = append(p.queue, &slot{item: Item{Kind: Unrecognised, Body: e}})
	case e.Kind == KindHeader:
		if s := p.pending(e.SxFy, ""); s != nil {
			s.open = false // a second header of the SxFy ends the first
		}
		p.queue = append(p.queue, &slot{item: Item{Kind: LoneHeader, Header: e}, open: true})
	case e.Kind == KindBody || e.Kind == KindEncrypted:
		kind := Pair
		if e.Kind == KindEncrypted {
			kind = Encrypted
		}
		if s := p.pending(e.SxFy, e.TID); s != nil {
			s.item.Kind, s.item.Body, s.open = kind, e, false
		} else {
			if kind == Pair {
				kind = LoneBody
			}
			p.queue = append(p.queue, &slot{item: Item{Kind: kind, Body: e}})
		}
	default:
		p.queue = append(p.queue, &slot{item: Item{Kind: Unclassified, Body: e}})
	}
}

// pending is the open header of sxfy, of tid as well when both carry one.
func (p *Pairer) pending(sxfy, tid string) *slot {
	for _, s := range p.queue {
		if s.open && s.item.Header.SxFy == sxfy && (tid == "" || s.item.Header.TID == "" || s.item.Header.TID == tid) {
			return s
		}
	}

	return nil
}

// complete takes the longest run of closed items at the queue's front whose entries all lie below every entry still queued,
// so the items returned are always the entries given up to an offset: a pair whose body lies above a later item's first entry
// waits for that item, and both come out together once it closes.
func (p *Pairer) complete() []Item {
	n, high := 0, int64(-1)
	for i, s := range p.queue {
		if s.open {
			break
		}
		high = max(high, s.last())
		if i+1 == len(p.queue) || high < p.queue[i+1].first() {
			n = i + 1
		}
	}
	out := make([]Item, n)
	for i := range n {
		out[i] = p.queue[i].item
	}
	p.queue = p.queue[n:]

	return out
}

// first is the lowest offset the slot's item holds, the offset the queue is ordered by.
func (s *slot) first() int64 { return s.item.Entries()[0].At() }

// last is the highest offset the slot's item holds.
func (s *slot) last() int64 {
	var o int64 = -1
	for _, e := range s.item.Entries() {
		o = max(o, e.At())
	}

	return o
}

// Flush ends every open header as a header without a body and returns every item, as a file's stop does.
func (p *Pairer) Flush() []Item {
	for _, s := range p.queue {
		s.open = false
	}

	return p.complete()
}

// Waiting is the number of entries the pairer holds.
func (p *Pairer) Waiting() int { return len(p.queue) }
