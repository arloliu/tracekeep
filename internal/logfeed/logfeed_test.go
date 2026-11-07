package logfeed

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/secs2"
	"github.com/arloliu/go-secs/v2/sml"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// fixture builds synthetic entries of one file of equipment T1 from go-secs messages, its offsets growing as a log's would.
type fixture struct {
	t      *testing.T
	offset int64
}

func (f *fixture) raw(fields map[string]any) []byte {
	f.t.Helper()
	base := map[string]any{"v": 1, "equipment_id": "T1", "source": "feed-a", "tz": "Asia/Taipei", "file": "host-20261107.log",
		"offset": f.offset, "ts": "2026-11-07 08:00:00.000"}
	f.offset += 100
	for k, v := range fields {
		base[k] = v
	}
	b, err := json.Marshal(base)
	if err != nil {
		f.t.Fatal(err)
	}

	return b
}

func (f *fixture) entry(fields map[string]any) *Entry {
	f.t.Helper()
	e, err := Decode("T1", f.raw(fields))
	if err != nil {
		f.t.Fatal(err)
	}

	return e
}

// message is a go-secs message and its header and body entries as a host program logs them.
func (f *fixture) message(stream, function uint8, w bool, sysID uint32, item secs2.Item, dir string) (*hsms.DataMessage, *Entry, *Entry) {
	f.t.Helper()
	m, err := hsms.NewDataMessage(stream, function, w, 1, hsms.ToSystemBytes(sysID), item)
	if err != nil {
		f.t.Fatal(err)
	}
	h := m.HeaderBytes()
	sf := fmt.Sprintf("S%dF%d", stream, function)
	tid := fmt.Sprint(sysID)
	header := f.entry(map[string]any{"kind": KindHeader, "dir": dir, "header": hex.EncodeToString(h[:]), "sxfy": sf, "tid": tid})
	body := f.entry(map[string]any{"kind": KindBody, "sxfy": sf, "tid": tid, "text": sml.MustEncodeMessage(m)})

	return m, header, body
}

func TestDecode(t *testing.T) {
	t.Parallel()
	f := &fixture{t: t}
	e := f.entry(map[string]any{"kind": KindUnclassified, "text": "a line"})
	if e.At() != 0 || e.UTCNs != time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC).UnixNano() || e.Uncertain {
		t.Errorf("entry %+v", e)
	}
	for name, fields := range map[string]map[string]any{
		"a newer schema":            {"v": 2, "kind": KindUnclassified, "text": "x"},
		"another equipment":         {"equipment_id": "T2", "kind": KindUnclassified, "text": "x"},
		"no source":                 {"source": "", "kind": KindUnclassified, "text": "x"},
		"a file past 128":           {"file": string(make([]byte, 129)), "kind": KindUnclassified, "text": "x"},
		"no offset":                 {"offset": nil, "kind": KindUnclassified, "text": "x"},
		"a negative offset":         {"offset": -1, "kind": KindUnclassified, "text": "x"},
		"an unknown zone":           {"tz": "Mars/Base", "kind": KindUnclassified, "text": "x"},
		"no zone":                   {"tz": "", "kind": KindUnclassified, "text": "x"},
		"an offset of a string":     {"offset": "7", "kind": KindUnclassified, "text": "x"},
		"a timestamp of other form": {"ts": "07/11/2026 08:00", "kind": KindUnclassified, "text": "x"},
	} {
		if _, err := Decode("T1", f.raw(fields)); !errors.Is(err, ErrUnreadable) {
			t.Errorf("%s: Decode = %v, want ErrUnreadable", name, err)
		}
	}
	if _, err := Decode("T1", []byte("{not json")); !errors.Is(err, ErrUnreadable) {
		t.Errorf("JSON that does not parse: %v, want ErrUnreadable", err)
	}
	for name, fields := range map[string]map[string]any{
		"an unknown kind":          {"kind": "trace"},
		"a header without its dir": {"kind": KindHeader, "sxfy": "S1F1"},
		"a header of a bad sxfy":   {"kind": KindHeader, "dir": DirSent, "sxfy": "S1"},
		"a header not of hex":      {"kind": KindHeader, "dir": DirSent, "sxfy": "S1F1", "header": "zz"},
		"a body without its text":  {"kind": KindBody, "sxfy": "S1F1"},
		"a text of a number":       {"kind": KindBody, "sxfy": "S1F1", "text": 17},
		"a dir of a boolean":       {"kind": KindHeader, "sxfy": "S1F1", "dir": false},
	} {
		e, err := Decode("T1", f.raw(fields))
		if !errors.Is(err, ErrMalformed) || e == nil {
			t.Errorf("%s: Decode = %v, %v; want the entry with ErrMalformed", name, e, err)
		}
	}
}

// A wall time occurring twice or never takes the earlier reading and is uncertain (logfeed.md §15.5 *Time*).
func TestWallTime(t *testing.T) {
	t.Parallel()
	utc := func(s string) int64 {
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}

		return v.UnixNano()
	}
	for _, c := range []struct {
		ts, tz, want string
		uncertain    bool
	}{
		{"2026-11-07 08:00:00", "Asia/Taipei", "2026-11-07T00:00:00Z", false},
		{"2026-07-01 12:00:00.5", "Europe/Berlin", "2026-07-01T10:00:00.5Z", false},
		// Berlin's 2026 fall-back: 02:30 occurs twice, at 00:30 and 01:30 UTC.
		{"2026-10-25 02:30:00", "Europe/Berlin", "2026-10-25T00:30:00Z", true},
		// Berlin's 2026 spring-forward: 02:30 never occurs; its readings are 00:30 and 01:30 UTC.
		{"2026-03-29 02:30:00", "Europe/Berlin", "2026-03-29T00:30:00Z", true},
	} {
		got, uncertain, err := WallTime(c.ts, c.tz)
		if err != nil || got != utc(c.want) || uncertain != c.uncertain {
			t.Errorf("WallTime(%s, %s) = %s, %v, %v; want %s, %v", c.ts, c.tz, time.Unix(0, got).UTC(), uncertain, err, c.want, c.uncertain)
		}
	}
}

func TestWindow(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(0, 0)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	w := NewWindow[string](time.Second, catalog.NoWatermark)
	for _, a := range []struct {
		offset int64
		ms     int
		item   string
		want   Disposition
	}{
		{300, 0, "c", Held}, {100, 100, "a", Held}, {100, 200, "a2", Duplicate}, {200, 500, "b", Held},
	} {
		if d, held := w.Add(a.offset, at(a.ms), a.item); d != a.want || (d == Duplicate && held != "a") {
			t.Fatalf("Add(%d) = %v, %q; want %v", a.offset, d, held, a.want)
		}
	}
	// At 1.2 s a has waited and b has not: b holds c, which waited, behind it.
	if got := w.Release(at(1200)); fmt.Sprint(got) != "[a]" {
		t.Errorf("Release at 1.2 s = %v, want [a]", got)
	}
	if got := w.Release(at(1600)); fmt.Sprint(got) != "[b c]" {
		t.Errorf("Release at 1.6 s = %v, want [b c]", got)
	}
	if d, _ := w.Add(250, at(1700), "late"); d != Stale {
		t.Errorf("an entry below a released one = %v, want Stale", d)
	}
	if d, held := w.Add(100, at(1700), "again"); d != Duplicate || held != "a" {
		t.Errorf("a redelivery of a released, unsettled entry = %v, %q; want a duplicate of a", d, held)
	}
	w.Settle(200)
	if d, _ := w.Add(100, at(1800), "again"); d != Stale {
		t.Errorf("a redelivery of a settled entry = %v, want Stale", d)
	}
	if d, _ := NewWindow[string](time.Second, 500).Add(500, t0, "x"); d != Stale {
		t.Errorf("an entry at the watermark = %v, want Stale", d)
	}
	w.Add(400, at(1900), "d")
	if got := w.Drain(); fmt.Sprint(got) != "[d]" || w.Held() != 0 {
		t.Errorf("Drain = %v, held %d", got, w.Held())
	}
	w.Stop()
	if d, _ := w.Add(900, t0, "after"); d != Stale {
		t.Errorf("an entry of a stopped file = %v, want Stale", d)
	}
	if d, _ := w.Add(300, t0, "c again"); d != Duplicate {
		t.Errorf("a redelivery of an unsettled entry of a stopped file = %v, want Duplicate", d)
	}
}

func TestDevices(t *testing.T) {
	t.Parallel()
	two := uint16(2)
	d := &Devices{Main: "eqp", Rules: []DeviceRule{{Token: "1", Device: "main"}, {Session: &two, Device: "lp1"}},
		Overrides: map[string][]DeviceRule{"T9": {{Token: "1", Device: "lp9"}}}}
	for _, c := range []struct {
		equipment, token string
		session          *uint16
		want             string
	}{
		{"T1", "1", &two, "main"}, {"T1", "", &two, "lp1"}, {"T1", "7", nil, "eqp"}, {"T9", "1", nil, "lp9"}, {"T9", "", &two, "eqp"},
	} {
		if got := d.Of(c.equipment, c.token, c.session); got != c.want {
			t.Errorf("Of(%s, %q, %v) = %s, want %s", c.equipment, c.token, c.session, got, c.want)
		}
	}
}

func kinds(items []Item) string {
	s := ""
	for _, it := range items {
		s += map[ItemKind]string{Pair: "P", LoneHeader: "H", LoneBody: "B", Encrypted: "E", Unclassified: "U", Unrecognised: "R"}[it.Kind]
	}

	return s
}

// Pairing keeps the file's order, takes a body's pending header by SxFy and TID, ends a header past the window or by a second of its SxFy.
func TestPairer(t *testing.T) {
	t.Parallel()
	f := &fixture{t: t}
	item := secs2.NewListItem(secs2.NewASCIIItem("LOT1"))
	_, h1, b1 := f.message(1, 3, true, 1, item, DirSent)
	_, h2, b2 := f.message(1, 4, false, 1, item, DirReceived)
	line := f.entry(map[string]any{"kind": KindUnclassified, "text": "x"})
	p := NewPairer(3)
	var got []Item
	for _, e := range []*Entry{h1, h2, line, b1, b2} {
		got = append(got, p.Add(e, false)...)
	}
	if kinds(got) != "PPU" || got[0].Header != h1 || got[0].Body != b1 || got[1].Header != h2 || got[1].Body != b2 {
		t.Errorf("items %s, want P P U in the file's order", kinds(got))
	}

	_, h3, b3 := f.message(6, 11, true, 3, item, DirReceived)
	_, h4, _ := f.message(6, 11, true, 4, item, DirReceived)
	got = append(p.Add(h3, false), p.Add(h4, false)...)
	if kinds(got) != "H" || got[0].Header != h3 {
		t.Errorf("a second header of the SxFy = %s, want the first ended", kinds(got))
	}
	b4 := f.entry(map[string]any{"kind": KindBody, "sxfy": "S6F11", "tid": "4", "text": *b3.Text})
	if got = p.Add(b4, false); kinds(got) != "P" || got[0].Header != h4 {
		t.Errorf("the second header's body = %s", kinds(got))
	}

	_, h5, _ := f.message(1, 3, true, 5, item, DirSent)
	got = p.Add(h5, false)
	for range 2 {
		got = append(got, p.Add(f.entry(map[string]any{"kind": KindUnclassified, "text": "x"}), false)...)
	}
	if kinds(got) != "" {
		t.Errorf("within the window = %s, want nothing", kinds(got))
	}
	got = p.Add(f.entry(map[string]any{"kind": KindUnclassified, "text": "x"}), false)
	if kinds(got) != "HUUU" {
		t.Errorf("at the window's last entry = %s, want the header ended and the entries behind it", kinds(got))
	}
	enc := f.entry(map[string]any{"kind": KindEncrypted, "sxfy": "S7F25", "text": "CIPHER"})
	bad := f.entry(map[string]any{"kind": KindUnclassified, "text": "x"})
	if got = append(p.Add(enc, false), p.Add(bad, true)...); kinds(got) != "ER" {
		t.Errorf("an encrypted body and a malformed entry = %s", kinds(got))
	}
	_, h6, _ := f.message(1, 3, true, 6, item, DirSent)
	p.Add(h6, false)
	if got = p.Flush(); kinds(got) != "H" || p.Waiting() != 0 {
		t.Errorf("Flush = %s", kinds(got))
	}
}

// A pair whose body lies above an open header waits for it, so the items returned are always the entries given up to an offset.
func TestPairerPrefix(t *testing.T) {
	t.Parallel()
	f := &fixture{t: t}
	item := secs2.NewListItem()
	_, ha, ba := f.message(1, 3, true, 1, item, DirSent)
	_, hb, bb := f.message(2, 41, true, 2, item, DirSent)
	// The file's order: A's header, B's header, A's body, B's body.
	for i, e := range []*Entry{ha, hb, ba, bb} {
		*e.Offset = int64(100 * i)
	}
	p := NewPairer(5)
	for _, e := range []*Entry{ha, hb, ba} {
		if got := p.Add(e, false); len(got) != 0 {
			t.Fatalf("after %s@%d: %s, want nothing while B's header is open below A's body", e.Kind, e.At(), kinds(got))
		}
	}
	if got := p.Add(bb, false); kinds(got) != "PP" || got[0].Header != ha || got[1].Header != hb {
		t.Errorf("once B pairs = %s, want both pairs in order", kinds(got))
	}
}

// A closed pair whose body lies above a later item's header waits for that item too, even when another header is open above both.
func TestPairerPrefixBehindAClosedPair(t *testing.T) {
	t.Parallel()
	f := &fixture{t: t}
	item := secs2.NewListItem()
	_, ha, ba := f.message(1, 3, true, 1, item, DirSent)
	_, hb, bb := f.message(2, 41, true, 2, item, DirSent)
	_, hc, bc := f.message(5, 1, true, 3, item, DirSent)
	// The file's order: A's header, B's header, A's body, C's header, B's body, C's body.
	for i, e := range []*Entry{ha, hb, ba, hc, bb, bc} {
		*e.Offset = int64(100 * i)
	}
	p := NewPairer(3)
	settled := int64(-1)
	for _, e := range []*Entry{ha, hb, ba, hc, bb, bc} {
		high := settled
		for _, it := range p.Add(e, false) {
			for _, x := range it.Entries() {
				if x.At() <= settled {
					t.Fatalf("an entry at or below the settled offset %d: %d", settled, x.At())
				}
				high = max(high, x.At())
			}
		}
		// What one call returns is settled together; nothing below it may stay queued.
		settled = high
		if p.Waiting() > 0 && settled >= minQueued(p) {
			t.Fatalf("after %d: settled %d at or above a queued entry %d", e.At(), settled, minQueued(p))
		}
	}
	if got := p.Flush(); len(got) != 0 {
		t.Errorf("left after the file = %s", kinds(got))
	}
}

// minQueued is the lowest offset the pairer holds.
func minQueued(p *Pairer) int64 {
	low := int64(1 << 62)
	for _, s := range p.queue {
		for _, e := range s.item.Entries() {
			low = min(low, e.At())
		}
	}

	return low
}

// A pair becomes a reconstructed data record, its header trusted only when it names the body's stream and function;
// everything else an annotation holding what the spec names (logfeed.md §15.5).
func TestBuild(t *testing.T) {
	t.Parallel()
	f := &fixture{t: t}
	m, h, b := f.message(6, 11, true, 0x01020304, secs2.NewListItem(secs2.NewUintItem(4, 7), secs2.NewASCIIItem("LOT'S")), DirReceived)
	built, err := Build(&Item{Kind: Pair, Header: h, Body: b})
	if err != nil {
		t.Fatal(err)
	}
	r := built.Record
	if r.Kind != tracepack.KindData || r.Dir != tracepack.DirEquipmentToHost || r.Fidelity != tracepack.FidelityReconstructed ||
		r.DecodeStatus != tracepack.DecodeStatusReconstructedOK || r.TSUTCNs != h.UTCNs || !built.Primary || !built.Trusted ||
		r.FieldValidity&tracepack.FieldValiditySystemBytes == 0 || string(r.Payload) != string(m.ToBytes()) {
		t.Errorf("data record %+v", built)
	}

	// A reply logged under its primary's header: the frame is built, its session and system bytes not trusted.
	_, _, reply := f.message(6, 12, false, 9, secs2.NewBinaryItem(0), DirSent)
	built, err = Build(&Item{Kind: Pair, Header: h, Body: reply})
	if err != nil || built.Trusted || built.Record.FieldValidity != tracepack.FieldValidityFunction {
		t.Errorf("a reply under its primary's header = %+v, %v", built, err)
	}

	// A logged header without W over a body whose SML has W: the frame takes the header's W, which the log established.
	noW := f.entry(map[string]any{"kind": KindHeader, "dir": DirReceived, "sxfy": "S6F11", "header": "0001060b000001020304"})
	built, err = Build(&Item{Kind: Pair, Header: noW, Body: b})
	if err != nil || !built.Trusted || len(built.Record.Payload) < 7 || built.Record.Payload[6]&0x80 != 0 {
		t.Errorf("a trusted header without W = %+v, %v; want its W in the frame", built, err)
	}
}

// Everything but a pair that parses becomes an annotation holding what the spec names, parse-failed for an entry that did not parse.
func TestBuildAnnotations(t *testing.T) {
	t.Parallel()
	f := &fixture{t: t}
	_, h, b := f.message(6, 11, true, 0x01020304, secs2.NewListItem(secs2.NewUintItem(4, 7)), DirReceived)
	bad := f.entry(map[string]any{"kind": KindBody, "sxfy": "S6F11", "text": "S6F11 W <L [1] <U4 1>"})
	wReply := f.entry(map[string]any{"kind": KindBody, "sxfy": "S6F12", "text": "S6F12 W\n<B 0x00>\n."})
	failed, na := tracepack.DecodeStatusParseFailed, tracepack.DecodeStatusNotApplicable
	for name, c := range map[string]struct {
		it     Item
		kind   tracepack.AnnotationKind
		status tracepack.DecodeStatus
		text   string
	}{
		"a body that does not parse": {Item{Kind: Pair, Header: h, Body: bad}, tracepack.AnnotationKindUnparsedEntry, failed, *bad.Text},
		"a W on a reply":             {Item{Kind: Pair, Header: h, Body: wReply}, tracepack.AnnotationKindUnparsedEntry, failed, *wReply.Text},
		"a lone header":              {Item{Kind: LoneHeader, Header: h}, tracepack.AnnotationKindUnparsedEntry, failed, string(h.Raw)},
		"a lone body":                {Item{Kind: LoneBody, Body: b}, tracepack.AnnotationKindUnparsedEntry, na, *b.Text},
		"an unclassified entry":      {Item{Kind: Unclassified, Body: f.entry(map[string]any{"kind": KindUnclassified, "text": "raw"})}, tracepack.AnnotationKindUnrecognisedLine, na, "raw"},
	} {
		built, err := Build(&c.it)
		if err != nil || built.Record.Kind != tracepack.KindAnnotation || built.Record.DecodeStatus != c.status {
			t.Errorf("%s: %+v, %v; want an annotation with %v", name, built, err, c.status)
			continue
		}
		a, err := tracepack.UnmarshalAnnotation(built.Record.Payload)
		if err != nil || a.AnnotationKind != c.kind || a.Text == nil || *a.Text != c.text {
			t.Errorf("%s: annotation %+v, %v; want %v holding %q", name, a, err, c.kind, c.text)
		}
	}
}

// A reply takes its primary's device, remembered by its trusted system bytes or its TID; any other item the rules'.
func TestReplyDevices(t *testing.T) {
	t.Parallel()
	f := &fixture{t: t}
	d := &Devices{Main: "eqp", Rules: []DeviceRule{{Token: "2", Device: "lp1"}}}
	var r Replies
	_, h, b := f.message(1, 3, true, 7, secs2.NewListItem(), DirSent)
	h.Device = "2"
	primary := Item{Kind: Pair, Header: h, Body: b}
	pb, _ := Build(&primary)
	dev := r.Device(d, &primary, &pb)
	r.Remember(&primary, &pb, dev)
	_, rh, rb := f.message(1, 4, false, 7, secs2.NewListItem(), DirReceived)
	reply := Item{Kind: Pair, Header: rh, Body: rb}
	rbuilt, _ := Build(&reply)
	if dev != "lp1" || r.Device(d, &reply, &rbuilt) != "lp1" {
		t.Errorf("primary on %s, reply on %s; want both lp1", dev, r.Device(d, &reply, &rbuilt))
	}
}

// Replies are correlated by session, system bytes and direction, or by a TID from either entry; a W-clear primary keeps its rules' device.
func TestReplyCorrelation(t *testing.T) {
	t.Parallel()
	f := &fixture{t: t}
	d := &Devices{Main: "eqp", Rules: []DeviceRule{{Token: "1", Device: "s1"}, {Token: "2", Device: "s2"}}}
	var r Replies
	remember := func(stream, function uint8, w bool, session uint16, sb uint32, token, dir string) string {
		m, err := hsms.NewDataMessage(stream, function, w, session, hsms.ToSystemBytes(sb), secs2.NewListItem())
		if err != nil {
			t.Fatal(err)
		}
		hb := m.HeaderBytes()
		sf := fmt.Sprintf("S%dF%d", stream, function)
		h := f.entry(map[string]any{"kind": KindHeader, "dir": dir, "header": hex.EncodeToString(hb[:]), "sxfy": sf, "device": token})
		b := f.entry(map[string]any{"kind": KindBody, "sxfy": sf, "text": sml.MustEncodeMessage(m)})
		it := Item{Kind: Pair, Header: h, Body: b}
		built, err := Build(&it)
		if err != nil {
			t.Fatal(err)
		}
		dev := r.Device(d, &it, &built)
		r.Remember(&it, &built, dev)

		return dev
	}
	// Two primaries with the same system bytes on two sessions: each reply takes its own session's device.
	remember(1, 3, true, 1, 7, "1", DirSent)
	remember(1, 3, true, 2, 7, "2", DirSent)
	if dev := remember(1, 4, false, 1, 7, "", DirReceived); dev != "s1" {
		t.Errorf("session 1's reply on %s, want s1", dev)
	}
	if dev := remember(1, 4, false, 2, 7, "", DirReceived); dev != "s2" {
		t.Errorf("session 2's reply on %s, want s2", dev)
	}
	// A W-clear primary of an odd function follows the rules, whatever a remembered transaction of its bytes says.
	if dev := remember(6, 11, false, 1, 7, "", DirReceived); dev != "eqp" {
		t.Errorf("a W-clear primary on %s, want the rules' eqp", dev)
	}

	// A body-only TID, with no trusted header, routes the reply.
	ph := f.entry(map[string]any{"kind": KindHeader, "dir": DirSent, "sxfy": "S2F41", "device": "2"})
	pm, _ := hsms.NewDataMessage(2, 41, true, 1, hsms.ToSystemBytes(9), secs2.NewListItem())
	pb := f.entry(map[string]any{"kind": KindBody, "sxfy": "S2F41", "tid": "T-9", "text": sml.MustEncodeMessage(pm)})
	primary := Item{Kind: Pair, Header: ph, Body: pb}
	pbuilt, _ := Build(&primary)
	r.Remember(&primary, &pbuilt, r.Device(d, &primary, &pbuilt))
	rh := f.entry(map[string]any{"kind": KindHeader, "dir": DirReceived, "sxfy": "S2F42"})
	rm, _ := hsms.NewDataMessage(2, 42, false, 1, hsms.ToSystemBytes(9), secs2.NewListItem())
	rb := f.entry(map[string]any{"kind": KindBody, "sxfy": "S2F42", "tid": "T-9", "text": sml.MustEncodeMessage(rm)})
	reply := Item{Kind: Pair, Header: rh, Body: rb}
	rbuilt, _ := Build(&reply)
	if dev := r.Device(d, &reply, &rbuilt); dev != "s2" {
		t.Errorf("a reply by a body-only TID on %s, want its primary's s2", dev)
	}
}

// Decode takes any bytes without panicking, and returns an entry only with a nil error or ErrMalformed.
func FuzzDecode(f *testing.F) {
	for _, seed := range []string{
		`{"v":1,"equipment_id":"T1","source":"s","tz":"UTC","file":"f","offset":1,"ts":"2026-11-07 08:00:00","kind":"body","sxfy":"S1F1","text":"S1F1 W."}`,
		`{"v":1,"equipment_id":"T1","source":"s","tz":"Europe/Berlin","file":"f","offset":0,"ts":"2026-03-29 02:30:00","kind":"header","dir":"sent","sxfy":"S1F1"}`,
		`{"v":1,"offset":-1}`, `{"text":17}`, `[]`, `null`, ``,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data string) {
		e, err := Decode("T1", []byte(data))
		if e != nil && err != nil && !errors.Is(err, ErrMalformed) {
			t.Errorf("an entry with %v", err)
		}
		if e == nil && err == nil {
			t.Error("neither an entry nor an error")
		}
	})
}
