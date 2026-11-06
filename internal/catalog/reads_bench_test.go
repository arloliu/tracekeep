package catalog

import (
	"bytes"
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
)

// historyCaptures is how many captures BenchmarkListingHistory's equipment holds.
const historyCaptures = 10

// listingPop is a seeded listing population of equipment T1 over hours hours, as the listings must report it:
// its captures in seed order, each with its completeness, and each capture's packs hour by hour from hour 1, nil for a deleted one.
type listingPop struct {
	captures []CaptureListing
	packs    [][]*PackListing
	hours    int64
}

// seedListing seeds n open captures of equipment T1 on devices devices, the i-th on eqp-(i mod devices), each with every completeness input:
// seqs 0 to 49 evicted in hour 0; 50 to 53 parked and 54 terminated; 55 to 99 a declared loss of its first pack;
// from seq 100 one pack of one ten-seq range in each of hours 1 to hours; and three suspects between its first two packs.
// Every tenth capture, the first included, is closed with a crash gap; with gaps, the same captures also have every third pack deleted, each a gap.
// It returns the population as the listings report it, each completeness classified from the inputs seeded, never read back.
func seedListing(tb testing.TB, c *Catalog, n, devices int, hours int64, gaps bool) *listingPop {
	tb.Helper()
	pop := &listingPop{hours: hours}
	var closed []tracepack.UUID
	crash := []CrashGap{{Start: new(hourNS(hours)), End: new(hourNS(hours + 1))}}
	for i, ci := range seedCaptureInfos(tb, c, devices, n) {
		tenth := i%10 == 0
		s := packsSeed{hour0: 1, hours: hours, packs: 1, ranges: 1, span: 10, seq0: 100}
		if tenth && gaps {
			s.deletedEvery = 3
		}
		ids, next := seedPacks(tb, c, ci.ID, s)
		evicted := Range{SeqFrom: 0, SeqTo: 49, TSFrom: 0, TSTo: 49}
		seedEvicted(tb, c, ci.ID, 0, []Range{evicted})
		seedParked(tb, c, ci.ID, 0, []uint64{50, 51, 52, 53}, string(VersionParked))
		seedParked(tb, c, ci.ID, 0, []uint64{54}, string(VersionTerminated))
		loss := DeclaredLoss{SeqFrom: new(uint64(55)), SeqTo: new(uint64(99))}
		seedLosses(tb, c, ids[0], []Loss{{SeqFrom: loss.SeqFrom, SeqTo: loss.SeqTo}})
		seedConflicts(tb, c, ci.ID, ids[0], ids[1], 100, 3)
		in := completenessInputs{
			highest: next - 1, hasHighest: true, origin: new(ci.OriginNS), evicted: []Range{evicted}, losses: []DeclaredLoss{loss},
			parked:    []parkedSeq{{50, 50, VersionParked, ""}, {51, 51, VersionParked, ""}, {52, 52, VersionParked, ""}, {53, 53, VersionParked, ""}, {54, 54, VersionTerminated, "a1"}},
			conflicts: []ConflictRange{{100, 100, "suspect"}, {101, 101, "suspect"}, {102, 102, "suspect"}},
		}
		packs := make([]*PackListing, len(ids))
		for k, id := range ids {
			if s.deletedEvery > 0 && (k+1)%s.deletedEvery == 0 {
				continue
			}
			from, ts := s.seq0+uint64(k)*s.span, hourNS(1+int64(k))                                       //nolint:gosec // a small pack index
			r := Range{SeqFrom: from, SeqTo: from + s.span - 1, TSFrom: ts, TSTo: ts + int64(s.span) - 1} //nolint:gosec // a small span
			packs[k] = &PackListing{ID: id, Role: RoleSegment, State: "staged", Size: 1 << 20, FormatMinor: 2, Ranges: []Range{r}}
			in.retained = append(in.retained, r)
		}
		ci.LastRecordNS = new(hourNS(hours) + int64(s.span) - 1) //nolint:gosec // a small span
		if tenth {
			closed = append(closed, ci.ID)
			ci.State, in.crashGaps = StateClosed, crash
		}
		pop.captures = append(pop.captures, CaptureListing{CaptureInfo: ci, Completeness: in.classify()})
		pop.packs = append(pop.packs, packs)
	}
	seedCloses(tb, c, closed, hourNS(hours+1), hourNS(hours), hourNS(hours+1))

	return pop
}

// whole is the window of the population's whole history, hour 0 to its last; hour is its last hour alone.
// Both select every capture: each is open or has its last record in the last hour.
func (p *listingPop) whole() Selector {
	return Selector{EquipmentID: "T1", From: hourNS(0), To: hourNS(p.hours + 1)}
}

func (p *listingPop) hour() Selector {
	return Selector{EquipmentID: "T1", From: hourNS(p.hours), To: hourNS(p.hours + 1)}
}

// listed is the population's captures in the listings' order: by device, source, origin and id.
func (p *listingPop) listed() []CaptureListing {
	out := slices.Clone(p.captures)
	slices.SortFunc(out, func(a, b CaptureListing) int {
		return cmp.Or(cmp.Compare(a.Device, b.Device), cmp.Compare(a.Source, b.Source), cmp.Compare(a.OriginNS, b.OriginNS), bytes.Compare(a.ID[:], b.ID[:]))
	})

	return out
}

// wantCaptures is ListCaptures' answer for a window that selects every capture.
func (p *listingPop) wantCaptures() []CaptureInfo {
	listed := p.listed()
	out := make([]CaptureInfo, 0, len(listed))
	for _, cl := range listed {
		out = append(out, cl.CaptureInfo)
	}

	return out
}

// wantDevices is ListDevices' answer for sel, which selects every capture: no descriptor names an endpoint and no recorder instance two captures.
func (p *listingPop) wantDevices(sel Selector) []DeviceInfo {
	var out []DeviceInfo
	for _, cl := range p.listed() {
		if len(out) == 0 || out[len(out)-1].Name != cl.Device || out[len(out)-1].Source != cl.Source {
			out = append(out, DeviceInfo{Name: cl.Device, Source: cl.Source})
		}
		d := &out[len(out)-1]
		if cl.State == StateOpen {
			d.OpenCaptures++
		}
		if d.LastRecordNS == nil || *cl.LastRecordNS > *d.LastRecordNS {
			d.LastRecordNS = cl.LastRecordNS
		}
		d.Gap = d.Gap || slices.ContainsFunc(cl.Completeness.Gaps, func(g Gap) bool { return g.meets(sel.From, sel.To) })
	}

	return out
}

// wantPacks is ListPacks' answer for sel, which selects every capture, at cursor:
// each hour from 1 a scope of every capture, open at revision 1, with its pack unless deleted; hour 0 no scope and every capture evicted.
func (p *listingPop) wantPacks(sel Selector, cursor Cursor) Listing {
	listed := p.listed()
	byID := slices.Clone(listed)
	slices.SortFunc(byID, func(a, b CaptureListing) int { return bytes.Compare(a.ID[:], b.ID[:]) })
	packs := map[tracepack.UUID][]*PackListing{}
	for i, cl := range p.captures {
		packs[cl.ID] = p.packs[i]
	}
	out := Listing{Cursor: cursor, Captures: listed}
	for h := floorHour(sel.From); h <= floorHour(sel.To-1); h++ {
		hl := HourListing{Hour: h}
		for _, cl := range byID {
			if h == 0 {
				hl.Evicted = append(hl.Evicted, cl.ID)
				continue
			}
			sl := ScopeListing{Capture: cl.ID, State: "open", Revision: 1}
			if pk := packs[cl.ID][h-1]; pk != nil {
				sl.Packs = []PackListing{*pk}
			}
			hl.Scopes = append(hl.Scopes, sl)
		}
		hl.fill(listed)
		out.Hours = append(out.Hours, hl)
	}

	return out
}

// sameAnswer reports whether two answers hold the same values, an empty slice equal to a nil one.
func sameAnswer(a, b any) bool { return sameValue(reflect.ValueOf(a), reflect.ValueOf(b)) }

func sameValue(a, b reflect.Value) bool {
	switch a.Kind() { //nolint:exhaustive // every other kind compares by value
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}

		return sameValue(a.Elem(), b.Elem())
	case reflect.Slice:
		if a.Len() != b.Len() {
			return false
		}
		for i := range a.Len() {
			if !sameValue(a.Index(i), b.Index(i)) {
				return false
			}
		}
	case reflect.Struct:
		for i := range a.NumField() {
			if !sameValue(a.Field(i), b.Field(i)) {
				return false
			}
		}
	default:
		return a.Equal(b)
	}

	return true
}

// BenchmarkListings is the API's listings over one piece of equipment's captures, each with every completeness input (seedListing) over 24 hours.
// Its starting states hold 1, 100 or 1,500 captures (TRACEKEEP_BENCH_SCALE) on 1 or 100 devices, at most one a capture,
// without gaps, or with them in every tenth capture; the window is the whole history.
// One iteration lists once and asserts its complete answer equal to the one built from the seed (listingPop):
// ListCaptures the captures, ListDevices the devices, ListPacks the captures with their completeness and every hour,
// and Completeness, every capture's in turn. They write nothing.
func BenchmarkListings(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1, 100, 1500) {
		for _, devices := range []int{1, 100} {
			if devices > n {
				continue
			}
			for _, gaps := range []bool{false, true} {
				b.Run(fmt.Sprintf("captures=%d/devices=%d/gaps=%t", n, devices, gaps), func(b *testing.B) {
					benchEngines(b, func(b *testing.B, c *Catalog) {
						pop := seedListing(b, c, n, devices, 24, gaps)
						benchListingOps(b, c, pop, pop.whole(), true)
					})
				})
			}
		}
	}
}

// BenchmarkListingHistory is the API's listings of a window held constant while the history outside it grows.
// Its starting state holds historyCaptures captures of one piece of equipment on one device, with gaps (seedListing), over 24, 720 or 8,760 hours (TRACEKEEP_BENCH_SCALE).
// One iteration lists the last hour, or the whole history, and asserts its complete answer as BenchmarkListings does.
// Completeness takes no window, a capture's answer being its whole history (catalog.md §7.3), so it runs under window=whole alone.
func BenchmarkListingHistory(b *testing.B) {
	for _, hours := range benchscale.Sizes(b, int64(24), 720, 8760) {
		b.Run(fmt.Sprintf("hours=%d", hours), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				pop := seedListing(b, c, historyCaptures, 1, hours, true)
				b.Run("window=hour", func(b *testing.B) { benchListingOps(b, c, pop, pop.hour(), false) })
				b.Run("window=whole", func(b *testing.B) { benchListingOps(b, c, pop, pop.whole(), true) })
			})
		})
	}
}

// benchListingOps runs each listing of sel over pop as its own sub-benchmark, Completeness only with completeness,
// each asserting its answer the one pop gives.
func benchListingOps(b *testing.B, c *Catalog, pop *listingPop, sel Selector, completeness bool) {
	b.Helper()
	var cursor Cursor
	if err := c.read.QueryRowxContext(b.Context(), `SELECT epoch, value FROM counters WHERE name = 'scope_events'`).Scan(&cursor.Epoch, &cursor.Seq); err != nil {
		b.Fatal(err)
	}
	wantCaptures, wantDevices, wantPacks := pop.wantCaptures(), pop.wantDevices(sel), pop.wantPacks(sel, cursor)
	b.Run("ListCaptures", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := c.ListCaptures(b.Context(), sel, time.Minute); err != nil || !sameAnswer(got, wantCaptures) {
				b.Fatalf("ListCaptures = %d captures, %v; not the %d seeded as seeded", len(got), err, len(wantCaptures))
			}
		}
	})
	b.Run("ListDevices", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := c.ListDevices(b.Context(), sel, time.Minute); err != nil || !sameAnswer(got, wantDevices) {
				b.Fatalf("ListDevices = %+v, %v; want %+v", got, err, wantDevices)
			}
		}
	})
	b.Run("ListPacks", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := c.ListPacks(b.Context(), sel, time.Minute); err != nil || !sameAnswer(got, wantPacks) {
				b.Fatalf("ListPacks = %d captures and %d hours, %v; not the %d and %d seeded as seeded", len(got.Captures), len(got.Hours), err, len(wantPacks.Captures), len(wantPacks.Hours))
			}
		}
	})
	if !completeness {
		return
	}
	b.Run("Completeness", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for i, cl := range pop.captures {
				if cp, err := c.Completeness(b.Context(), cl.ID); err != nil || !sameAnswer(cp, cl.Completeness) {
					b.Fatalf("Completeness of capture %d = %+v, %v; want %+v", i, cp, err, cl.Completeness)
				}
			}
		}
	})
}

// BenchmarkLookups is the point reads of a capture's history: a human read's ReadSet and NextReadHour, a restore's RecordCopies,
// and the reconciler's KeyStanding of a registered, a tombstoned and an unknown key.
// Its starting state is one open capture with 1,000, 20,000 or 200,000 seeded ranges (TRACEKEEP_BENCH_SCALE), 100 packs an hour,
// and a tenth as many tombstones as ranges. One iteration reads near the end of the history, its last hour or seq, and asserts the result.
// They write nothing.
func BenchmarkLookups(b *testing.B) {
	for _, prior := range benchscale.Sizes(b, 1000, 20000, 200000) {
		b.Run(fmt.Sprintf("prior=%d", prior), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				h := seedHistory(b, c, prior)
				tombstoned := seedTombstones(b, c, prior/10, "deleted", 1)
				benchLookups(b, c, h, tombstoned[len(tombstoned)-1])
			})
		})
	}
}

func benchLookups(b *testing.B, c *Catalog, h *history, tombstoned string) {
	b.Helper()
	b.Run("ReadSet", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if rs, err := c.ReadSet(b.Context(), h.capture, h.lastHour); err != nil || len(rs.Packs) != historyHourRanges/10 || len(rs.Packs[0].Ranges) != 10 {
				b.Fatalf("ReadSet = %d packs, %v; want %d of 10 ranges", len(rs.Packs), err, historyHourRanges/10)
			}
		}
	})
	b.Run("NextReadHour", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if hour, ok, err := c.NextReadHour(b.Context(), h.capture, h.lastHour, h.lastHour+maxHours); err != nil || !ok || hour != h.lastHour {
				b.Fatalf("NextReadHour = %d, %t, %v; want %d", hour, ok, err, h.lastHour)
			}
		}
	})
	b.Run("RecordCopies", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if copies, in, err := c.RecordCopies(b.Context(), h.capture, h.next-1, h.lastHour, 10); err != nil || !in || len(copies) != 1 {
				b.Fatalf("RecordCopies = %d, %t, %v; want the last pack's copy", len(copies), in, err)
			}
		}
	})
	for _, tc := range []struct {
		name string
		key  string
		want KeyStanding
	}{
		{"registered", "staging/" + h.packs[len(h.packs)-1].String(), KeyRegistered},
		{"tombstoned", tombstoned, KeyTombstoned},
		{"unknown", "staging/unknown", KeyUnknown},
	} {
		b.Run("KeyStanding/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if got, err := c.KeyStanding(b.Context(), tc.key); err != nil || got != tc.want {
					b.Fatalf("KeyStanding = %v, %v; want %v", got, err, tc.want)
				}
			}
		})
	}
}
