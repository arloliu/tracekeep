package catalog

import (
	"fmt"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
)

// historyCaptures is how many captures BenchmarkListingHistory's tool holds.
const historyCaptures = 10

// listingPop is a seeded listing population of tool T1: its captures in order, which of them hold gaps, its devices and hours.
type listingPop struct {
	captures []tracepack.UUID
	gapped   []bool
	devices  int
	hours    int64
}

// seedListing seeds n open captures of tool T1 on devices devices, the i-th on eqp-(i mod devices), each with every completeness input:
// seqs 0 to 49 evicted in hour 0; 50 to 53 parked and 54 terminated; 55 to 99 a declared loss of its first pack;
// from seq 100 one pack of one ten-seq range in each of hours 1 to hours; and three suspects between its first two packs.
// Every tenth capture, the first included, is closed with a crash gap; with gaps, the same captures also have every third pack deleted, each a gap.
func seedListing(tb testing.TB, c *Catalog, n, devices int, hours int64, gaps bool) *listingPop {
	tb.Helper()
	pop := &listingPop{captures: seedCaptures(tb, c, devices, n), gapped: make([]bool, n), devices: min(devices, n), hours: hours}
	var closed []tracepack.UUID
	for i, capture := range pop.captures {
		tenth := i%10 == 0
		pop.gapped[i] = tenth && gaps
		s := packsSeed{hour0: 1, hours: hours, packs: 1, ranges: 1, span: 10, seq0: 100}
		if pop.gapped[i] {
			s.deletedEvery = 3
		}
		packs, _ := seedPacks(tb, c, capture, s)
		seedEvicted(tb, c, capture, 0, []Range{{SeqFrom: 0, SeqTo: 49, TSFrom: 0, TSTo: 49}})
		seedParked(tb, c, capture, 0, []uint64{50, 51, 52, 53}, string(VersionParked))
		seedParked(tb, c, capture, 0, []uint64{54}, string(VersionTerminated))
		seedLosses(tb, c, packs[0], []Loss{{SeqFrom: new(uint64(55)), SeqTo: new(uint64(99))}})
		seedConflicts(tb, c, capture, packs[0], packs[1], 100, 3)
		if tenth {
			closed = append(closed, capture)
		}
	}
	seedCloses(tb, c, closed, hourNS(hours+1), hourNS(hours), hourNS(hours+1))

	return pop
}

// whole is the window of the population's whole history, hour 0 to its last; hour is its last hour alone.
func (p *listingPop) whole() (Selector, int) {
	return Selector{ToolID: "T1", From: hourNS(0), To: hourNS(p.hours + 1)}, int(p.hours) + 1
}

func (p *listingPop) hour() (Selector, int) {
	return Selector{ToolID: "T1", From: hourNS(p.hours), To: hourNS(p.hours + 1)}, 1
}

// BenchmarkListings is the API's listings over one tool's captures, each with every completeness input (seedListing) over 24 hours.
// Its starting states hold 1, 100 or 1,500 captures (TRACEKEEP_BENCH_SCALE) on 1 or 100 devices, at most one a capture,
// without gaps, or with them in every tenth capture; the window is the whole history.
// One iteration lists once and asserts what it lists: ListCaptures the captures, ListDevices the devices,
// ListPacks the captures and hours, and Completeness, every capture's in turn, its inputs and gaps. They write nothing.
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
						sel, hours := pop.whole()
						benchListingOps(b, c, pop, sel, hours)
					})
				})
			}
		}
	}
}

// BenchmarkListingHistory is the API's listings of a window held constant while the history outside it grows.
// Its starting state holds historyCaptures captures of one tool on one device, with gaps (seedListing), over 24, 720 or 8,760 hours (TRACEKEEP_BENCH_SCALE).
// One iteration lists the last hour, or the whole history, and asserts what it lists as BenchmarkListings does.
func BenchmarkListingHistory(b *testing.B) {
	for _, hours := range benchscale.Sizes(b, int64(24), 720, 8760) {
		b.Run(fmt.Sprintf("hours=%d", hours), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				pop := seedListing(b, c, historyCaptures, 1, hours, true)
				b.Run("window=hour", func(b *testing.B) {
					sel, n := pop.hour()
					benchListingOps(b, c, pop, sel, n)
				})
				b.Run("window=whole", func(b *testing.B) {
					sel, n := pop.whole()
					benchListingOps(b, c, pop, sel, n)
				})
			})
		})
	}
}

// benchListingOps runs each listing of sel over pop as its own sub-benchmark, sel's window holding hours hours.
func benchListingOps(b *testing.B, c *Catalog, pop *listingPop, sel Selector, hours int) {
	b.Helper()
	n := len(pop.captures)
	b.Run("ListCaptures", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := c.ListCaptures(b.Context(), sel, time.Minute); err != nil || len(got) != n {
				b.Fatalf("ListCaptures = %d, %v; want %d", len(got), err, n)
			}
		}
	})
	b.Run("ListDevices", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := c.ListDevices(b.Context(), sel, time.Minute); err != nil || len(got) != pop.devices {
				b.Fatalf("ListDevices = %d, %v; want %d", len(got), err, pop.devices)
			}
		}
	})
	b.Run("ListPacks", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := c.ListPacks(b.Context(), sel, time.Minute); err != nil || len(got.Captures) != n || len(got.Hours) != hours {
				b.Fatalf("ListPacks = %d captures and %d hours, %v; want %d and %d", len(got.Captures), len(got.Hours), err, n, hours)
			}
		}
	})
	b.Run("Completeness", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for i, capture := range pop.captures {
				cp, err := c.Completeness(b.Context(), capture)
				if err != nil || len(cp.Evicted) != 1 || len(cp.Deferred) != 4 || len(cp.Conflicts) != 3 || (len(cp.Gaps) > 0) != pop.gapped[i] {
					b.Fatalf("Completeness of capture %d = %d evicted, %d deferred, %d conflicts, %d gaps, %v; want 1, 4, 3 and gaps %t",
						i, len(cp.Evicted), len(cp.Deferred), len(cp.Conflicts), len(cp.Gaps), err, pop.gapped[i])
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
