package catalog

import (
	"encoding/binary"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
)

// A seeded history holds historyHourRanges ranges an hour: 100 packs of 10 ranges of 10 seqs each, from hour 1 and seq 0.
const (
	historyHourRanges = 1000
	historySpan       = 10
)

// claimEvery makes every claimEvery-th record of a registered pack a claim-check record, with a receipt.
const claimEvery = 10

// history is a capture's seeded prior ranges and the seq above them.
type history struct {
	capture tracepack.UUID
	prior   int
	// packs are the seeded packs, in seq order.
	packs []tracepack.UUID
	// next is the first seq above the seeded ranges, and lastHour their last hour.
	next     uint64
	lastHour int64
}

// regShape is what each iteration of BenchmarkRegisterPack registers: the ranges its pack adds and the suspects it records,
// and pack, which builds iteration i's pack and evidence.
type regShape struct {
	ranges, conflicts int
	pack              func(tb testing.TB, h *history, i int) (*Pack, *Evidence)
}

// benchLease takes the compactor lease for a day, so a long benchmark never outlives it.
func benchLease(tb testing.TB, c *Catalog) Lease {
	tb.Helper()
	l, ok, err := c.TakeLease(tb.Context(), "compactor/2", "bench", 24*time.Hour)
	if err != nil || !ok {
		tb.Fatalf("TakeLease = %v, %v", ok, err)
	}

	return l
}

// fixedN returns the iterations -benchtime Nx fixes, and skips b unless the flag has that form
// with N times perIter ranges at most a tenth of the prior seeded ones, so the probe's population grows by at most a tenth.
func fixedN(b *testing.B, perIter, prior int) int {
	b.Helper()
	v := flag.Lookup("test.benchtime").Value.String()
	s, ok := strings.CutSuffix(v, "x")
	n, err := strconv.Atoi(s)
	if !ok || err != nil || n < 1 {
		b.Skipf("-benchtime %s: this benchmark runs at a fixed -benchtime Nx, N at most %d here", v, prior/10/perIter)
	}
	if n*perIter > prior/10 {
		b.Skipf("-benchtime %dx: %d packs of %d ranges exceed a tenth of the %d seeded ranges", n, n, perIter, prior)
	}

	return n
}

// seedHistory seeds one open capture of tool T1 with prior ranges, prior a multiple of historyHourRanges.
func seedHistory(tb testing.TB, c *Catalog, prior int) *history {
	tb.Helper()
	capture := seedCaptures(tb, c, 1, 1)[0]
	hours := int64(prior / historyHourRanges)
	packs, next := seedPacks(tb, c, capture, packsSeed{hour0: 1, hours: hours, packs: historyHourRanges / 10, ranges: 10, span: historySpan})

	return &history{capture: capture, prior: prior, packs: packs, next: next, lastHour: hours}
}

// seededRange is the r-th seeded range of h and its hour.
func (h *history) seededRange(r int) (Range, int64) {
	from := uint64(r) * historySpan //nolint:gosec // r is not negative
	hour := 1 + int64(r/historyHourRanges)
	ts := hourNS(hour) + int64(r%historyHourRanges)*historySpan

	return Range{SeqFrom: from, SeqTo: from + historySpan - 1, TSFrom: ts, TSTo: ts + historySpan - 1}, hour
}

// benchPack is a segment of h's capture in hour holding ranges, with the evidence of one delivery of each seq of delivered,
// every claimEvery-th of them by claim-check with its receipt.
func benchPack(tb testing.TB, h *history, hour int64, ranges []Range, delivered []uint64) (*Pack, *Evidence) {
	tb.Helper()
	id := newID(tb)
	p := &Pack{
		ID: id, Capture: h.capture, ToolID: "T1", Hour: hour, Role: RoleSegment, Key: "staging/" + id.String(), Size: 1 << 20,
		SeqStart: ranges[0].SeqFrom, FormatMinor: 2, Ranges: ranges,
	}
	ev := &Evidence{Deliveries: make([]Identity, 0, len(delivered))}
	for _, seq := range delivered {
		var header [44]byte
		binary.LittleEndian.PutUint64(header[:8], seq)
		v := VersionKey{Capture: h.capture, Seq: seq, Header: header, Digest: [32]byte{1}}
		ev.Deliveries = append(ev.Deliveries, Identity{Version: v, Incarnation: 1, StreamSeq: int64(seq) + 1, Hour: hour}) //nolint:gosec // a seeded seq is small
		if seq%claimEvery == 0 {
			ev.Receipts = append(ev.Receipts, Receipt{Seq: seq, Header: header, PayloadSHA256: v.Digest, ObjectKey: fmt.Sprintf("claims/%s.%d", h.capture, seq), ObjectSize: 1 << 10})
		}
	}

	return p, ev
}

// above returns a run of n seqs above h's seeded ranges and everything registered above them, advancing h.next past it.
func (h *history) above(n uint64) (first uint64) {
	first = h.next
	h.next += n

	return first
}

// evidenceShape is a pack of one range of d seqs, each delivered once: the evidence's processing, with one overlap probe.
func evidenceShape(d int) regShape {
	return regShape{ranges: 1, pack: func(tb testing.TB, h *history, _ int) (*Pack, *Evidence) {
		from := h.above(uint64(d)) //nolint:gosec // d is positive
		seqs := make([]uint64, d)
		for i := range seqs {
			seqs[i] = from + uint64(i) //nolint:gosec // i is not negative
		}
		ts := hourNS(h.lastHour) + int64(from) //nolint:gosec // a seeded seq is small

		return benchPack(tb, h, h.lastHour, []Range{{SeqFrom: from, SeqTo: from + uint64(d) - 1, TSFrom: ts, TSTo: ts + int64(d) - 1}}, seqs) //nolint:gosec // d is positive
	}}
}

// rangesShape is a pack of n one-seq ranges, a seq apart, and one delivery: n overlap probes, with almost no evidence.
func rangesShape(n int) regShape {
	return regShape{ranges: n, pack: func(tb testing.TB, h *history, _ int) (*Pack, *Evidence) {
		from := h.above(2 * uint64(n)) //nolint:gosec // n is positive
		ranges := make([]Range, n)
		for i := range ranges {
			seq := from + 2*uint64(i)             //nolint:gosec // i is not negative
			ts := hourNS(h.lastHour) + int64(seq) //nolint:gosec // a seeded seq is small
			ranges[i] = Range{SeqFrom: seq, SeqTo: seq, TSFrom: ts, TSTo: ts}
		}

		return benchPack(tb, h, h.lastHour, ranges, []uint64{from})
	}}
}

// missShape is a pack of one range of historySpan seqs just above the seeded history, one delivered: a probe that finds nothing.
func missShape() regShape {
	return regShape{ranges: 1, pack: func(tb testing.TB, h *history, _ int) (*Pack, *Evidence) {
		from := h.above(historySpan)
		ts := hourNS(h.lastHour) + int64(from) //nolint:gosec // a seeded seq is small

		return benchPack(tb, h, h.lastHour, []Range{{SeqFrom: from, SeqTo: from + historySpan - 1, TSFrom: ts, TSTo: ts + historySpan - 1}}, []uint64{from})
	}}
}

// hitShape is a pack repeating the seeded range i from the top in that range's hour, one seq delivered:
// a probe that finds one overlap near the end of the history, a distinct one each iteration, recording one suspect.
func hitShape() regShape {
	return regShape{ranges: 1, conflicts: 1, pack: func(tb testing.TB, h *history, i int) (*Pack, *Evidence) {
		r, hour := h.seededRange(h.prior - 1 - i)

		return benchPack(tb, h, hour, []Range{r}, []uint64{r.SeqFrom})
	}}
}

// BenchmarkRegisterPack is ingest's registration of a pack with populated evidence: one identity a delivery and a receipt a claim-check record.
// Its starting state is one open capture with prior seeded ranges (1,000, 20,000 and 200,000 by TRACEKEEP_BENCH_SCALE), the scope of their last hour open.
// One iteration registers a fresh pack, built outside the timer, and asserts it registered, the ranges it added and the suspects it recorded:
//   - evidence: one range of 100, 1,000 or 50,000 seqs above the history, each delivered, every tenth by claim-check;
//   - ranges: as many one-seq ranges above the history, one delivered, measuring the overlap probe apart from the evidence;
//   - miss and hit: one range of ten seqs, one delivered, just above the history or repeating a distinct seeded range from its top.
//
// It runs only at a fixed -benchtime Nx, N times the pack's ranges at most a tenth of the prior ones; the ranges before and after are logged.
func BenchmarkRegisterPack(b *testing.B) {
	priors := benchscale.Sizes(b, 1000, 20000, 200000)
	records := benchscale.Sizes(b, 100, 1000, 50000)
	for _, prior := range priors {
		for _, d := range records {
			b.Run(fmt.Sprintf("evidence/deliveries=%d/prior=%d", d, prior), func(b *testing.B) { benchRegister(b, prior, evidenceShape(d)) })
			b.Run(fmt.Sprintf("ranges/ranges=%d/prior=%d", d, prior), func(b *testing.B) { benchRegister(b, prior, rangesShape(d)) })
		}
		b.Run(fmt.Sprintf("miss/prior=%d", prior), func(b *testing.B) { benchRegister(b, prior, missShape()) })
		b.Run(fmt.Sprintf("hit/prior=%d", prior), func(b *testing.B) { benchRegister(b, prior, hitShape()) })
	}
}

func benchRegister(b *testing.B, prior int, shape regShape) {
	n := fixedN(b, shape.ranges, prior)
	benchEngines(b, func(b *testing.B, c *Catalog) {
		h := seedHistory(b, c, prior)
		ranges := `SELECT COUNT(*) FROM pack_ranges WHERE capture_id = ?`
		before := count(b, c, ranges, h.capture[:])
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			b.StopTimer()
			p, ev := shape.pack(b, h, i)
			b.StartTimer()
			out, err := c.RegisterPack(b.Context(), p, Registration{Evidence: ev})
			b.StopTimer()
			if err != nil || out != Registered {
				b.Fatalf("RegisterPack = %v, %v; want registered", out, err)
			}
			if got := count(b, c, `SELECT COUNT(*) FROM pack_ranges WHERE pack_id = ?`, p.ID[:]); got != shape.ranges {
				b.Fatalf("the pack added %d ranges, want %d", got, shape.ranges)
			}
			if got := count(b, c, `SELECT COUNT(*) FROM conflicts WHERE pack_id = ?`, p.ID[:]); got != shape.conflicts {
				b.Fatalf("the pack recorded %d suspects, want %d", got, shape.conflicts)
			}
			i++
			b.StartTimer()
		}
		if b.N != n {
			b.Fatalf("ran %d iterations, -benchtime fixed %d", b.N, n)
		}
		b.Logf("pack_ranges of the capture: %d before, %d after %d packs", before, count(b, c, ranges, h.capture[:]), b.N)
	})
}

// BenchmarkResolve is ingest's resolution of a delivery under an active termination order of its capture, which probes the held seqs.
// Its starting state is one open capture with prior seeded ranges (1,000, 20,000 and 200,000 by TRACEKEEP_BENCH_SCALE)
// and an active order covering every stream seq; no version is parked.
// One iteration resolves one delivery and asserts its resolution: held, the top seeded seq, staged by its pack;
// unheld, the seq above them, covered by the order. It writes nothing.
func BenchmarkResolve(b *testing.B) {
	for _, prior := range benchscale.Sizes(b, 1000, 20000, 200000) {
		b.Run(fmt.Sprintf("prior=%d", prior), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				h := seedHistory(b, c, prior)
				exec(b, c, `INSERT INTO termination_orders (action_id, capture_id, incarnation_id, cut_off, state) VALUES ('a1', ?, 1, ?, 'active')`,
					h.capture[:], int64(1)<<40)
				for _, tc := range []struct {
					name string
					seq  uint64
					want Resolution
				}{{"held", h.next - 1, ResolvedStaged}, {"unheld", h.next, CoveredByOrder}} {
					v := VersionKey{Capture: h.capture, Seq: tc.seq, Header: [44]byte{1}, Digest: [32]byte{1}}
					b.Run(tc.name, func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							if got, err := c.Resolve(b.Context(), &v, 1, int64(tc.seq)+1); err != nil || got != tc.want { //nolint:gosec // a seeded seq is small
								b.Fatalf("Resolve = %v, %v; want %v", got, err, tc.want)
							}
						}
					})
				}
			})
		})
	}
}

// BenchmarkAuditWhileRegistering is an API write's wait for the write connection while ingest registers.
// Its starting state is one open capture per registering writer, each writer registering packs of 100 delivered seqs above its capture's last,
// one after another, from before the loop until after it; registering=0 is the idle control.
// One iteration appends one audit row and asserts it written; after the loop the audit log grew by exactly the iterations
// and every writer registered without error. The writers are 1, 4 and 16 by TRACEKEEP_BENCH_SCALE.
func BenchmarkAuditWhileRegistering(b *testing.B) {
	for _, writers := range append([]int{0}, benchscale.Sizes(b, 1, 4, 16)...) {
		b.Run(fmt.Sprintf("registering=%d", writers), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				captures := seedCaptures(b, c, 1, max(writers, 1))
				stop, wait, registered := registering(b, c, captures[:writers])
				before := count(b, c, `SELECT COUNT(*) FROM audit_log`)
				regsBefore := registered.Load()
				e := AuditEntry{Actor: "oidc:bench", Action: "restore", ActionID: "a1", Capture: &captures[0]}
				b.ReportAllocs()
				for b.Loop() {
					if err := c.Audit(b.Context(), e); err != nil {
						b.Fatal(err)
					}
				}
				during := registered.Load() - regsBefore
				stop()
				if err := wait(); err != nil {
					b.Fatalf("a writer failed: %v", err)
				}
				if got := count(b, c, `SELECT COUNT(*) FROM audit_log`); got != before+b.N {
					b.Fatalf("audit rows %d, want %d", got, before+b.N)
				}
				b.Logf("%d registrations completed during %d audit writes", during, b.N)
			})
		})
	}
}

// registering starts one writer for each capture, registering packs of 100 delivered seqs until stop,
// and returns once each has registered its first; wait joins them and returns the first error, and registered counts their packs.
func registering(tb testing.TB, c *Catalog, captures []tracepack.UUID) (stop func(), wait func() error, registered *atomic.Int64) {
	tb.Helper()
	regs := new(atomic.Int64)
	done := make(chan struct{})
	var wg, first sync.WaitGroup
	errs := make(chan error, len(captures))
	for _, capture := range captures {
		h := &history{capture: capture, lastHour: 1}
		shape := evidenceShape(100)
		first.Add(1)
		wg.Go(func() {
			once := sync.OnceFunc(first.Done)
			defer once()
			for {
				p, ev := shape.pack(tb, h, 0)
				if _, err := c.RegisterPack(tb.Context(), p, Registration{Evidence: ev}); err != nil {
					errs <- err
					return
				}
				regs.Add(1)
				once()
				select {
				case <-done:
					return
				default:
				}
			}
		})
	}
	first.Wait()

	return func() { close(done) }, func() error {
		wg.Wait()
		close(errs)

		return <-errs
	}, regs
}
