package catalog

import (
	"encoding/binary"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

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
// pack, which builds iteration i's pack and evidence, and parkedEvery, which parks every parkedEvery-th delivery's version first (none at zero).
type regShape struct {
	ranges, conflicts int
	pack              func(tb testing.TB, h *history, i int) (*Pack, *Evidence)
	parkedEvery       int
}

// parkedStates are the states a parked delivery's version is seeded in, in turn: those a registration reads and still registers past,
// the unresolved two its staging moves to staged and one already staged.
var parkedStates = []VersionState{VersionParking, VersionParked, VersionStaged}

// parkShare seeds a parked row of every every-th delivery of ev, the k-th of them in parkedStates[k mod 3], and returns their versions.
func parkShare(tb testing.TB, c *Catalog, ev *Evidence, every int) []VersionKey {
	tb.Helper()
	var ds []Identity
	var states []VersionState
	for i := 0; i < len(ev.Deliveries); i += every {
		ds = append(ds, ev.Deliveries[i])
		states = append(states, parkedStates[len(states)%len(parkedStates)])
	}
	seedVersions(tb, c, ds, states)
	vs := make([]VersionKey, len(ds))
	for i := range ds {
		vs[i] = ds[i].Version
	}

	return vs
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

// parkedShape is evidenceShape(d) with a tenth of its deliveries' versions parked before the registration, which stages them.
func parkedShape(d int) regShape {
	s := evidenceShape(d)
	s.parkedEvery = 10

	return s
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
//   - parked: as evidence, with every tenth delivery's version parked beforehand, outside the timer, in turn parking, parked and staged,
//     so the registration reads each state it registers past; it asserts every one of them staged after;
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
			b.Run(fmt.Sprintf("parked/deliveries=%d/prior=%d", d, prior), func(b *testing.B) { benchRegister(b, prior, parkedShape(d)) })
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
			var parked []VersionKey
			if shape.parkedEvery > 0 {
				parked = parkShare(b, c, ev, shape.parkedEvery)
			}
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
			checkReceipts(b, c, p, ev)
			for _, v := range parked {
				if state, err := readVersionState(b.Context(), c.read, &v); err != nil || state != VersionStaged {
					b.Fatalf("the parked version of seq %d = %q, %v; want staged", v.Seq, state, err)
				}
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

// checkReceipts asserts the evidence p's registration wrote: a receipt of each claim-check record, naming p first, with its header and digest,
// and its live claim object, and nothing else over p's seqs.
func checkReceipts(tb testing.TB, c *Catalog, p *Pack, ev *Evidence) {
	tb.Helper()
	lo, hi := int64(p.Ranges[0].SeqFrom), int64(p.Ranges[len(p.Ranges)-1].SeqTo) //nolint:gosec // a seeded seq is small
	want, wantKeys := make([]string, 0, len(ev.Receipts)), make([]string, 0, len(ev.Receipts))
	for _, rc := range ev.Receipts {
		want = append(want, fmt.Sprintf("%d/%x/%x", rc.Seq, rc.Header, rc.PayloadSHA256))
		wantKeys = append(wantKeys, fmt.Sprintf("%d/%s/live", rc.Seq, rc.ObjectKey))
	}
	var receipts []struct {
		Seq    int64  `db:"seq"`
		Header []byte `db:"header"`
		Digest []byte `db:"payload_sha256"`
	}
	err := c.read.SelectContext(tb.Context(), &receipts, `SELECT seq, header, payload_sha256 FROM claim_records
		WHERE capture_id = ? AND seq BETWEEN ? AND ? AND first_pack_id = ? ORDER BY seq`, p.Capture[:], lo, hi, p.ID[:])
	if err != nil {
		tb.Fatal(err)
	}
	got := make([]string, 0, len(receipts))
	for _, r := range receipts {
		got = append(got, fmt.Sprintf("%d/%x/%x", r.Seq, r.Header, r.Digest))
	}
	var objects []struct {
		Seq   int64  `db:"seq"`
		Key   string `db:"object_key"`
		State string `db:"state"`
	}
	err = c.read.SelectContext(tb.Context(), &objects, `SELECT seq, object_key, state FROM claim_objects
		WHERE capture_id = ? AND seq BETWEEN ? AND ? ORDER BY seq`, p.Capture[:], lo, hi)
	if err != nil {
		tb.Fatal(err)
	}
	gotKeys := make([]string, 0, len(objects))
	for _, o := range objects {
		gotKeys = append(gotKeys, fmt.Sprintf("%d/%s/%s", o.Seq, o.Key, o.State))
	}
	if !slices.Equal(got, want) || !slices.Equal(gotKeys, wantKeys) {
		tb.Fatalf("the pack wrote %d receipts and %d claim objects, want %d of each as its evidence names them", len(got), len(gotKeys), len(want))
	}
}

// BenchmarkResolve is ingest's resolution of a delivery under an active termination order of its capture, which probes the held seqs.
// Its starting state is one open capture with prior seeded ranges (1,000, 20,000 and 200,000 by TRACEKEEP_BENCH_SCALE)
// and an active order covering every stream seq; no version is parked.
// One iteration resolves one delivery and asserts its resolution: held, the top seeded seq, staged by its pack;
// unheld, the seq above them, covered by the order; held-start, a seq of the first seeded range, far from the end, staged.
// Then the history gains a merged hour above it, hour lastHour+1, whose archive holds one range of an hour's seqs (historyHourRanges*historySpan),
// the widest range a capture stores: wide-stored resolves the top seeded seq again, staged, beside it;
// then that hour is evicted, outside any timer, so its range leaves pack_ranges, and wide-evicted resolves the same seq, staged.
// They write nothing.
func BenchmarkResolve(b *testing.B) {
	for _, prior := range benchscale.Sizes(b, 1000, 20000, 200000) {
		b.Run(fmt.Sprintf("prior=%d", prior), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				h := seedHistory(b, c, prior)
				exec(b, c, `INSERT INTO termination_orders (action_id, capture_id, incarnation_id, cut_off, state) VALUES ('a1', ?, 1, ?, 'active')`,
					h.capture[:], int64(1)<<40)
				resolve := func(name string, seq uint64, want Resolution) {
					v := VersionKey{Capture: h.capture, Seq: seq, Header: [44]byte{1}, Digest: [32]byte{1}}
					b.Run(name, func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							if got, err := c.Resolve(b.Context(), &v, 1, int64(seq)+1); err != nil || got != want { //nolint:gosec // a seeded seq is small
								b.Fatalf("Resolve = %v, %v; want %v", got, err, want)
							}
						}
					})
				}
				resolve("held", h.next-1, ResolvedStaged)
				resolve("unheld", h.next, CoveredByOrder)
				resolve("held-start", historySpan/2, ResolvedStaged)
				wide := seedWideHour(b, c, h)
				resolve("wide-stored", h.next-1, ResolvedStaged)
				l := benchLease(b, c)
				if _, err := c.RecordRetention(b.Context(), l, month, delay, 0); err != nil {
					b.Fatal(err)
				}
				ev, err := c.EvictHour(b.Context(), l, Scope{Capture: h.capture, Hour: wide}, hourNS(1<<20)+int64(month))
				if err != nil || !ev.Evicted || len(ev.Packs) != 1 {
					b.Fatalf("EvictHour = %+v, %v; want the merged hour evicted", ev, err)
				}
				if got := count(b, c, `SELECT COUNT(*) FROM pack_ranges WHERE capture_id = ?`, h.capture[:]); got != prior {
					b.Fatalf("%d ranges after the eviction, want the %d seeded", got, prior)
				}
				resolve("wide-evicted", h.next-1, ResolvedStaged)
			})
		})
	}
}

// seedWideHour writes a merged hour of h's capture above its history, hour lastHour+1:
// its scope, and its archive holding one range of an hour's seqs from h.next, the merge's output; it returns the hour.
// h.next stays below it, so the probes of the history are unchanged.
func seedWideHour(tb testing.TB, c *Catalog, h *history) int64 {
	tb.Helper()
	hour, id := h.lastHour+1, newID(tb)
	from, span := h.next, uint64(historyHourRanges*historySpan)
	seedTx(tb, c, func(tx *sqlx.Tx) {
		insertRows(tb, tx, "packs", []string{"pack_id", "capture_id", "tool_id", "hour", "role", "object_key", "size", "seq_start", "format_minor", "state", "adopted", "registered_at"},
			[][]any{{id[:], h.capture[:], "T1", hour, RoleArchive, "staging/" + id.String(), 1 << 20, int64(from), 2, "staged", false, 0}}) //nolint:gosec // a seeded seq is small
		insertRows(tb, tx, "pack_ranges", []string{"pack_id", "capture_id", "seq_from", "seq_to", "ts_from", "ts_to"},
			[][]any{{id[:], h.capture[:], int64(from), int64(from + span - 1), hourNS(hour), hourNS(hour) + int64(span) - 1}}) //nolint:gosec // a seeded seq is small
		insertRows(tb, tx, "scopes", []string{"capture_id", "hour", "state", "revision"}, [][]any{{h.capture[:], hour, "settled", 2}})
	})
	if got := count(tb, c, `SELECT COUNT(*) FROM pack_ranges WHERE capture_id = ? AND seq_to - seq_from = ?`, h.capture[:], int64(span)-1); got != 1 { //nolint:gosec // a small span
		tb.Fatalf("seeded one wide range, found %d", got)
	}

	return hour
}

// writerSeed is the prior ranges each registering writer's capture holds, the population every registration probes.
const writerSeed = historyHourRanges

// BenchmarkAuditWhileRegistering is an API write's wait for the write connection while ingest registers.
// Its starting state is one open capture per registering writer, each seeded with writerSeed ranges in hour 1;
// each writer registers one pack of 100 delivered seqs above them, every tenth by claim-check, then deletes that pack's rows in a second transaction,
// over and over from before the loop until after it, so every registration probes the same population and nothing the writers write accumulates;
// registering=0 is the idle control.
// One iteration appends one audit row; outside the timer it asserts that row written and deletes it, so every iteration writes into the same audit log.
// After the loop the writers' tables hold what they were seeded with, the audit log what it held before,
// and every writer ran without error; the counts before and after are logged.
// The writers are 1, 4 and 16 by TRACEKEEP_BENCH_SCALE.
func BenchmarkAuditWhileRegistering(b *testing.B) {
	for _, writers := range append([]int{0}, benchscale.Sizes(b, 1, 4, 16)...) {
		b.Run(fmt.Sprintf("registering=%d", writers), func(b *testing.B) {
			benchEngines(b, func(b *testing.B, c *Catalog) {
				captures := seedCaptures(b, c, 1, max(writers, 1))
				hs := make([]*history, writers)
				for i := range hs {
					packs, next := seedPacks(b, c, captures[i], packsSeed{hour0: 1, hours: 1, packs: writerSeed / 10, ranges: 10, span: historySpan})
					hs[i] = &history{capture: captures[i], prior: writerSeed, packs: packs, next: next, lastHour: 1}
				}
				population := tableCounts(b, c, writerTables...)
				stop, wait, registered := registering(b, c, hs)
				audits := count(b, c, `SELECT COUNT(*) FROM audit_log`)
				regsBefore := registered.Load()
				e := AuditEntry{Actor: "oidc:bench", Action: "restore", ActionID: "a1", Capture: &captures[0]}
				b.ReportAllocs()
				for b.Loop() {
					if err := c.Audit(b.Context(), e); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					if got := count(b, c, `SELECT COUNT(*) FROM audit_log WHERE actor = ?`, e.Actor); got != 1 {
						b.Fatalf("%d audit rows of the iteration, want 1", got)
					}
					exec(b, c, `DELETE FROM audit_log WHERE actor = ?`, e.Actor)
					b.StartTimer()
				}
				during := registered.Load() - regsBefore
				stop()
				if err := wait(); err != nil {
					b.Fatalf("a writer failed: %v", err)
				}
				if got := tableCounts(b, c, writerTables...); got != population {
					b.Fatalf("the writers' tables hold %s, seeded with %s", got, population)
				}
				after := count(b, c, `SELECT COUNT(*) FROM audit_log`)
				if after != audits {
					b.Fatalf("audit rows %d after %d iterations, want the %d before", after, b.N, audits)
				}
				b.Logf("%d registrations completed during %d audit writes; the writers' tables %s before and after; audit rows %d before, %d after",
					during, b.N, population, audits, after)
			})
		})
	}
}

// writerTables are the tables a registering writer writes.
var writerTables = []string{"packs", "pack_ranges", "claim_records", "claim_objects", "scope_events", "conflicts"}

// tableCounts counts the rows of tables, as one line.
func tableCounts(tb testing.TB, c *Catalog, tables ...string) string {
	tb.Helper()
	parts := make([]string, 0, len(tables))
	for _, table := range tables {
		parts = append(parts, fmt.Sprintf("%s=%d", table, count(tb, c, `SELECT COUNT(*) FROM `+table)))
	}

	return strings.Join(parts, " ")
}

// registering starts one writer for each history, each registering a pack of 100 delivered seqs just above its history
// and then deleting that pack's rows, until stop; it returns once each has registered its first.
// wait joins them and returns the first error, and registered counts their registrations.
func registering(tb testing.TB, c *Catalog, hs []*history) (stop func(), wait func() error, registered *atomic.Int64) {
	tb.Helper()
	regs := new(atomic.Int64)
	done := make(chan struct{})
	var wg, first sync.WaitGroup
	errs := make(chan error, len(hs))
	for _, h := range hs {
		shape, base := evidenceShape(100), h.next
		first.Add(1)
		wg.Go(func() {
			once := sync.OnceFunc(first.Done)
			defer once()
			for {
				h.next = base
				p, ev := shape.pack(tb, h, 0)
				out, err := c.RegisterPack(tb.Context(), p, Registration{Evidence: ev})
				if err == nil && out != Registered {
					err = fmt.Errorf("registration %v", out)
				}
				if err == nil {
					err = unregister(tb, c, p)
				}
				if err != nil {
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

// unregister deletes, in one write transaction, the rows a registration of p into an open scope with evidence and no overlap wrote:
// the pack, its ranges, its receipts and their claim objects.
func unregister(tb testing.TB, c *Catalog, p *Pack) error {
	tb.Helper()
	tx, err := c.write.BeginTxx(tb.Context(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	lo, hi := int64(p.Ranges[0].SeqFrom), int64(p.Ranges[len(p.Ranges)-1].SeqTo) //nolint:gosec // a seeded seq is small
	for _, stmt := range []struct {
		q    string
		args []any
	}{
		{`DELETE FROM pack_ranges WHERE pack_id = ?`, []any{p.ID[:]}},
		{`DELETE FROM packs WHERE pack_id = ?`, []any{p.ID[:]}},
		{`DELETE FROM claim_records WHERE capture_id = ? AND seq BETWEEN ? AND ?`, []any{p.Capture[:], lo, hi}},
		{`DELETE FROM claim_objects WHERE capture_id = ? AND seq BETWEEN ? AND ?`, []any{p.Capture[:], lo, hi}},
	} {
		if _, err := tx.ExecContext(tb.Context(), stmt.q, stmt.args...); err != nil {
			return err
		}
	}

	return tx.Commit()
}
