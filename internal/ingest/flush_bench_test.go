package ingest

import (
	"flag"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/store"
)

// seededRanges is the ranges a flush benchmark registers in each of its captures before it starts, one segment of seedSeqs seqs each:
// registration's bound lets a run add a tenth of them, so a flush benchmark runs only at -benchtime Nx with N at most seededRanges/10.
const seededRanges = 1000

// seedSeqs is the seqs of one seeded segment.
const seedSeqs = 100

// benchFlush is a capture a flush benchmark writes: admitted, seeded and fed fresh seqs.
type benchFlush struct {
	x       *Executor
	capture tracepack.UUID
	payload []byte
	// next is the first seq not yet written.
	next uint64
}

// payload is a frame a flush benchmark writes, by name.
type payload struct {
	name  string
	frame []byte
}

// newBenchFlush admits capture on x by its start and seeds it with seededRanges segments of contiguous seqs from 0 in now's hour,
// registered with empty evidence, so the flushes that follow continue its history without a gap.
func newBenchFlush(tb testing.TB, x *Executor, capture tracepack.UUID, payload []byte) *benchFlush {
	tb.Helper()
	if _, dec := dispose(tb, x, inline(tb, capture, startRecord(tb, now), descriptorOf(tb, "eqp"))); dec.Action != ActBatch {
		tb.Fatalf("start disposed %+v, want batched", dec)
	}
	for i := range uint64(seededRanges) {
		from := i * seedSeqs
		p := &catalog.Pack{
			ID: newPackID(), Capture: capture, ToolID: "T1", Hour: HourOf(now), Role: catalog.RoleSegment, Key: "seed/" + fmt.Sprint(i),
			Size: 1, SeqStart: from, FormatMinor: 2,
			Ranges: []catalog.Range{{SeqFrom: from, SeqTo: from + seedSeqs - 1, TSFrom: now + int64(from), TSTo: now + int64(from+seedSeqs-1)}}, //nolint:gosec // small seqs
		}
		if out, err := x.Catalog.RegisterPack(tb.Context(), p, catalog.Registration{Evidence: &catalog.Evidence{}}); err != nil || out != catalog.Registered {
			tb.Fatalf("seed registration = %v, %v", out, err)
		}
	}

	return &benchFlush{x: x, capture: capture, payload: payload, next: seededRanges * seedSeqs}
}

// batch disposes n fresh records of the capture, the seqs after the last batch's, all in now's hour, into a new batch on h.
func (s *benchFlush) batch(tb testing.TB, n int, h Msg) *Batch {
	tb.Helper()
	b := &Batch{Key: Key{Capture: s.capture, Hour: HourOf(now)}, Entries: make([]Entry, 0, n)}
	for range n {
		r := tracepack.Record{
			Seq: s.next, TSUTCNs: now + int64(s.next), Kind: tracepack.KindData, //nolint:gosec // a small seq
			Dir: tracepack.DirEquipmentToHost, Fidelity: tracepack.FidelityWireExact, Payload: s.payload,
		}
		d, dec := dispose(tb, s.x, inline(tb, s.capture, r, nil))
		if dec.Action != ActBatch {
			tb.Fatalf("seq %d disposed %+v, want batched", r.Seq, dec)
		}
		b.ConfigID = d.ConfigID
		b.Entries = append(b.Entries, Entry{D: d, Msg: h})
		s.next++
	}

	return b
}

// payloads are the frames a flush benchmark writes: dataRecord's 14 bytes and an S6F11 of 1 KiB.
func payloads() []payload {
	return []payload{
		{"14B", dataRecord(1, now).Payload},
		{"1KiB", maskingtest.Frame(6, 11, maskingtest.ASCII(strings.Repeat("v", 1024-14-3)))},
	}
}

// counts are the packs and ranges the catalog holds, which a flush benchmark logs before and after its run.
func counts(tb testing.TB, c *catalog.Catalog) (packs, ranges int) {
	tb.Helper()
	_, read := c.Pools()
	if err := read.QueryRowContext(tb.Context(), `SELECT COUNT(*) FROM packs`).Scan(&packs); err != nil {
		tb.Fatal(err)
	}
	if err := read.QueryRowContext(tb.Context(), `SELECT COUNT(*) FROM pack_ranges`).Scan(&ranges); err != nil {
		tb.Fatal(err)
	}

	return packs, ranges
}

// fixedIterations returns the iterations -benchtime Nx fixes, and skips b, naming both numbers, unless the flag has that form
// with N at most a tenth of seededRanges: each iteration adds one range a capture, so the probe's population grows by at most a tenth.
func fixedIterations(b *testing.B) int {
	b.Helper()
	v := flag.Lookup("test.benchtime").Value.String()
	s, ok := strings.CutSuffix(v, "x")
	n, err := strconv.Atoi(s)
	if !ok || err != nil || n < 1 {
		b.Skipf("-benchtime %s: this benchmark runs at a fixed -benchtime Nx, N at most %d", v, seededRanges/10)
	}
	if n > seededRanges/10 {
		b.Skipf("-benchtime %dx: %d ranges a capture exceed a tenth of the %d seeded", n, n, seededRanges)
	}

	return n
}

// logCounts records the catalog's packs and ranges before a run of n iterations;
// the function it returns asserts the run's iterations and the ranges it added, one a capture an iteration, and logs both counts with the result.
func logCounts(b *testing.B, c *catalog.Catalog, n, captures int) func() {
	b.Helper()
	packs, ranges := counts(b, c)

	return func() {
		if b.N != n {
			b.Fatalf("ran %d iterations, -benchtime fixed %d", b.N, n)
		}
		p, r := counts(b, c)
		if r-ranges != n*captures {
			b.Fatalf("the run added %d ranges, want %d", r-ranges, n*captures)
		}
		b.Logf("catalog: packs %d -> %d, ranges %d -> %d; %d added over %d seeded a capture", packs, p, ranges, r, r-ranges, seededRanges)
	}
}

// benchFlusher is newPipeline's flusher on st, which is local file work when local.
func benchFlusher(tb testing.TB, x *Executor, st store.ObjectStore, local bool) *Flusher {
	tb.Helper()

	return &Flusher{
		Catalog: x.Catalog, Store: st, WorkDir: tb.TempDir(), Configs: x.Configs, Interval: time.Second, LocalStore: local,
		HorizonHour: func() int64 { return HourOf(x.Horizon()) }, Writer: "tracekeep-bench", Now: func() time.Time { return time.Unix(0, now) },
	}
}

// benchStore is the store named name: the memory store, or the fs store under a fresh directory, whose puts are local file work.
func benchStore(tb testing.TB, name string) (store.ObjectStore, bool) {
	tb.Helper()
	if name == "mem" {
		return store.NewMem(), false
	}
	st, err := store.NewFS(tb.TempDir())
	if err != nil {
		tb.Fatal(err)
	}

	return st, true
}

// BenchmarkFlush is Flusher.Flush of one batch on the SQLite catalog (P11's fs puts):
// its segment written to the working file and synced, reopened, put, registered with its evidence, and each message acknowledged.
//   - Starting state: one admitted capture seeded with seededRanges segments, a store (memory, or fs under a temporary directory) and a counting Msg.
//   - One iteration flushes n fresh records of the payload's size, disposed outside the timer with seqs after the last iteration's,
//     so each registers one range above the history and meets no overlap; the batch is new each iteration, since the split sorts its entries in place.
//   - Each iteration asserts one pack registered and n messages acknowledged, nothing beyond the horizon or terminated.
//   - Registration's bound: a run adds one range an iteration, so it runs only at a fixed -benchtime Nx with N at most seededRanges/10 (100),
//     and skips otherwise, naming both numbers; it asserts its iterations and the ranges it added, and logs the catalog's packs and ranges before and after.
//   - The working file's sync is included: the Writer syncs it before the put.
func BenchmarkFlush(b *testing.B) {
	for _, stName := range []string{"mem", "fs"} {
		for _, p := range payloads() {
			for _, n := range benchscale.Sizes(b, 100, 1000, 10000) {
				b.Run(fmt.Sprintf("store=%s/payload=%s/records=%d", stName, p.name, n), func(b *testing.B) {
					iters := fixedIterations(b)
					c := catalogtest.Open(b, catalog.EngineSQLite)
					x := executor(b, c, newClaims())
					st, local := benchStore(b, stName)
					f := benchFlusher(b, x, st, local)
					s := newBenchFlush(b, x, tracepack.UUID{0xf1}, p.frame)
					h := &counting{}
					done := logCounts(b, c, iters, 1)
					ctx := b.Context()
					b.SetBytes(int64(n * len(p.frame)))
					b.ReportAllocs()
					for b.Loop() {
						b.StopTimer()
						batch := s.batch(b, n, h)
						acked := h.acked.Load()
						b.StartTimer()
						fl, err := f.Flush(ctx, batch)
						if err != nil || fl.Acked != n || len(fl.Packs) != 1 || fl.BeyondHorizon || fl.Terminated {
							b.Fatalf("Flush = %+v, %v; want one pack and %d acknowledged", fl, err, n)
						}
						if got := h.acked.Load() - acked; got != int64(n) {
							b.Fatalf("%d messages acknowledged, want %d", got, n)
						}
					}
					done()
				})
			}
		}
	}
}

// BenchmarkFlushDue is the ingester's flush of several batches due at once, Ingester.flush (P9):
// each batch flushed in turn, the heartbeat's held messages handed over before each, and its messages settled.
//   - Starting state: k admitted captures, each seeded with seededRanges segments, the memory store and a counting Msg.
//   - One iteration flushes k batches of 100 fresh 14-byte records, one a capture, all due together:
//     disposed, batched and taken from the batcher outside the timer, seqs after the last iteration's.
//   - Each iteration asserts no error, every one of the k×100 messages acknowledged and nothing left held.
//   - Registration's bound: a run adds one range a capture an iteration, so it runs only at a fixed -benchtime Nx with N at most seededRanges/10 (100),
//     and skips otherwise, naming both numbers; it asserts its iterations and the ranges it added, and logs the catalog's packs and ranges before and after.
func BenchmarkFlushDue(b *testing.B) {
	const records = 100
	for _, k := range benchscale.Sizes(b, 2, 10, 50) {
		b.Run(fmt.Sprintf("batches=%d", k), func(b *testing.B) {
			iters := fixedIterations(b)
			c := catalogtest.Open(b, catalog.EngineSQLite)
			x := executor(b, c, newClaims())
			st, _ := benchStore(b, "mem")
			g := &Ingester{
				Executor: x, Batcher: NewBatcher(time.Second, 1<<30), Flusher: benchFlusher(b, x, st, false),
				Log: slog.New(slog.DiscardHandler), Now: func() time.Time { return time.Unix(0, now) },
			}
			flushes := make([]*benchFlush, k)
			for i := range flushes {
				flushes[i] = newBenchFlush(b, x, tracepack.UUID{0xf2, byte(i)}, dataRecord(1, now).Payload)
			}
			h := &counting{}
			done := logCounts(b, c, iters, k)
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				for _, s := range flushes {
					for _, e := range s.batch(b, records, h).Entries {
						g.Batcher.Add(e, g.Now())
					}
				}
				due := g.take(func() []*Batch { return g.Batcher.Due(g.Now().Add(time.Second)) })
				if len(due) != k {
					b.Fatalf("%d batches due, want %d", len(due), k)
				}
				acked := h.acked.Load()
				b.StartTimer()
				if err := g.flush(ctx, due); err != nil {
					b.Fatal(err)
				}
				if got := h.acked.Load() - acked; got != int64(k*records) {
					b.Fatalf("%d messages acknowledged, want %d", got, k*records)
				}
				if len(g.flushing) != 0 || g.batched() != 0 {
					b.Fatalf("%d messages still flushing and %d batched, want none", len(g.flushing), g.batched())
				}
			}
			done()
		})
	}
}
