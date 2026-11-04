package records

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// benchReaders is the reader's bound on the packs a merge opens, max_readers' default; no cluster here exceeds it.
const benchReaders = 16

// smallPayload is a data record's frame of BenchmarkRead, its HSMS header alone.
const smallPayload = 14

// benchSeqRangePacks is the segments BenchmarkReadSeqRange's hour is split into: twelve, an hour of five-minute flushes;
// benchSeqRange is the seqs its range selects.
const (
	benchSeqRangePacks = 12
	benchSeqRange      = 10
)

// BenchmarkRead reads an hour of n records in k packs, by Reader.Read over an fs store, the reader's baseline.
// Its starting state is the world serving one read set of k staged segments, written once outside the timer:
// disjoint packs each hold a run of consecutive seqs, k clusters of one pack;
// overlapping packs interleave their seqs (pack i holds every k-th seq from i), one cluster of k packs merged together, no seq held twice.
// An iteration reads the whole hour into a counting sink, writes nothing, and asserts n records and no incompleteness.
// Its sizes are 1,000, 72,000 and 360,000 records (TRACEKEEP_BENCH_SCALE); 1, 12 and 16 packs and both layouts run at every size.
func BenchmarkRead(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 72_000, 360_000) {
		for _, k := range []int{1, 12, benchReaders} {
			for _, overlap := range []bool{false, true} {
				if overlap && k == 1 {
					continue // one pack has nothing to overlap
				}
				layout := "disjoint"
				if overlap {
					layout = "overlapping"
				}
				b.Run(fmt.Sprintf("records=%d/packs=%d/%s", n, k, layout), func(b *testing.B) {
					w := newWorld(b)
					w.serve(benchPacks(b, w, n, k, overlap, smallPayload))
					r := w.reader(benchReaders)
					b.ReportAllocs()
					for b.Loop() {
						var s counting
						if err := r.Read(b.Context(), window(), &s); err != nil || s.records != n || s.incomplete != 0 {
							b.Fatalf("Read = %d records, %d incomplete, %v; want %d, 0", s.records, s.incomplete, err, n)
						}
					}
				})
			}
		}
	}
}

// BenchmarkReadSeqRange reads a seq range of an hour of n records in benchSeqRangePacks packs, by Reader.Read over an fs store.
// Its starting state is BenchmarkRead's, disjoint or overlapping, its records' frames of 14 B or 1 KiB, written once outside the timer
// (at 360,000 records of 1 KiB, about 360 MiB of packs in the test's temporary directory).
// An iteration reads benchSeqRange seqs, the first of the hour, the middle, the last, or an empty range just past the last,
// into a counting sink, writes nothing, and asserts benchSeqRange records, none for the empty range, and no incompleteness.
// Its sizes are 1,000, 72,000 and 360,000 records (TRACEKEEP_BENCH_SCALE); both layouts and payloads run at every size.
func BenchmarkReadSeqRange(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 1000, 72_000, 360_000) {
		for _, overlap := range []bool{false, true} {
			layout := "disjoint"
			if overlap {
				layout = "overlapping"
			}
			for _, payload := range []int{smallPayload, 1 << 10} {
				b.Run(fmt.Sprintf("records=%d/packs=%d/%s/payload=%s", n, benchSeqRangePacks, layout, payloadName(payload)), func(b *testing.B) {
					w := newWorld(b)
					w.serve(benchPacks(b, w, n, benchSeqRangePacks, overlap, payload))
					r := w.reader(benchReaders)
					last := uint64(n) //nolint:gosec // a positive count
					for _, rg := range []struct {
						name  string
						from  uint64
						count int
					}{
						{"first", 1, benchSeqRange},
						{"middle", last / 2, benchSeqRange},
						{"last", last - benchSeqRange + 1, benchSeqRange},
						{"empty", last + 1, 0},
					} {
						b.Run("range="+rg.name, func(b *testing.B) {
							q := window()
							to := rg.from + benchSeqRange - 1
							q.SeqFrom, q.SeqTo = &rg.from, &to
							b.ReportAllocs()
							for b.Loop() {
								var s counting
								if err := r.Read(b.Context(), q, &s); err != nil || s.records != rg.count || s.incomplete != 0 {
									b.Fatalf("Read of seqs %d to %d = %d records, %d incomplete, %v; want %d, 0", rg.from, to, s.records, s.incomplete, err, rg.count)
								}
							}
						})
					}
				})
			}
		}
	}
}

// payloadName names a frame's size, in KiB when it is a multiple of one.
func payloadName(n int) string {
	if n%(1<<10) == 0 {
		return fmt.Sprintf("%dKiB", n>>10)
	}

	return fmt.Sprintf("%dB", n)
}

// benchPacks writes n data records, seqs 1 to n, as k staged segments of the test's hour, interleaved when overlap is set,
// each record's frame of payload bytes (sized).
func benchPacks(b *testing.B, w *world, n, k int, overlap bool, payload int) []catalog.ReadPack {
	b.Helper()
	parts := make([][]tracepack.Record, k)
	for i := range n {
		p := i * k / n
		if overlap {
			p = i % k
		}
		seq := uint64(i + 1) //nolint:gosec // a positive index
		parts[p] = append(parts[p], sized(seq, payload))
	}
	packs := make([]catalog.ReadPack, 0, k)
	for _, recs := range parts {
		packs = append(packs, w.pack(b, catalog.RoleSegment, catalog.PackStaged, false, recs...))
	}

	return packs
}

// sized is data(seq, 6, 11) with its frame grown to payload bytes, at least data's 14, by a body of pseudo-random bytes,
// which no block compresses away.
func sized(seq uint64, payload int) tracepack.Record {
	r := data(seq, 6, 11)
	if payload <= len(r.Payload) {
		return r
	}
	frame := make([]byte, payload)
	copy(frame, r.Payload)
	binary.BigEndian.PutUint32(frame, uint32(payload-4)) //nolint:gosec // a small frame
	rng := rand.New(rand.NewPCG(seq, 0))                 //nolint:gosec // synthetic bytes, not a secret
	for i := len(r.Payload); i < payload; i++ {
		frame[i] = byte(rng.Uint32())
	}
	r.Payload = frame

	return r
}

// counting is a sink that counts the versions and incompletes it receives.
type counting struct{ records, incomplete int }

func (c *counting) Versions(rs []Record) error { c.records += len(rs); return nil }

func (c *counting) Incomplete(Incomplete) error { c.incomplete++; return nil }
