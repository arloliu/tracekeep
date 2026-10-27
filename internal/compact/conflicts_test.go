package compact

import (
	"slices"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
)

// recordsAt are data records of seqs in hour hr.
func recordsAt(hr int64, seqs ...uint64) []tracepack.Record {
	recs := make([]tracepack.Record, len(seqs))
	for i, seq := range seqs {
		recs[i] = data(seq)
		recs[i].TSUTCNs = hr*nsPerHour + int64(seq) //nolint:gosec // small test seqs
	}

	return recs
}

// segmentOf writes recs as a segment of capture's hour hr, its metadata adjusted by adjust.
func segmentOf(t *testing.T, capture tracepack.UUID, hr int64, recs []tracepack.Record, adjust func(*tracepack.PackMeta)) built {
	t.Helper()
	b := writeRecords(t, capture, tracepack.PackRoleSegment, hr, recs, adjust)
	key, err := tracepack.SegmentKey("", tool, capture, recs[0].Seq, b.id)
	if err != nil {
		t.Fatal(err)
	}
	b.key = key

	return b
}

func noAdjust(*tracepack.PackMeta) {}

func (f *fixture) conflicts(t *testing.T) ConflictReport {
	t.Helper()
	rep, err := f.r.Conflicts(t.Context(), f.lease)
	if err != nil {
		t.Fatal(err)
	}

	return rep
}

// Equal copies of a seq run in two segments, a redelivery's, clear their suspect, and the merge keeps each record once.
func TestEqualCopiesClearTheSuspect(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		capture := f.admit(t)
		f.stage(t, segmentOf(t, capture, hour, recordsAt(hour, 0, 1, 2, 3, 4), noAdjust))
		f.stage(t, segmentOf(t, capture, hour, recordsAt(hour, 3, 4, 5, 6, 7), noAdjust))
		if got := f.candidates(t); len(got) != 0 {
			t.Fatalf("candidates with a suspect = %v, want none", got)
		}
		if rep := f.conflicts(t); rep.Cleared != 1 || rep.Confirmed+rep.Changed+rep.Failed != 0 {
			t.Fatalf("Conflicts = %+v, want one cleared", rep)
		}
		scope := catalog.Scope{Capture: capture, Hour: hour}
		if rep := f.merges(t); !slices.Equal(rep.Merged, []catalog.Scope{scope}) {
			t.Fatalf("Merges = %+v, want the scope merged", rep)
		}
		snap, err := c.MergeSnapshot(t.Context(), scope)
		if err != nil || len(snap.Inputs) != 1 {
			t.Fatalf("snapshot = %+v, %v", snap, err)
		}
		p, _, err := f.r.read(t.Context(), store.ObjectInfo{Key: snap.Inputs[0].Key, Size: snap.Inputs[0].Size})
		if err != nil || !slices.Equal(p.Ranges, []catalog.Range{{SeqFrom: 0, SeqTo: 7, TSFrom: hour * nsPerHour, TSTo: hour*nsPerHour + 7}}) {
			t.Errorf("the archive's pack = %+v, %v; want seqs 0-7 once", p, err)
		}
	})
}

// Differing copies confirm their suspect, which keeps the scope unmerged:
// a record whose bytes differ, in one hour and across two, and equal records whose redaction entries differ.
func TestDifferingCopiesConfirmTheSuspect(t *testing.T) {
	t.Parallel()
	changed := recordsAt(hour, 3, 4, 5)
	changed[1].Payload = append([]byte(nil), changed[1].Payload...)
	changed[1].Payload[len(changed[1].Payload)-1] ^= 0xff
	cases := map[string]struct {
		a, b func(t *testing.T, capture tracepack.UUID) built
	}{
		"differing bytes": {
			a: func(t *testing.T, capture tracepack.UUID) built {
				t.Helper()
				return segmentOf(t, capture, hour, recordsAt(hour, 0, 1, 2, 3, 4), noAdjust)
			},
			b: func(t *testing.T, capture tracepack.UUID) built {
				t.Helper()
				return segmentOf(t, capture, hour, changed, noAdjust)
			},
		},
		"another hour": {
			a: func(t *testing.T, capture tracepack.UUID) built {
				t.Helper()
				return segmentOf(t, capture, hour, recordsAt(hour, 0, 1, 2, 3, 4), noAdjust)
			},
			b: func(t *testing.T, capture tracepack.UUID) built {
				t.Helper()
				return segmentOf(t, capture, hour+1, recordsAt(hour+1, 4, 5), noAdjust)
			},
		},
		"differing redaction entries": {
			a: func(t *testing.T, capture tracepack.UUID) built {
				t.Helper()
				return segmentOf(t, capture, hour, withBody(recordsAt(hour, 0, 1, 2, 3, 4), 4), redacted(4, 1))
			},
			b: func(t *testing.T, capture tracepack.UUID) built {
				t.Helper()
				return segmentOf(t, capture, hour, withBody(recordsAt(hour, 3, 4, 5), 4), redacted(4, 2))
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				f := newFixture(t, c)
				f.merging(t)
				capture := f.admit(t)
				f.stage(t, tc.a(t, capture))
				f.stage(t, tc.b(t, capture))
				if rep := f.conflicts(t); rep.Confirmed != 1 || rep.Cleared+rep.Changed+rep.Failed != 0 {
					t.Fatalf("Conflicts = %+v, want one confirmed", rep)
				}
				if ss, err := c.Suspects(t.Context()); err != nil || len(ss) != 0 {
					t.Errorf("suspects = %+v, %v; want none left", ss, err)
				}
				if got := f.candidates(t); len(got) != 0 {
					t.Errorf("candidates = %v, want the confirmed conflict to block the merge", got)
				}
			})
		})
	}
}

// withBody gives the record of seq a two-byte message body after its frame prefix, which a mask may cover.
func withBody(recs []tracepack.Record, seq uint64) []tracepack.Record {
	for i := range recs {
		if recs[i].Seq == seq {
			p := append(append([]byte(nil), recs[i].Payload...), 0xaa, 0xbb)
			p[3] += 2
			recs[i].Payload = p
		}
	}

	return recs
}

// redacted adds a redaction policy and one entry for seq, masking the first n bytes of its message body.
func redacted(seq, n uint64) func(*tracepack.PackMeta) {
	return func(m *tracepack.PackMeta) {
		alg, key := tracepack.EncryptionAlgorithmAESSIVCMAC512, "k"
		m.RedactionPolicy = &tracepack.RedactionPolicy{
			PolicyID: "p", PolicyVersion: 1, KeyID: "k", DigestAlgorithm: tracepack.DigestAlgorithmHMACSHA256,
			EncryptionAlgorithm: &alg, EncryptionKeyID: &key,
		}
		m.Classifiers = []string{"c"}
		m.Redaction = []tracepack.RedactionEntry{{
			Seq: seq, MaskedRanges: []tracepack.MaskedRange{{Offset: 14, Length: n}}, Domain: "d", Digest: make([]byte, 32),
			SIV: make([]byte, 16), HasSIV: true,
		}}
	}
}

// A merge that fails on records the catalog never saw overlap writes them confirmed, so the scope waits for an operator.
func TestMergeFailingOnAConflictRecordsIt(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		capture := f.admit(t)
		f.stage(t, segmentOf(t, capture, hour, recordsAt(hour, 0, 1, 2, 3, 4), noAdjust))
		// The second segment holds seq 4 too, differing, but its registration names 5-6 only: no suspect is raised.
		changed := recordsAt(hour, 4, 5, 6)
		changed[0].Payload = append([]byte(nil), changed[0].Payload...)
		changed[0].Payload[0] ^= 0xff
		b := segmentOf(t, capture, hour, changed, noAdjust)
		f.put(t, b.key, b.data)
		p, reason, err := f.r.read(t.Context(), store.ObjectInfo{Key: b.key, Size: int64(len(b.data))})
		if err != nil || reason != "" {
			t.Fatalf("read = %q, %v", reason, err)
		}
		p.Ranges = p.Ranges[0:1]
		p.Ranges[0].SeqFrom = 5
		if out, err := c.RegisterPack(t.Context(), p, catalog.Registration{Evidence: &catalog.Evidence{}, HorizonHour: f.horizon}); err != nil || out != catalog.Registered {
			t.Fatalf("RegisterPack = %v, %v", out, err)
		}
		scope := catalog.Scope{Capture: capture, Hour: hour}
		if rep := f.merges(t); !slices.Equal(rep.Conflicted, []catalog.Scope{scope}) {
			t.Fatalf("Merges = %+v, want the scope conflicted", rep)
		}
		if got := f.candidates(t); len(got) != 0 {
			t.Errorf("candidates = %v, want the recorded conflict to block the scope", got)
		}
		if keys := f.archives(t); len(keys) != 0 {
			t.Errorf("archives = %v, want none put", keys)
		}
	})
}
