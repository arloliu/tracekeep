package compact

import (
	"bytes"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
)

// metaLen is the encoded length of the pack metadata of b.
func metaLen(t *testing.T, b built) int64 {
	t.Helper()
	r, err := tracepack.Open(t.Context(), bytes.NewReader(b.data), int64(len(b.data)), tracepack.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := r.Header().Meta.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	return int64(len(raw))
}

// bodiedAt gives the record at seq a two-byte message body, as withBody does maskedSeq's.
func bodiedAt(recs []tracepack.Record, seq uint64) []tracepack.Record {
	for i := range recs {
		if recs[i].Seq == seq {
			p := append(append([]byte(nil), recs[i].Payload...), 0xaa, 0xbb)
			p[3] += 2
			recs[i].Payload = p
		}
	}

	return recs
}

// redactedAt is redacted(2) with its entry at seq.
func redactedAt(seq uint64) func(*tracepack.PackMeta) {
	return func(m *tracepack.PackMeta) {
		redacted(2)(m)
		m.Redaction[0].Seq = seq
	}
}

// overBudget stages two masked segments of one scope, each within a reader budget their archive would exceed, and sets the compactor's budget there.
func (f *fixture) overBudget(t *testing.T, capture tracepack.UUID) catalog.Scope {
	t.Helper()
	a := segmentOf(t, capture, hour, bodiedAt(recordsAt(hour, 0, 1, 2), 1), redactedAt(1))
	b := segmentOf(t, capture, hour, withBody(recordsAt(hour, 3, 4, 5)), redactedAt(4))
	f.stage(t, a)
	f.stage(t, b)
	f.r.Reader.MaxPackMetadataLen = max(metaLen(t, a), metaLen(t, b))

	return catalog.Scope{Capture: capture, Hour: hour}
}

// A merge whose archive's metadata would exceed the reader's budget marks its scope unmergeable once:
// no archive is put, the next pass does not try it again, and the refusal is counted apart from a failure,
// so the merge task still succeeds and the scope counts in no unmerged age (compactor.md §8.1).
func TestMergeOverBudgetLeavesTheScopeUnmergeable(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		s := metrics.NewServer()
		var err error
		if f.r.Metrics, err = metrics.NewCompact(s); err != nil {
			t.Fatal(err)
		}
		scope := f.overBudget(t, f.admit(t))
		if err := f.r.mergePass(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		snap, err := c.MergeSnapshot(t.Context(), scope)
		if err != nil || snap.State != "unmergeable" {
			t.Fatalf("scope = %+v, %v; want unmergeable", snap, err)
		}
		if keys := f.archives(t); len(keys) != 0 {
			t.Errorf("archives = %v, want none put", keys)
		}
		got := scrape(t, s)
		for _, want := range []string{
			`tracekeep_compact_merges_total{outcome="refused"} 1`, `tracekeep_compact_last_success_timestamp{task="merge"}`,
			`tracekeep_compact_scopes{state="unmergeable"} 1`, "tracekeep_compact_oldest_unmerged_scope_age_seconds 0",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("/metrics lacks %s", want)
			}
		}
		if rep := f.merges(t); len(rep.Refused)+len(rep.Failed)+len(rep.Merged) != 0 {
			t.Errorf("a second pass = %+v, want the unmergeable scope left alone", rep)
		}
	})
}

// An input over the reader's budget refuses the merge the same way.
func TestMergeOfAnInputOverBudget(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		scope := f.overBudget(t, f.admit(t))
		f.r.Reader.MaxPackMetadataLen = 64
		if rep := f.merges(t); !slices.Equal(rep.Refused, []catalog.Scope{scope}) {
			t.Fatalf("Merges = %+v, want the scope refused", rep)
		}
	})
}

// An operator's request on an unmergeable scope is refused again and ends failed with the reason while the budget holds,
// and merges and settles the scope once it does not.
func TestMergeRequestOnAnUnmergeableScope(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		capture := f.admit(t)
		scope := f.overBudget(t, capture)
		if rep := f.merges(t); !slices.Equal(rep.Refused, []catalog.Scope{scope}) {
			t.Fatalf("Merges = %+v, want the scope refused", rep)
		}
		refused := f.request(t, catalog.RequestMerge, capture)
		if _, err := f.r.Requests(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		if state, outcome := f.status(t, refused); state != "failed" || outcome != overBudgetReason {
			t.Errorf("request = %s, %q; want failed, %q", state, outcome, overBudgetReason)
		}
		f.r.Reader = tracepack.ReaderOptions{}
		merge := f.request(t, catalog.RequestMerge, capture)
		f.r.Now = func() time.Time { return time.Unix(0, (hour+3)*nsPerHour) }
		if _, err := f.r.Requests(t.Context(), f.lease); err != nil {
			t.Fatal(err)
		}
		if state, outcome := f.status(t, merge); state != "done" || outcome != "merged" {
			t.Errorf("request = %s, %q; want done, merged", state, outcome)
		}
		if snap, err := c.MergeSnapshot(t.Context(), scope); err != nil || snap.State != "settled" {
			t.Errorf("scope = %+v, %v; want settled", snap, err)
		}
	})
}
