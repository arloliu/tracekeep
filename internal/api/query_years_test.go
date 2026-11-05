package api

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// countedReads is a catalog counting the read sets and next-hour lookups asked of it.
type countedReads struct {
	Catalog
	reads, nexts atomic.Int32
}

func (c *countedReads) ReadSet(ctx context.Context, capture tracepack.UUID, h int64) (catalog.ReadSet, error) {
	c.reads.Add(1)

	return c.Catalog.ReadSet(ctx, capture, h)
}

func (c *countedReads) NextReadHour(ctx context.Context, capture tracepack.UUID, from, to int64) (int64, bool, error) {
	c.nexts.Add(1)

	return c.Catalog.NextReadHour(ctx, capture, from, to)
}

// QueryRecords over a window of years steps only through the hours that hold packs, on each engine:
// two hours years apart cost two read sets and three next-hour lookups, however wide the window.
func TestQueryRecordsOverYearsReadsOnlyItsHours(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		const years = int64(5 * 365 * 24)
		w := newMasked(t, c)
		w.srv.Dev = true // a capture without a masking configuration is read under dev alone
		capture := w.admitUnmasked(t)
		cs := carriers(t)
		far := hour + years/2
		w.put(t, capture, hour, captured([]tracepack.Record{recordAt(&cs[0], hour, 1)}), nil, nil)
		w.put(t, capture, far, captured([]tracepack.Record{recordAt(&cs[0], far, 2)}), nil, nil)
		counted := &countedReads{Catalog: w.srv.Catalog}
		w.srv.Catalog = counted
		msgs, err := w.streamed(t, query(capture, func(q *tkv1.QueryRecordsRequest) {
			q.Selector.From, q.Selector.To = hourTS(hour-years), hourTS(hour+years)
		}))
		if err != nil {
			t.Fatal(err)
		}
		its, _ := items(t, msgs)
		if got := seqsOf(its); !slices.Equal(got, []uint64{1, 2}) {
			t.Errorf("records %v, want 1 and 2 from their hours years apart", got)
		}
		if r, n := counted.reads.Load(), counted.nexts.Load(); r > 2 || n > 3 {
			t.Errorf("%d read sets and %d next-hour lookups, want at most 2 and 3", r, n)
		}
	})
}
