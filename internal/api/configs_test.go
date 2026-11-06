package api

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/procconfig"
	"github.com/arloliu/tracekeep/internal/records"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// rebound is a catalog whose read sets of one hour name another configuration, or none, as no real capture's would:
// what a read set says is checked whatever the capture's configuration was.
type rebound struct {
	*configReads
	hour   int64
	config *int64
}

func (c *rebound) ReadSet(ctx context.Context, capture tracepack.UUID, h int64) (catalog.ReadSet, error) {
	rs, err := c.configReads.ReadSet(ctx, capture, h)
	if err == nil && h == c.hour {
		rs.ConfigID = c.config
	}

	return rs, err
}

// twoCaptures admits two captures sharing the masking configuration, each with a segment in hour and hour+1, seq 0 then 1,
// the first's first hour's object found gone at its first open, so its hour is read again; reads counts the configuration reads.
func twoCaptures(t *testing.T, c *catalog.Catalog) (*masked, *configReads, []tracepack.UUID) {
	t.Helper()
	m := newMasked(t, c)
	reads := &configReads{Catalog: m.srv.Catalog}
	m.srv.Catalog = reads
	caps := []tracepack.UUID{m.admitActive(t), m.admitActive(t)}
	gone, cs := map[string]bool{}, carriers(t)
	for i, capture := range caps {
		for h := range int64(2) {
			p := m.put(t, capture, hour+h, captured([]tracepack.Record{recordAt(&cs[0], hour+h, uint64(h))}), nil, m.policy()) //nolint:gosec // a small seq
			if i == 0 && h == 0 {
				gone[p.Key] = true
			}
		}
	}
	m.srv.Store = &flaky{ObjectStore: m.st, gone: gone}

	return m, reads, caps
}

// equipmentQuery is a QueryRecords request over hour and hour+1 of every capture of the equipment.
func equipmentQuery() *connect.Request[tkv1.QueryRecordsRequest] {
	return withToken(&tkv1.QueryRecordsRequest{Selector: &tkv1.Selector{EquipmentId: equipment, From: hourTS(hour), To: hourTS(hour + 2)}}, viewerToken)
}

// equipmentExport is an export request over hour and hour+1 of every capture of the equipment.
func equipmentExport() *exportRequest {
	return &exportRequest{Kind: "canonical", Selector: exportSelector{EquipmentID: equipment, From: hour * nsPerHour, To: (hour + 2) * nsPerHour}}
}

// A QueryRecords RPC reads a configuration its captures share once, across its hours, its captures and an hour read again;
// a new RPC reads it again.
func TestQueryRecordsDecodesAConfigurationOnce(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m, reads, _ := twoCaptures(t, c)
		msgs, err := m.streamed(t, equipmentQuery())
		if err != nil {
			t.Fatal(err)
		}
		if its, end := items(t, msgs); len(seqsOf(its)) != 4 || end.GetIncomplete() {
			t.Fatalf("the stream = %d records, end %v; want 4, complete", len(seqsOf(its)), end)
		}
		if n := reads.n.Load(); n != 1 {
			t.Errorf("one RPC read the configuration %d times, want once", n)
		}
		if _, err := m.streamed(t, equipmentQuery()); err != nil {
			t.Fatal(err)
		}
		if n := reads.n.Load(); n != 2 {
			t.Errorf("two RPCs read the configuration %d times, want twice", n)
		}
	})
}

// An export attempt reads a configuration its captures share once, across its hours, its captures and an hour exported again;
// another attempt reads it again.
func TestExportDecodesAConfigurationOnce(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m, reads, _ := twoCaptures(t, c)
		if _, _, err := m.srv.buildBundle(t.Context(), newID(t), equipmentExport(), t.TempDir()); err != nil {
			t.Fatal(err)
		}
		if n := reads.n.Load(); n != 1 {
			t.Errorf("one attempt read the configuration %d times, want once", n)
		}
		if _, _, err := m.srv.buildBundle(t.Context(), newID(t), equipmentExport(), t.TempDir()); err != nil {
			t.Fatal(err)
		}
		if n := reads.n.Load(); n != 2 {
			t.Errorf("two attempts read the configuration %d times, want twice", n)
		}
	})
}

// A later read set naming no configuration, or another without a policy, is still checked and withheld,
// by QueryRecords and by an export, after the capture's configuration was decided.
func TestALaterReadSetIsStillChecked(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		content := procconfig.Content{Classifier: "test", MaxFrameLen: 1 << 20}
		other, err := c.StageConfig(t.Context(), catalog.Staging{Content: content.Encode()})
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name   string
			config *int64
			reads  int64
		}{{"none", nil, 1}, {"another", &other, 2}} {
			m, reads, caps := twoCaptures(t, c)
			m.srv.Catalog = &rebound{configReads: reads, hour: hour + 1, config: tc.config}
			_, err := m.streamed(t, query(caps[0], func(q *tkv1.QueryRecordsRequest) { q.Selector.To = hourTS(hour + 2) }))
			if connect.CodeOf(err) != connect.CodeFailedPrecondition {
				t.Errorf("%s: QueryRecords = %v, want failed_precondition", tc.name, err)
			}
			if n := reads.n.Load(); n != tc.reads {
				t.Errorf("%s: QueryRecords read configurations %d times, want %d", tc.name, n, tc.reads)
			}
			if _, _, err := m.srv.buildBundle(t.Context(), newID(t), equipmentExport(), t.TempDir()); !errors.Is(err, records.ErrWithheld) {
				t.Errorf("%s: an export = %v, want withheld", tc.name, err)
			}
		}
	})
}

// contents is a catalog answering configuration reads from answer, counting them.
type contents struct {
	Catalog
	n      int
	answer func(ctx context.Context, n int) ([]byte, error)
}

func (c *contents) ConfigContent(ctx context.Context, _ int64) ([]byte, error) {
	c.n++

	return c.answer(ctx, c.n)
}

// A decode that failed or was cancelled is not remembered; a remembered answer is not given to a cancelled read;
// no configuration is read for a missing id, which withholds every time.
func TestConfigsRemembersOnlyDecisions(t *testing.T) {
	t.Parallel()
	unmasked := procconfig.Content{Classifier: "test", MaxFrameLen: 1 << 20}
	id := int64(7)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	cat := &contents{answer: func(ctx context.Context, n int) ([]byte, error) {
		switch {
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case n == 1:
			return []byte("not a configuration"), nil
		default:
			return unmasked.Encode(), nil
		}
	}}
	cs := configs{s: &Server{Catalog: cat}}
	if _, err := cs.withholds(t.Context(), &id); connect.CodeOf(err) != connect.CodeInternal {
		t.Errorf("a configuration that does not decode = %v, want internal", err)
	}
	if _, err := cs.withholds(cancelled, &id); err == nil {
		t.Error("a cancelled read decided the configuration")
	}
	if v, err := cs.withholds(t.Context(), &id); err != nil || !v || cat.n != 3 {
		t.Errorf("after a failed and a cancelled read = %v, %v after %d reads; want withheld after 3", v, err, cat.n)
	}
	if v, err := cs.withholds(t.Context(), &id); err != nil || !v || cat.n != 3 {
		t.Errorf("remembered = %v, %v after %d reads; want withheld, no read", v, err, cat.n)
	}
	if _, err := cs.withholds(cancelled, &id); !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled read of a remembered configuration = %v, want canceled", err)
	}
	for range 2 {
		if v, err := cs.withholds(t.Context(), nil); err != nil || !v {
			t.Errorf("no configuration = %v, %v; want withheld", v, err)
		}
	}
	if zero := int64(0); cat.n != 3 {
		t.Errorf("a missing configuration read the catalog: %d reads", cat.n)
	} else if v, err := cs.withholds(t.Context(), &zero); err != nil || !v || cat.n != 3 {
		t.Errorf("configuration 0 = %v, %v after %d reads; want withheld, no read", v, err, cat.n)
	}
}
