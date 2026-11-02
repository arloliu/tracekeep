package api

import (
	"context"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// A deadline is the dependency's whose call it ended: the catalog's own deadline at QueryRecords' traversal and at FindTransaction's read set,
// the request live, answers the catalog's fixed message, and the store's own deadline at a pack's read answers the store's.
func TestDependencyDeadlineKeepsItsSource(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		capture := m.admitUnmasked(t)
		p := m.put(t, capture, hour, []tracepack.Record{message(1, hour*nsPerHour+1, 1, true, true, 7)}, nil, nil)
		catalogDeadline := fmt.Errorf("catalog: %s: %w", catalogSentinel, context.DeadlineExceeded)
		queried := func() error { _, err := m.streamed(t, query(capture, nil)); return err }
		found := func() error { _, err := find(m, t, capture, 1, viewerToken); return err }
		if err := queried(); err != nil {
			t.Fatalf("the control = %v", err)
		}
		for _, tc := range []struct {
			name, op string
			call     func() error
		}{
			{"QueryRecords' next hour", "NextReadHour", queried},
			{"QueryRecords' read set", "ReadSet", queried},
			{"FindTransaction's read set", "ReadSet", found},
		} {
			m.srv.Catalog = failing(c, tc.op, catalogDeadline)
			if err := tc.call(); connect.CodeOf(err) != connect.CodeUnavailable || errMessage(err) != msgCatalog {
				t.Errorf("%s past the catalog's deadline = %v, want unavailable with the catalog's fixed message", tc.name, err)
			}
		}
		m.srv.Catalog = c
		m.srv.Store = &failingStore{ObjectStore: m.st, err: fmt.Errorf("read %s: %w", p.Key, context.DeadlineExceeded)}
		for name, call := range map[string]func() error{"QueryRecords": queried, "FindTransaction": found} {
			if err := call(); connect.CodeOf(err) != connect.CodeUnavailable || errMessage(err) != msgStore {
				t.Errorf("%s past the store's deadline = %v, want unavailable with the store's fixed message", name, err)
			}
		}
	})
}
