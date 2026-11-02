package api

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// A copy whose open or read fails answers unavailable with the store's fixed message,
// and neither the answer nor the log names the object's key, a signed URL or the store's text: Restore opens its copies through the records reader.
func TestRestoreStoreFailuresKeyless(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	m := newMasked(t, c)
	capture := m.admitActive(t)
	rs := recordsOf(carriers(t)[:1], hour)
	ms, entries := m.mask(t, capture, rs)
	p := m.put(t, capture, hour, ms, entries, m.policy())
	sentinel := errors.New("get https://store.invalid/" + p.Key + "?X-Amz-Signature=sig-sentinel: backend-sentinel")
	for name, open := range map[string]bool{"an open": true, "a read": false} {
		m.logs.Reset()
		m.srv.Store = &failingStore{ObjectStore: m.st, open: open, err: sentinel}
		_, err := m.restore(t, capture, 1, hour)
		if connect.CodeOf(err) != connect.CodeUnavailable || errMessage(err) != msgStore {
			t.Errorf("%s failing = %v, want unavailable with the store's fixed message", name, err)
		}
		if text := err.Error() + m.logs.String(); strings.Contains(text, p.Key) || strings.Contains(text, "sentinel") {
			t.Errorf("%s failing: the answer or the log names the key or the store's text: %v\n%s", name, err, m.logs)
		}
		if !strings.Contains(m.logs.String(), "operation=Restore") {
			t.Errorf("%s failing: the log = %q, want the failure", name, m.logs)
		}
	}
}

// A restore whose comparison passes the merge's held bytes, before its first copy's block or after it, or one of whose copies
// a reader's budget refuses, answers resource_exhausted: no payload, no audit row, every object closed and its read slot released;
// a corrupt copy stays unavailable.
func TestRestoreReadLimits(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		rs := recordsOf(carriers(t)[:1], hour)
		ms, entries := m.mask(t, capture, rs)
		m.put(t, capture, hour, ms, entries, m.policy())
		m.put(t, capture, hour, ms, entries, m.policy())
		st := &counted{ObjectStore: m.st}
		m.srv.Store, m.srv.Records.MaxReads = st, 1 // one slot: a restore that kept it would refuse the next
		need := heldNeeded(t, m, capture)
		for _, tc := range []struct {
			name   string
			held   int64
			reader tracepack.ReaderOptions
		}{
			{"held bytes of one byte", 1, tracepack.ReaderOptions{}},
			{"held bytes one short of both copies' blocks", need - 1, tracepack.ReaderOptions{}},
			{"a reader's budget below the pack metadata", 0, tracepack.ReaderOptions{MaxPackMetadataLen: 1}},
		} {
			m.srv.Records.HeldBytes, m.srv.restoreReader = tc.held, tc.reader
			got, err := m.restore(t, capture, 1, hour)
			if connect.CodeOf(err) != connect.CodeResourceExhausted || got != nil {
				t.Errorf("%s = %v, %v; want resource_exhausted and no payload", tc.name, got, err)
			}
			if o, cl := st.opened.Load(), st.closed.Load(); o != cl {
				t.Errorf("%s: %d objects opened, %d closed", tc.name, o, cl)
			}
		}
		if d := auditDetails(t, c); len(d) != 0 {
			t.Errorf("refused restores wrote audit rows %v", d)
		}
		m.srv.Records.HeldBytes, m.srv.restoreReader = 0, tracepack.ReaderOptions{}
		if got, err := m.restore(t, capture, 1, hour); err != nil || !bytes.Equal(got.GetPayload(), rs[0].Payload) {
			t.Errorf("the restore after the refusals = %v, %v; want the original, the read slot free", got, err)
		}
		// A corrupt copy: one byte of its block body flipped, its CRC left as it was.
		m.patch = func(b []byte) []byte {
			b = slices.Clone(b)
			r, err := tracepack.Open(t.Context(), bytes.NewReader(b), int64(len(b)), tracepack.ReaderOptions{})
			if err != nil {
				t.Fatal(err)
			}
			b[r.Blocks()[0].Offset+40] ^= 1

			return b
		}
		m.put(t, capture, hour, ms, entries, m.policy())
		if _, err := m.restore(t, capture, 1, hour); connect.CodeOf(err) != connect.CodeUnavailable {
			t.Errorf("a corrupt copy = %v, want unavailable", err)
		}
	})
}

// heldNeeded is the least held bytes the comparison of capture's seq 1 in hour passes with, found by bisection.
func heldNeeded(t *testing.T, m *masked, capture tracepack.UUID) int64 {
	t.Helper()
	copies, _, err := m.c.RecordCopies(t.Context(), capture, 1, hour, 16)
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := int64(1), int64(64<<20) // lo fails, hi passes
	for lo+1 < hi {
		mid := (lo + hi) / 2
		m.srv.Records.HeldBytes = mid
		if _, err := m.srv.version(t.Context(), capture, 1, copies); err == nil {
			hi = mid
		} else {
			lo = mid
		}
	}

	return hi
}
