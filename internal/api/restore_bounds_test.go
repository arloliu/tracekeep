package api

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// heldReads is a catalog whose ListCaptures and Capture, once gated, signal arrived and wait for release:
// a QueryRecords and a FindTransaction held there keep their read slots. It counts the copy listings.
type heldReads struct {
	Catalog
	gated   atomic.Bool
	arrived chan struct{}
	release chan struct{}
	listed  atomic.Int32
}

func (h *heldReads) hold() {
	if h.gated.Load() {
		h.arrived <- struct{}{}
		<-h.release
	}
}

func (h *heldReads) ListCaptures(ctx context.Context, sel catalog.Selector, q time.Duration) ([]catalog.CaptureInfo, error) {
	h.hold()

	return h.Catalog.ListCaptures(ctx, sel, q)
}

func (h *heldReads) Capture(ctx context.Context, id tracepack.UUID) (catalog.Capture, error) {
	h.hold()

	return h.Catalog.Capture(ctx, id)
}

func (h *heldReads) RecordCopies(ctx context.Context, capture tracepack.UUID, seq uint64, hour int64, limit int) ([]catalog.Copy, bool, error) {
	h.listed.Add(1)

	return h.Catalog.RecordCopies(ctx, capture, seq, hour, limit)
}

// A restore takes one of the replica's read slots before it lists any copy: with every slot held by a QueryRecords stream
// and a FindTransaction lookup, it is refused as resource_exhausted, lists nothing and writes no audit row; once they end, it answers.
func TestRestoreTakesAReadSlot(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		rs := recordsOf(carriers(t)[:1], hour)
		ms, entries := m.mask(t, capture, rs)
		m.put(t, capture, hour, ms, entries, m.policy())
		h := &heldReads{Catalog: c, arrived: make(chan struct{}, 2), release: make(chan struct{})}
		m.srv.Catalog, m.srv.Records.MaxReads = h, 2
		h.gated.Store(true)
		done := make(chan error, 2)
		go func() {
			_, err := m.streamed(t, query(capture, nil))
			done <- err
		}()
		go func() {
			_, err := m.cl.FindTransaction(context.Background(), withToken(&tkv1.FindTransactionRequest{CaptureId: capture.String(), Seq: 1, Hour: hourTS(hour)}, viewerToken))
			done <- err
		}()
		for range 2 {
			<-h.arrived
		}
		if _, err := m.restore(t, capture, 1, hour); connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Errorf("a restore with every read slot held = %v, want resource_exhausted", err)
		}
		if n := h.listed.Load(); n != 0 {
			t.Errorf("the refused restore listed copies %d times", n)
		}
		h.gated.Store(false)
		close(h.release)
		for range 2 {
			if err := <-done; connect.CodeOf(err) == connect.CodeResourceExhausted {
				t.Errorf("a held read was refused its slot: %v", err)
			}
		}
		if details := auditDetails(t, c); len(details) != 0 {
			t.Errorf("audit rows %v before any restore answered", details)
		}
		if got, err := m.restore(t, capture, 1, hour); err != nil || !bytes.Equal(got.GetPayload(), rs[0].Payload) {
			t.Errorf("the restore once the reads ended = %v, %v", got, err)
		}
	})
}

// A restore compares at most api.records.max_readers copies: a record with more, merged inputs, their archive and another hour's,
// is refused as resource_exhausted before any object opens, with no payload, no audit row and its read slot freed,
// even when the copy past the bound differs; the named hour's not_found is answered first as before.
func TestRestoreBoundsItsCopies(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		s := newScheduled(t, c)
		s.merge(t) // two merged inputs and their archive, three copies in the hour
		st := &counted{ObjectStore: s.st}
		s.srv.Store, s.srv.Records.MaxReads = st, 1
		exhausted := func(name string) {
			t.Helper()
			opened := st.opened.Load()
			if got, err := s.restore(t, s.capture, 1, hour); connect.CodeOf(err) != connect.CodeResourceExhausted || got != nil {
				t.Errorf("%s = %v, %v; want resource_exhausted and no payload", name, got, err)
			}
			if n := st.opened.Load() - opened; n != 0 {
				t.Errorf("%s opened %d objects", name, n)
			}
			if _, err := s.restore(t, s.capture, 1, hour+5); connect.CodeOf(err) != connect.CodeNotFound {
				t.Errorf("%s, naming an hour without a copy = %v, want not_found", name, err)
			}
		}
		s.srv.Records.MaxReaders = 2
		exhausted("three copies, at most two compared")
		s.srv.Records.MaxReaders = 3
		s.restored(t, "three copies, at most three compared")
		later := recordsOf(carriers(t)[:1], hour+1)
		ms, entries := s.mask(t, s.capture, later)
		s.put(t, s.capture, hour+1, ms, entries, s.policy())
		exhausted("a differing copy in another hour past the bound")
		s.srv.Records.MaxReaders = 4
		if _, err := s.restore(t, s.capture, 1, hour); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("four copies, at most four compared = %v, want the conflict", err)
		}
		if details := auditDetails(t, c); len(details) != 1 {
			t.Errorf("audit rows %v, want only the restore answered", details)
		}
		if o, cl := st.opened.Load(), st.closed.Load(); o != cl {
			t.Errorf("%d objects opened, %d closed", o, cl)
		}
	})
}

// Many equal copies, each with heavy metadata, restore within the merge's held bytes, and every object closes.
func TestRestoreManyEqualCopies(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		rs := recordsOf(carriers(t)[:1], hour)
		ms, entries := m.mask(t, capture, rs)
		const copies = 6
		for range copies {
			m.extensions = []tracepack.RawEntry{{Tag: 0x8001, Type: 7, Value: bytes.Repeat([]byte{0x5a}, 256<<10)}} // a private tag of type bytes
			m.put(t, capture, hour, ms, entries, m.policy())
		}
		st := &counted{ObjectStore: m.st}
		m.srv.Store, m.srv.Records.MaxReaders, m.srv.Records.HeldBytes = st, copies, 1<<20
		if got, err := m.restore(t, capture, 1, hour); err != nil || !bytes.Equal(got.GetPayload(), rs[0].Payload) {
			t.Errorf("Restore of %d equal copies = %v, %v", copies, got, err)
		}
		if o, cl := st.opened.Load(), st.closed.Load(); o != copies || cl != copies {
			t.Errorf("%d objects opened, %d closed; want %d each", o, cl, copies)
		}
	})
}
