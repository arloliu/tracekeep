package api

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
)

// scheduled is a masked world holding capture's seq 1 in two equal segments of hour, and a compactor over it under the lease.
type scheduled struct {
	*masked
	capture tracepack.UUID
	record  tracepack.Record
	segs    []*catalog.Pack
	r       *compact.Compactor
	lease   catalog.Lease
	at      time.Time
}

func newScheduled(t *testing.T, c *catalog.Catalog) *scheduled {
	t.Helper()
	m := newMasked(t, c)
	s := &scheduled{masked: m, capture: m.admitActive(t)}
	rs := recordsOf(carriers(t)[:1], hour)
	s.record = rs[0]
	ms, entries := m.mask(t, s.capture, rs)
	for range 2 {
		s.segs = append(s.segs, m.put(t, s.capture, hour, ms, entries, m.policy()))
	}
	lease, ok, err := c.TakeLease(t.Context(), "compactor/2", "test", time.Hour)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	s.lease, s.at = lease, time.Unix(0, (hour+1)*nsPerHour).Add(time.Hour)
	s.r = &compact.Compactor{
		Catalog: c, Store: m.st, HorizonHour: func() int64 { return hour - 24 }, Log: slog.New(slog.DiscardHandler),
		WorkDir: t.TempDir(), Writer: "tracekeep-test", QuietPeriod: 15 * time.Minute, DeleteDelay: time.Minute,
		Now: func() time.Time { return s.at },
	}

	return s
}

// during runs change once, while a restore is held after its copies read.
func (s *scheduled) during(t *testing.T, change func()) {
	t.Helper()
	s.srv.stall = func(stage string) {
		if stage == "copies" {
			s.srv.stall = nil
			change()
		}
	}
}

// merge clears the equal segments' suspect conflict and merges the scope.
// It may run in a held handler, so it reports with Errorf, never Fatal.
func (s *scheduled) merge(t *testing.T) {
	t.Helper()
	if rep, err := s.r.Conflicts(t.Context(), s.lease); err != nil || rep.Confirmed != 0 {
		t.Errorf("Conflicts = %+v, %v; want the equal copies cleared", rep, err)
	}
	if rep, err := s.r.Merges(t.Context(), s.lease); err != nil || len(rep.Merged) != 1 {
		t.Errorf("Merges = %+v, %v; want the scope merged", rep, err)
	}
}

func (s *scheduled) restored(t *testing.T, name string) {
	t.Helper()
	got, err := s.restore(t, s.capture, 1, hour)
	if err != nil || !bytes.Equal(got.GetPayload(), s.record.Payload) {
		t.Errorf("%s: Restore = %v, %v; want the record as received", name, got, err)
	}
}

// A pack registered while a restore is held after its copies read is not among them: the answer holds as of the read,
// and the next restore sees the new, differing version.
func TestRestoreHeldAcrossARegistration(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		s := newScheduled(t, c)
		later := recordsOf(carriers(t)[:1], hour+1)
		ms, entries := s.mask(t, s.capture, later)
		s.during(t, func() { s.put(t, s.capture, hour+1, ms, entries, s.policy()) })
		s.restored(t, "held across a registration")
		if _, err := s.restore(t, s.capture, 1, hour); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("the next restore = %v, want the conflict", err)
		}
	})
}

// A merge committed while a restore is held leaves its inputs readable for D: the restore reads them and answers.
func TestRestoreHeldAcrossAMerge(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		s := newScheduled(t, c)
		s.during(t, func() { s.merge(t) })
		s.restored(t, "held across a merge")
		s.restored(t, "after the merge, inputs and archive")
	})
}

// An input deleted while a restore is held refuses it as unavailable, never answering from fewer copies than it read;
// the retry reads afresh, without the deleted input, and answers.
func TestRestoreHeldAcrossADeletion(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		s := newScheduled(t, c)
		s.merge(t)
		s.during(t, func() {
			// The commit fixes delete_at by the catalog's own clock.
			s.at = time.Now().Add(time.Hour)
			rep := compact.RetentionReport{}
			if err := s.r.Deletions(t.Context(), s.lease, &rep); err != nil || rep.Deleted != 2 {
				t.Errorf("Deletions = %+v, %v; want both inputs deleted", rep, err)
			}
		})
		if _, err := s.restore(t, s.capture, 1, hour); connect.CodeOf(err) != connect.CodeUnavailable {
			t.Errorf("held across a deletion = %v, want unavailable", err)
		}
		s.restored(t, "the retry")
	})
}

// A copy excluded while a restore is held is still one it read: a differing version excluded at the hold
// refuses the held restore as a conflict, and the next restore, reading afresh without it, answers.
func TestRestoreHeldAcrossAnExclusion(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		s := newScheduled(t, c)
		later := recordsOf(carriers(t)[:1], hour+1)
		ms, entries := s.mask(t, s.capture, later)
		other := s.put(t, s.capture, hour+1, ms, entries, s.policy())
		s.during(t, func() {
			if _, err := c.ExcludeInput(t.Context(), other.ID, "operator", "x1"); err != nil {
				t.Error(err)
			}
		})
		if _, err := s.restore(t, s.capture, 1, hour); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("held across the differing copy's exclusion = %v, want the conflict it read", err)
		}
		s.restored(t, "after the exclusion")
	})
}
