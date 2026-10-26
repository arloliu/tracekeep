package metrics

import (
	"strings"
	"testing"
	"time"
)

// lease_held follows the lease's lapse: a lease not renewed in time stops counting once it lapses, with no cycle to notice.
func TestLeaseHeldLapses(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewCompact(s)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Unix(1000, 0)
	m.now = func() time.Time { return clock }
	m.Lease(clock.Add(time.Minute))
	if b := body(t, s); !strings.Contains(b, "tracekeep_compact_lease_held 1") {
		t.Errorf("a live lease not held:\n%s", b)
	}
	clock = clock.Add(time.Minute)
	if b := body(t, s); !strings.Contains(b, "tracekeep_compact_lease_held 0") {
		t.Errorf("a lapsed lease still held:\n%s", b)
	}
	m.Lease(clock.Add(time.Minute))
	m.Lease(time.Time{})
	if b := body(t, s); !strings.Contains(b, "tracekeep_compact_lease_held 0") {
		t.Errorf("a released lease still held:\n%s", b)
	}
}
