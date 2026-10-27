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

// The merge's counters and the scope gauges, an unmerged scope's age counted from its hour's end.
func TestMergeMetrics(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewCompact(s)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Unix(10000, 0)
	m.now = func() time.Time { return clock }
	m.Merges(2, 1, 0, 1)
	m.Scopes(map[string]int{"open": 3, "settled": 5}, 2, clock.Add(-90*time.Second))
	b := body(t, s)
	for _, want := range []string{
		`tracekeep_compact_merges_total{outcome="merged"} 2`, `tracekeep_compact_merges_total{outcome="stale"} 1`,
		`tracekeep_compact_merges_total{outcome="failed"} 1`, `tracekeep_compact_scopes{state="open"} 3`,
		`tracekeep_compact_scopes{state="reopened"} 0`, "tracekeep_compact_conflicts_open 2",
		"tracekeep_compact_oldest_unmerged_scope_age_seconds 90",
	} {
		if !strings.Contains(b, want) {
			t.Errorf("/metrics lacks %s:\n%s", want, b)
		}
	}
	m.Scopes(nil, 0, time.Time{})
	if b := body(t, s); !strings.Contains(b, "tracekeep_compact_oldest_unmerged_scope_age_seconds 0") {
		t.Errorf("no unmerged scope, an age:\n%s", b)
	}
}

// closes_total counts closes by trigger.
func TestCloseMetrics(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewCompact(s)
	if err != nil {
		t.Fatal(err)
	}
	m.Closes(map[string]int{"inactivity": 2, "successor": 1})
	m.Closes(map[string]int{"operator": 1})
	b := body(t, s)
	for _, want := range []string{
		`tracekeep_compact_closes_total{trigger="inactivity"} 2`, `tracekeep_compact_closes_total{trigger="successor"} 1`,
		`tracekeep_compact_closes_total{trigger="operator"} 1`,
	} {
		if !strings.Contains(b, want) {
			t.Errorf("/metrics lacks %s:\n%s", want, b)
		}
	}
}

// Deletion and retention count their deletions and tombstones, and the claim gauges are set as read.
func TestRetentionMetrics(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewCompact(s)
	if err != nil {
		t.Fatal(err)
	}
	m.Orphans(0, 0, 1)
	m.Retention(2, 5)
	m.Claims(7, 7000, 1)
	b := body(t, s)
	for _, want := range []string{
		"tracekeep_compact_deletions_total 2", "tracekeep_compact_tombstones_total 6", "tracekeep_compact_claim_objects 7",
		"tracekeep_compact_claim_object_bytes 7000", "tracekeep_compact_claim_objects_survivors 1",
	} {
		if !strings.Contains(b, want) {
			t.Errorf("/metrics lacks %s:\n%s", want, b)
		}
	}
}
