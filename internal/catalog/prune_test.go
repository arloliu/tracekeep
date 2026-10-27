package catalog

import (
	"errors"
	"testing"
)

// eventsAt writes the scope events seq 1, 2, … at the times ats and sets the counter to the last, as their transactions would.
func eventsAt(t *testing.T, c *Catalog, ats ...int64) {
	t.Helper()
	capture := newID(t)
	for i, at := range ats {
		exec(t, c, `INSERT INTO scope_events (seq, capture_id, hour, event, revision, at) VALUES (?, ?, 1, 'created', 1, ?)`, i+1, capture[:], at)
	}
	exec(t, c, `UPDATE counters SET value = ? WHERE name = 'scope_events'`, len(ats))
}

func lowWater(t *testing.T, c *Catalog) int {
	t.Helper()

	return count(t, c, `SELECT low_water FROM counters WHERE name = 'scope_events'`)
}

// Pruning deletes the seq prefix below the lowest seq of an event younger than the period, never by time alone,
// raises low_water to it in the same transaction, and deletes nothing when no event is younger.
func TestPruneEvents(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		l := compactorLease(t, c, "w")
		eventsAt(t, c, 10, 70, 20, 60) // seq 3 is old but above seq 2, which is young
		prune := func(now int64) int {
			t.Helper()
			n, err := c.PruneEvents(t.Context(), l, 25, now)
			if err != nil {
				t.Fatal(err)
			}

			return n
		}
		if n := prune(1000); n != 0 || lowWater(t, c) != 0 || count(t, c, `SELECT COUNT(*) FROM scope_events`) != 4 {
			t.Fatalf("with no young event: pruned %d, low_water %d", n, lowWater(t, c))
		}
		if n := prune(80); n != 1 || lowWater(t, c) != 2 || count(t, c, `SELECT MIN(seq) FROM scope_events`) != 2 {
			t.Fatalf("pruned %d, low_water %d; want seq 1 gone and low_water 2", n, lowWater(t, c))
		}
		if n := prune(80); n != 0 || lowWater(t, c) != 2 {
			t.Errorf("again: pruned %d, low_water %d", n, lowWater(t, c))
		}
		// low_water is never lowered, whatever a prune computes.
		exec(t, c, `UPDATE counters SET low_water = 9 WHERE name = 'scope_events'`)
		prune(80)
		if lowWater(t, c) != 9 {
			t.Errorf("low_water lowered to %d", lowWater(t, c))
		}
	})
}

func TestPruneEventsUnderALostLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		stale := compactorLease(t, c, "old")
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		compactorLease(t, c, "new")
		eventsAt(t, c, 10, 70)
		if _, err := c.PruneEvents(t.Context(), stale, 25, 80); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("PruneEvents = %v, want ErrLeaseLost", err)
		}
		if count(t, c, `SELECT COUNT(*) FROM scope_events`) != 2 || lowWater(t, c) != 0 {
			t.Error("a refused prune wrote")
		}
	})
}
