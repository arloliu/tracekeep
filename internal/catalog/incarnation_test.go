package catalog

import (
	"errors"
	"testing"
)

func TestIncarnate(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		first, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 100)
		if err != nil || first.ID != 1 {
			t.Fatalf("first Incarnate = %+v, %v; want incarnation 1", first, err)
		}
		if again, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 100); err != nil || again.ID != first.ID {
			t.Errorf("Incarnate of the same stream = %+v, %v; want incarnation 1", again, err)
		}
		if _, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 200); !errors.Is(err, ErrIncarnationChanged) {
			t.Errorf("Incarnate of a recreated stream = %v, want ErrIncarnationChanged", err)
		}
		if p, err := c.Incarnate(ctx, RoleParking, "PARKED", 300); err != nil || p.ID != 2 {
			t.Errorf("Incarnate of the parking stream = %+v, %v; want incarnation 2", p, err)
		}
	})
}

// The highest last sequence observed is kept: a lower observation may be one another replica overtook.
func TestObserveLastSeqKeepsTheHighest(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		inc, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, seq := range []int64{10, 10, 12, 11} {
			if err := c.ObserveLastSeq(ctx, inc.ID, seq); err != nil {
				t.Fatalf("ObserveLastSeq(%d): %v", seq, err)
			}
		}
		if got, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 100); err != nil || got.LastSeqSeen != 12 {
			t.Errorf("Incarnate = %+v, %v; want 12 observed", got, err)
		}
	})
}
