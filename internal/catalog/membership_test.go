package catalog

import (
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

const ttl = time.Minute

// lapse ends a lease now by the database's clock, as time passing would.
func lapse(t *testing.T, c *Catalog, query string) {
	t.Helper()
	if _, err := c.write.ExecContext(t.Context(), query); err != nil {
		t.Fatal(err)
	}
}

func TestLease(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		a, ok, err := c.TakeLease(ctx, "compactor", "a", ttl)
		if err != nil || !ok || a.Fence != 1 {
			t.Fatalf("first take = %+v, %v, %v; want fence 1", a, ok, err)
		}
		if _, ok, err := c.TakeLease(ctx, "compactor", "b", ttl); err != nil || ok {
			t.Fatalf("take of a live lease = %v, %v; want refused", ok, err)
		}
		if err := c.RenewLease(ctx, a, ttl); err != nil {
			t.Fatalf("renew by the holder: %v", err)
		}

		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor'`)
		b, ok, err := c.TakeLease(ctx, "compactor", "b", ttl)
		if err != nil || !ok || b.Fence != 2 {
			t.Fatalf("take of a lapsed lease = %+v, %v, %v; want fence 2", b, ok, err)
		}
		if err := c.RenewLease(ctx, a, ttl); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("renew by the old holder = %v, want ErrLeaseLost", err)
		}
		err = c.inTx(ctx, func(tx *sqlx.Tx) error { return c.checkLease(ctx, tx, a) })
		if !errors.Is(err, ErrLeaseLost) {
			t.Errorf("lease check of the old holder = %v, want ErrLeaseLost", err)
		}
		if err := c.inTx(ctx, func(tx *sqlx.Tx) error { return c.checkLease(ctx, tx, b) }); err != nil {
			t.Errorf("lease check of the holder = %v", err)
		}
	})
}

// A replica registers only when it executes every configuration not yet retired (rollouts.md §6.4).
func TestRegisterNeedsEveryUnretiredConfig(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		v1 := activate(t, c, "v1")
		v2 := activate(t, c, "v2")
		if again := activate(t, c, "v2"); again != v2 {
			t.Fatalf("the same content activated as %d and %d", v2, again)
		}
		if _, err := c.Register(ctx, "old", []int64{v1}, ttl); !errors.Is(err, ErrConfigNotExecuted) {
			t.Fatalf("Register without v2 = %v, want ErrConfigNotExecuted", err)
		}
		if _, err := c.Register(ctx, "new", []int64{v1, v2}, ttl); err != nil {
			t.Fatalf("Register with both: %v", err)
		}
		if err := c.RetireConfig(ctx, v2, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Register(ctx, "old", []int64{v1}, ttl); err != nil {
			t.Errorf("Register once v2 is retired: %v", err)
		}
	})
}

// A membership renews only while its lease is live and its registration is the current one.
func TestRenew(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		m, err := c.Register(ctx, "r", nil, ttl)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Renew(ctx, m, ttl); err != nil {
			t.Fatalf("renew: %v", err)
		}
		again, err := c.Register(ctx, "r", nil, ttl)
		if err != nil || again.Generation == m.Generation {
			t.Fatalf("re-register = %+v, %v; want a new generation", again, err)
		}
		if err := c.Renew(ctx, m, ttl); !errors.Is(err, ErrMembershipLapsed) {
			t.Errorf("renew of a replaced registration = %v, want ErrMembershipLapsed", err)
		}
		lapse(t, c, `UPDATE consumers SET lease_until = 0 WHERE replica_id = 'r'`)
		if err := c.Renew(ctx, again, ttl); !errors.Is(err, ErrMembershipLapsed) {
			t.Errorf("renew of a lapsed lease = %v, want ErrMembershipLapsed", err)
		}
		if err := c.Leave(ctx, "r"); err != nil {
			t.Fatal(err)
		}
		if err := c.Renew(ctx, again, ttl); !errors.Is(err, ErrMembershipLapsed) {
			t.Errorf("renew after leaving = %v, want ErrMembershipLapsed", err)
		}
		// A registration after leaving takes a generation never used, so no earlier handle renews it.
		rejoined, err := c.Register(ctx, "r", nil, ttl)
		if err != nil {
			t.Fatal(err)
		}
		if rejoined.Generation == m.Generation || rejoined.Generation == again.Generation {
			t.Fatalf("the rejoined generation %d reuses an earlier one (%d, %d)", rejoined.Generation, m.Generation, again.Generation)
		}
		if err := c.Renew(ctx, again, ttl); !errors.Is(err, ErrMembershipLapsed) {
			t.Errorf("renew of the earlier handle after rejoining = %v, want ErrMembershipLapsed", err)
		}
	})
}

// A retirement is refused when a capture the caller did not find quiescent is bound to the configuration,
// such as one admitted after the caller looked (rollouts.md §6.4).
func TestRetireNeedsEveryBoundCaptureQuiescent(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		cfg := gate(t, c)
		early := newID(t)
		if _, err := c.Admit(ctx, early, descriptor(t, "a")); err != nil {
			t.Fatal(err)
		}
		observed := []tracepack.UUID{early} // the caller's quiescence check saw this capture only
		late := newID(t)
		if _, err := c.Admit(ctx, late, descriptor(t, "b")); err != nil {
			t.Fatal(err)
		}
		if err := c.RetireConfig(ctx, cfg, observed); !errors.Is(err, ErrNotQuiescent) {
			t.Fatalf("RetireConfig = %v, want ErrNotQuiescent", err)
		}
		if err := c.RetireConfig(ctx, cfg, []tracepack.UUID{early, late}); err != nil {
			t.Errorf("RetireConfig with every bound capture quiescent: %v", err)
		}
	})
}

func TestTakeLeaseOfAnUnknownName(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		if _, _, err := c.TakeLease(t.Context(), "nobody", "a", ttl); !errors.Is(err, ErrUnknownLease) {
			t.Errorf("TakeLease = %v, want ErrUnknownLease", err)
		}
	})
}

// A new capture binds the newest configuration every leased member supports;
// a lapsed member no longer holds a newer one back (rollouts.md §6.4).
func TestBindableConfig(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		if id, ok := bindable(t, c); ok {
			t.Fatalf("bindable with no configuration = %d", id)
		}
		v1 := activate(t, c, "v1")
		if _, err := c.Register(ctx, "old", []int64{v1}, ttl); err != nil {
			t.Fatal(err)
		}
		v2 := activate(t, c, "v2")
		if _, err := c.Register(ctx, "new", []int64{v1, v2}, ttl); err != nil {
			t.Fatal(err)
		}
		if id, ok := bindable(t, c); !ok || id != v1 {
			t.Fatalf("bindable while the old member is leased = %d, %v; want v1", id, ok)
		}
		lapse(t, c, `UPDATE consumers SET lease_until = 0 WHERE replica_id = 'old'`)
		if id, ok := bindable(t, c); !ok || id != v2 {
			t.Errorf("bindable once the old member lapsed = %d, %v; want v2", id, ok)
		}
	})
}

func activate(t *testing.T, c *Catalog, content string) int64 {
	t.Helper()
	id, err := c.ActivateConfig(t.Context(), []byte(content))
	if err != nil {
		t.Fatal(err)
	}

	return id
}

func bindable(t *testing.T, c *Catalog) (int64, bool) {
	t.Helper()
	var id int64
	var ok bool
	err := c.inTx(t.Context(), func(tx *sqlx.Tx) error {
		if err := c.lockMembership(t.Context(), tx); err != nil {
			return err
		}
		var err error
		id, ok, err = c.bindableConfig(t.Context(), tx)

		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	return id, ok
}
