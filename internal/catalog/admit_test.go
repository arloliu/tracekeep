package catalog

import (
	"errors"
	"testing"
)

func descriptor(t *testing.T, b string) *Descriptor {
	t.Helper()

	return &Descriptor{Bytes: []byte(b), ToolID: "T1", RecorderInstanceID: newID(t), Device: "eqp", OriginNS: 1}
}

// gate registers one member executing a fresh configuration, so admissions can bind it.
func gate(t *testing.T, c *Catalog) int64 {
	t.Helper()
	cfg := activate(t, c, "config")
	if _, err := c.Register(t.Context(), "r", []int64{cfg}, ttl); err != nil {
		t.Fatal(err)
	}

	return cfg
}

func TestAdmit(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		cfg := gate(t, c)
		id := newID(t)
		d := descriptor(t, "desc-a")

		res, err := c.Admit(ctx, start(id), d)
		if err != nil || res != (Result{Outcome: Admitted, ConfigID: cfg}) {
			t.Fatalf("Admit of an unknown capture = %+v, %v; want admitted with %d", res, err, cfg)
		}
		got, err := c.Capture(ctx, id)
		if err != nil || got.State != StateOpen || got.ConfigID != cfg || got.Device != "eqp" || string(got.Descriptor) != "desc-a" {
			t.Fatalf("Capture = %+v, %v", got, err)
		}
		if res, err := c.Admit(ctx, start(id), d); err != nil || res != (Result{Outcome: Unchanged, ConfigID: cfg}) {
			t.Errorf("Admit with equal bytes = %+v, %v; want unchanged", res, err)
		}
		if res, err := c.Admit(ctx, start(id), descriptor(t, "desc-b")); err != nil || res.Outcome != DescriptorConflict {
			t.Errorf("Admit with other bytes = %+v, %v; want a descriptor conflict", res, err)
		}
		if got, _ := c.Capture(ctx, id); string(got.Descriptor) != "desc-a" {
			t.Errorf("a conflicting admission overwrote the descriptor with %q", got.Descriptor)
		}
	})
}

func TestReject(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		cfg := gate(t, c)
		id := newID(t)

		if res, err := c.Reject(ctx, start(id), []byte("bad")); err != nil || res.Outcome != Rejected {
			t.Fatalf("Reject of an unknown capture = %+v, %v; want rejected", res, err)
		}
		if got, err := c.Capture(ctx, id); err != nil || got.State != StateRejected || string(got.Descriptor) != "bad" {
			t.Fatalf("Capture = %+v, %v; want rejected with its bytes", got, err)
		}
		// A fixed consumer accepts the retained bytes: rejected → open.
		if res, err := c.Admit(ctx, start(id), descriptor(t, "bad")); err != nil || res.Outcome != Admitted {
			t.Fatalf("Admit of the rejected bytes = %+v, %v; want admitted", res, err)
		}
		if res, err := c.Reject(ctx, start(id), []byte("bad")); err != nil || res != (Result{Outcome: AlreadyAdmitted, ConfigID: cfg}) {
			t.Errorf("Reject of an admitted capture with equal bytes = %+v, %v; want already admitted", res, err)
		}
		if res, err := c.Reject(ctx, start(id), []byte("other")); err != nil || res.Outcome != DescriptorConflict {
			t.Errorf("Reject of an admitted capture with other bytes = %+v, %v; want a descriptor conflict", res, err)
		}
		if got, _ := c.Capture(ctx, id); got.State != StateOpen {
			t.Errorf("a rejection overwrote an admitted capture: %+v", got)
		}
	})
}

// A rejected capture whose retained bytes differ from the delivery's is not admitted.
func TestAdmitRejectedWithOtherBytes(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		gate(t, c)
		id := newID(t)
		if _, err := c.Reject(ctx, start(id), []byte("bad")); err != nil {
			t.Fatal(err)
		}
		if res, err := c.Admit(ctx, start(id), descriptor(t, "good")); err != nil || res.Outcome != DescriptorConflict {
			t.Errorf("Admit = %+v, %v; want a descriptor conflict", res, err)
		}
		if got, _ := c.Capture(ctx, id); got.State != StateRejected {
			t.Errorf("state %q, want rejected", got.State)
		}
	})
}

// With no configuration every leased member supports, admission writes nothing.
func TestAdmitWithoutABindableConfig(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		id := newID(t)
		if _, err := c.Admit(ctx, start(id), descriptor(t, "d")); !errors.Is(err, ErrNoBindableConfig) {
			t.Fatalf("Admit = %v, want ErrNoBindableConfig", err)
		}
		if _, err := c.Capture(ctx, id); !errors.Is(err, ErrUnknownCapture) {
			t.Errorf("Capture after a failed admission = %v, want no row", err)
		}
	})
}
