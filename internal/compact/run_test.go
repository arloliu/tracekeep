package compact

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/testbus"
)

// pair is two reconcilers of one catalog, store and bus, as two compact processes would be.
type pair struct {
	a, b *Reconciler
	s    *store.Mem
	js   jetstream.JetStream
}

func reconcilers(t *testing.T, c *catalog.Catalog) pair {
	t.Helper()
	nc, err := nats.Connect(testbus.Start(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	traffic, parked, err := bus.Ensure(t.Context(), js, bus.Config{SubjectPrefix: "tk", ClaimBucket: "claims"})
	if err != nil {
		t.Fatal(err)
	}
	s := store.NewMem()
	mk := func(holder string) *Reconciler {
		return &Reconciler{
			Catalog: c, Store: s, HorizonHour: func() int64 { return hour - 24 }, Log: slog.New(slog.DiscardHandler),
			Traffic: traffic, Parked: parked, Holder: holder, LeaseTTL: time.Hour, Interval: time.Minute,
		}
	}

	return pair{a: mk("a"), b: mk("b"), s: s, js: js}
}

func step(t *testing.T, r *Reconciler) {
	t.Helper()
	if err := r.Step(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Only the lease's holder reconciles: a second process adopts nothing while the first holds the lease.
func TestStepUnderTheLease(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := reconcilers(t, c)
		a, b := p.a, p.b
		f := &fixture{c: c, s: p.s}
		capture := f.admit(t)
		first, second := segment(t, capture, []uint64{1}, false), segment(t, capture, []uint64{2}, false)
		f.put(t, first.key, first.data)
		step(t, a)
		step(t, b)
		step(t, b)
		if got, _ := c.KeyStanding(t.Context(), first.key); got != catalog.KeyUnknown {
			t.Fatalf("standing %v after the standby's cycles, want unknown", got)
		}
		step(t, a)
		f.put(t, second.key, second.data)
		step(t, a)
		step(t, a)
		for _, k := range []string{first.key, second.key} {
			if got, _ := c.KeyStanding(t.Context(), k); got != catalog.KeyRegistered {
				t.Errorf("%s standing %v, want registered by the holder", k, got)
			}
		}
	})
}

// A holder whose lease lapsed and passed to another process does nothing further, and takes no lease back while the other's is live.
func TestStepAfterTheLeaseIsLost(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := reconcilers(t, c)
		a, b := p.a, p.b
		a.LeaseTTL = time.Nanosecond // lapses as soon as it is taken
		step(t, a)
		step(t, b)
		f := &fixture{c: c, s: p.s}
		orphan := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, orphan.key, orphan.data)
		step(t, a)
		step(t, a)
		if a.lease != nil {
			t.Errorf("a holds %+v after losing the lease", *a.lease)
		}
		if got, _ := c.KeyStanding(t.Context(), orphan.key); got != catalog.KeyUnknown {
			t.Errorf("standing %v, want unknown: the lost holder adopted", got)
		}
	})
}

// A stream that is no longer the catalog's incarnation stops the reconciler until an operator adopts it.
func TestStepStopsOnAChangedIncarnation(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		p := reconcilers(t, c)
		a, js := p.a, p.js
		step(t, a)
		cfg := a.Traffic.CachedInfo().Config
		if err := js.DeleteStream(ctx, cfg.Name); err != nil {
			t.Fatal(err)
		}
		again, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		a.Traffic = again
		if err := a.Step(ctx); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Step = %v, want ErrIncarnationChanged", err)
		}
		if err := a.Run(ctx); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Run = %v, want ErrIncarnationChanged", err)
		}
	})
}
