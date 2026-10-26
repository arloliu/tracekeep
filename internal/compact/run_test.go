package compact

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
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
	cfg := bus.Config{SubjectPrefix: "tk", ClaimBucket: "claims"}
	if _, _, err := bus.Ensure(t.Context(), js, cfg); err != nil {
		t.Fatal(err)
	}
	s := store.NewMem()
	mk := func(holder string) *Reconciler {
		return &Reconciler{
			Catalog: c, Store: s, HorizonHour: func() int64 { return hour - 24 }, Log: slog.New(slog.DiscardHandler),
			JS: js, Bus: cfg, Holder: holder, LeaseTTL: time.Hour, Interval: time.Minute,
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
		recreate(t, js, bus.StreamName(a.Bus, catalog.RoleTraffic))
		if err := a.Step(ctx); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Step = %v, want ErrIncarnationChanged", err)
		}
		if err := a.Run(ctx); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Errorf("Run = %v, want ErrIncarnationChanged", err)
		}
	})
}

// recreate deletes the stream name and creates it again with its configuration: a new incarnation the catalog does not record.
func recreate(t *testing.T, js jetstream.JetStream, name string) {
	t.Helper()
	s, err := js.Stream(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	cfg := s.CachedInfo().Config
	if err := js.DeleteStream(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
}

// A changed stream stops the reconciler before any effect: an orphan the last pass found unknown is not adopted,
// and a tombstoned key is not deleted (parking.md *incarnations*: verified at start and with every lease renewal).
func TestChangedIncarnationStopsBeforeAnyEffect(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		p := reconcilers(t, c)
		a := p.a
		f := &fixture{c: c, s: p.s}
		capture := f.admit(t)
		orphan, gone := segment(t, capture, []uint64{1}, false), segment(t, capture, []uint64{2}, false)
		f.put(t, orphan.key, orphan.data)
		step(t, a) // the orphan waits
		tombstone(t, c, capture, gone)
		f.put(t, gone.key, gone.data)
		recreate(t, p.js, bus.StreamName(a.Bus, catalog.RoleParking))
		if err := a.Step(ctx); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Fatalf("Step = %v, want ErrIncarnationChanged", err)
		}
		if got, _ := c.KeyStanding(ctx, orphan.key); got != catalog.KeyUnknown {
			t.Errorf("orphan standing %v, want unknown", got)
		}
		if !f.present(t, gone.key) {
			t.Errorf("the tombstoned key was deleted")
		}
	})
}

// A cycle longer than the lease keeps it: the keeper renews it while an object's read is held,
// and the adoption that follows commits under the same fence.
func TestLongCycleKeepsTheLease(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := reconcilers(t, c)
		a := p.a
		a.LeaseTTL = time.Second
		f := &fixture{c: c, s: p.s}
		orphan := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, orphan.key, orphan.data)
		step(t, a)
		renewed := make(chan struct{}, 16)
		a.hooks[hookRenewed] = func() { renewed <- struct{}{} }
		// The hold point: the orphan's read waits until the keeper has renewed the lease past its first expiry.
		// A cancelled cycle, the lease lost meanwhile, ends the hold; the assertions below then fail.
		a.Store = &opening{ObjectStore: p.s, at: orphan.key, hook: func(ctx context.Context) {
			for range 4 {
				select {
				case <-renewed:
				case <-ctx.Done():
					return
				}
			}
		}}
		step(t, a)
		if a.lease == nil {
			t.Fatal("the lease was lost during a long cycle")
		}
		if got, _ := c.KeyStanding(t.Context(), orphan.key); got != catalog.KeyRegistered {
			t.Errorf("standing %v, want adopted after the held read", got)
		}
	})
}

// tombstone tombstones p's key as a registration past the horizon does.
func tombstone(t *testing.T, c *catalog.Catalog, capture tracepack.UUID, p built) {
	t.Helper()
	pk := &catalog.Pack{
		ID: p.id, Capture: capture, ToolID: tool, Hour: hour, Role: catalog.RoleSegment, Key: p.key, Size: int64(len(p.data)),
		SeqStart: 2, FormatMinor: 1, Ranges: []catalog.Range{{SeqFrom: 2, SeqTo: 2}},
	}
	ev := &catalog.Evidence{Deliveries: []catalog.Identity{{Version: catalog.VersionKey{Capture: capture, Seq: 2}, Incarnation: 1, StreamSeq: 3}}}
	if out, err := c.RegisterPack(t.Context(), pk, catalog.Registration{Evidence: ev, HorizonHour: hour + 1}); err != nil || out != catalog.BeyondHorizon {
		t.Fatalf("RegisterPack = %v, %v; want the key tombstoned", out, err)
	}
}

// A stream changed in the middle of a cycle is found by the keeper's next renewal, which cancels the cycle:
// an orphan whose read was held across the change is not adopted, and the reconciler stops.
func TestChangedIncarnationFoundAtRenewal(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := reconcilers(t, c)
		a := p.a
		a.LeaseTTL = time.Second
		f := &fixture{c: c, s: p.s}
		orphan := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, orphan.key, orphan.data)
		step(t, a)
		// The hold point: past the cycle's first checks, the parking stream is replaced; the read waits for the cycle to end.
		a.Store = &opening{ObjectStore: p.s, at: orphan.key, hook: func(ctx context.Context) {
			recreate(t, p.js, bus.StreamName(a.Bus, catalog.RoleParking))
			<-ctx.Done()
		}}
		if err := a.Step(t.Context()); !errors.Is(err, catalog.ErrIncarnationChanged) {
			t.Fatalf("Step = %v, want ErrIncarnationChanged", err)
		}
		if got, _ := c.KeyStanding(t.Context(), orphan.key); got != catalog.KeyUnknown {
			t.Errorf("standing %v, want the held orphan not adopted", got)
		}
	})
}

// A lease lost in the middle of a cycle, here taken over by another process, cancels the cycle at the keeper's next renewal:
// the held read ends, Step returns, the holder drops the lease and adopts nothing; its metrics count no success and no lease.
func TestLeaseLostDuringACycle(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		p := reconcilers(t, c)
		a := p.a
		a.LeaseTTL = time.Second
		s := metrics.NewServer()
		var err error
		if a.Metrics, err = metrics.NewCompact(s); err != nil {
			t.Fatal(err)
		}
		f := &fixture{c: c, s: p.s}
		orphan := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, orphan.key, orphan.data)
		step(t, a)
		first := stamp(scrape(t, s))
		a.Store = &opening{ObjectStore: p.s, at: orphan.key, hook: func(held context.Context) {
			if err := c.ReleaseLease(ctx, *a.lease); err != nil {
				t.Error(err)
			}
			if _, ok, err := c.TakeLease(ctx, "compactor", "other", time.Hour); err != nil || !ok {
				t.Errorf("takeover = %v, %v", ok, err)
			}
			<-held.Done()
		}}
		step(t, a)
		if a.lease != nil {
			t.Errorf("a still holds %+v", *a.lease)
		}
		if got, _ := c.KeyStanding(ctx, orphan.key); got != catalog.KeyUnknown {
			t.Errorf("standing %v, want the held orphan not adopted", got)
		}
		m := scrape(t, s)
		if !strings.Contains(m, "tracekeep_compact_lease_held 0") || !strings.Contains(m, `tracekeep_compact_cycle_seconds_count{task="reconcile"} 2`) {
			t.Errorf("after the lost cycle, want the lease not held and both cycles observed:\n%s", m)
		}
		if got := stamp(m); got != first || first == "" {
			t.Errorf("last success %q after the lost cycle, want the first cycle's %q", got, first)
		}
	})
}

// A stopping reconciler releases its lease, so a standby takes over at once instead of waiting for it to lapse.
func TestRunReleasesTheLease(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := reconcilers(t, c)
		a := p.a
		f := &fixture{c: c, s: p.s}
		orphan := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, orphan.key, orphan.data)
		step(t, a) // the orphan waits for the next cycle, whose read is the stop point
		ctx, stop := context.WithCancel(t.Context())
		a.Store = &opening{ObjectStore: p.s, at: orphan.key, hook: func(context.Context) { stop() }}
		if err := a.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want its context's end", err)
		}
		if a.lease != nil {
			t.Errorf("a still holds %+v", *a.lease)
		}
		if _, ok, err := c.TakeLease(t.Context(), "compactor", "standby", time.Hour); err != nil || !ok {
			t.Errorf("the standby's take = %v, %v; want the released lease", ok, err)
		}
	})
}

// The reconciler's metrics and readiness: the holder adopts, counts its cycles and holds the lease; the standby holds none;
// neither is ready before it read the lease's state, and both are after.
func TestReconcilerMetrics(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		p := reconcilers(t, c)
		sa, sb := metrics.NewServer(), metrics.NewServer()
		var err error
		if p.a.Metrics, err = metrics.NewCompact(sa); err != nil {
			t.Fatal(err)
		}
		if p.b.Metrics, err = metrics.NewCompact(sb); err != nil {
			t.Fatal(err)
		}
		if err := p.a.Ready(ctx); err == nil {
			t.Error("ready before reading the lease's state")
		}
		f := &fixture{c: c, s: p.s}
		orphan := segment(t, f.admit(t), []uint64{1}, false)
		f.put(t, orphan.key, orphan.data)
		step(t, p.a)
		step(t, p.a)
		step(t, p.b)
		for name, r := range map[string]*Reconciler{"holder": p.a, "standby": p.b} {
			if err := r.Ready(ctx); err != nil {
				t.Errorf("%s not ready after reading the lease's state: %v", name, err)
			}
		}
		a, b := scrape(t, sa), scrape(t, sb)
		for _, want := range []string{
			"tracekeep_compact_orphans_adopted_total 1", "tracekeep_compact_lease_held 1",
			`tracekeep_compact_cycle_seconds_count{task="reconcile"} 2`, `tracekeep_compact_last_success_timestamp{task="reconcile"}`,
		} {
			if !strings.Contains(a, want) {
				t.Errorf("the holder's /metrics lacks %s", want)
			}
		}
		if !strings.Contains(b, "tracekeep_compact_lease_held 0") || strings.Contains(b, `cycle_seconds_count{task="reconcile"}`) {
			t.Error("the standby counts a cycle or holds the lease")
		}
	})
}

// stamp is the reconcile task's last success in a scrape, empty when there is none.
func stamp(scraped string) string {
	for line := range strings.Lines(scraped) {
		if v, ok := strings.CutPrefix(line, `tracekeep_compact_last_success_timestamp{task="reconcile"} `); ok {
			return strings.TrimSpace(v)
		}
	}

	return ""
}

func scrape(t *testing.T, s *metrics.Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)

	return string(body)
}
