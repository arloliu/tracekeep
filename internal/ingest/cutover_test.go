package ingest

import (
	"bytes"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/policy"
)

// The cutover to masking (rollouts.md §6.4): outside dev, a start waits while no masking configuration is active, then binds the one activated;
// a capture admitted before the cutover keeps its policy-less configuration, so its backlog, and an old leader still publishing to it after
// its successor started, are written as received, while the successor's records are masked.
func TestCutoverToMasking(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		p := newPipeline(t, c)
		var plain int64
		for id := range p.x.Configs {
			plain = id
		}
		b := policy.Baseline()
		carriers, err := maskingtest.Carriers(&b)
		if err != nil {
			t.Fatal(err)
		}
		ppbody := &carriers[0]
		// The capture an earlier release admitted, before any predicate.
		old := tracepack.UUID{0x61}
		p.feed(t, inline(t, old, startRecord(t, now), descriptorOf(t, "eqp")))
		// This release, outside dev: only a masking configuration binds, and none is active yet.
		mcfg := maskingConfig(t)
		p.x.Keys = maskingKeys(t, c)
		p.x.Bindable = func(id int64) bool { cfg, ok := p.x.Configs[id]; return ok && cfg.Policy != nil }
		successor := tracepack.UUID{0x62}
		if _, dec := dispose(t, p.x, inline(t, successor, startRecord(t, now), descriptorOf(t, "eqp"))); dec.Action != ActNak || dec.Reason != ReasonIncompatible {
			t.Fatalf("a start before any masking configuration is active = %+v, want a wait as incompatible", dec)
		}
		masking, err := c.StageConfig(ctx, catalog.Staging{Content: mcfg.Content()})
		if err != nil {
			t.Fatal(err)
		}
		p.x.Configs[masking], p.f.Configs[masking] = mcfg, mcfg
		if _, err := c.Register(ctx, "r", []int64{plain, masking}, time.Minute); err != nil {
			t.Fatal(err)
		}
		if _, err := c.ActivateStaged(ctx, masking, nil, nil); err != nil {
			t.Fatal(err)
		}
		p.feed(t, inline(t, successor, startRecord(t, now), descriptorOf(t, "eqp")))
		if cp, err := c.Capture(ctx, successor); err != nil || cp.ConfigID != masking {
			t.Fatalf("the successor = %+v, %v; want bound to the masking configuration %d", cp, err, masking)
		}
		if cp, err := c.Capture(ctx, old); err != nil || cp.ConfigID != plain {
			t.Fatalf("the earlier capture = %+v, %v; want still bound to %d", cp, err, plain)
		}
		// The old leader publishes to the earlier capture after its successor started; the successor carries recipe content too.
		p.feed(t, inline(t, old, carrierRecord(ppbody, 1), nil))
		p.feed(t, inline(t, successor, carrierRecord(ppbody, 1), nil))
		p.flushAll(t)
		for _, k := range storedKeys(t, p.st) {
			recs := records(t, p.st, k)
			if len(recs) != 2 {
				t.Fatalf("pack %s holds %d records", k, len(recs))
			}
			inClear := bytes.Contains(recs[1].Payload, ppbody.Secrets[0])
			switch header(t, p, k).CaptureID {
			case old:
				if !inClear || recs[1].Quality.Has(tracepack.QualityRedacted) {
					t.Error("the earlier capture's record was masked: its configuration changed")
				}
			case successor:
				if inClear || !recs[1].Quality.Has(tracepack.QualityRedacted) {
					t.Error("the successor's record was not masked")
				}
			default:
				t.Errorf("pack %s of an unknown capture", k)
			}
		}
	})
}
