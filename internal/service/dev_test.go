package service

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
)

// putting signals every put of a segment.
type putting struct {
	store.ObjectStore
	put chan string
}

func (s *putting) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	err := s.ObjectStore.Put(ctx, key, r, size)
	if err == nil {
		select {
		case s.put <- key:
		case <-ctx.Done():
		}
	}

	return err
}

// The gate's loopback capture (docs/ROADMAP.md phase 1): tracekeep dev runs the simulator's go-secs loopback pair through the SDK,
// ingest registers the capture's segments, and the packs read back with the tracepack reader hold its start and its traffic.
func TestDevLoopbackCapture(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	d, err := NewDev(ctx, DevOptions{Dir: t.TempDir(), Flush: 200 * time.Millisecond, Rate: 50, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	put := &putting{ObjectStore: d.Env.Store, put: make(chan string)}
	d.Env.Store = put
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(run) }()
	// Flushes run one after another, put, registration, acknowledgements, so a later flush's put follows an earlier one's registration;
	// a flush holds one segment per hour, so of three puts the last comes from a later flush than the first, even across an hour.
	for range 3 {
		select {
		case <-put.put:
		case err := <-done:
			t.Fatalf("dev ended before three segments: %v", err)
		}
	}
	stop()
	if err := <-done; err != nil && !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("dev = %v", err)
	}

	got := registered(t, d)
	if len(got.captures) != 1 || got.start != 1 || got.data == 0 {
		t.Errorf("registered packs %v hold %d captures, %d starts and %d data records; want one capture with its start and traffic", got.keys, len(got.captures), got.start, got.data)
	}
	for id := range got.captures {
		if c, err := d.Env.Catalog.Capture(ctx, id); err != nil || c.State != catalog.StateOpen || c.ToolID != "SIM-TOOL" {
			t.Errorf("capture %s = %+v, %v; want it admitted", id, c, err)
		}
	}
}

// reading is what the registered segments of a store hold: the keys listed, the captures, and their start and data records.
type reading struct {
	keys        []string
	captures    map[tracepack.UUID]bool
	start, data int
}

// registered reads every registered segment of d's store with the tracepack reader.
func registered(t *testing.T, d *Dev) reading {
	t.Helper()
	ctx := t.Context()
	g := reading{captures: map[tracepack.UUID]bool{}}
	if err := d.Env.Store.List(ctx, "staging/", func(o store.ObjectInfo) error { g.keys = append(g.keys, o.Key); return nil }); err != nil {
		t.Fatal(err)
	}
	for _, k := range g.keys {
		if s, err := d.Env.Catalog.KeyStanding(ctx, k); err != nil || s != catalog.KeyRegistered {
			continue // a segment whose flush the stop cut before its registration
		}
		obj, err := d.Env.Store.Open(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		r, err := tracepack.Open(ctx, obj, obj.Size(), tracepack.ReaderOptions{})
		if err != nil {
			t.Fatalf("%s: %v", k, err)
		}
		g.captures[r.Header().CaptureID] = true
		if r.Header().Meta.ToolID != "SIM-TOOL" {
			t.Errorf("%s: tool %q, want the simulator's", k, r.Header().Meta.ToolID)
		}
		_, err = r.Iterate(ctx, tracepack.Query{}, func(it *tracepack.Item) error {
			switch {
			case it.Record.Seq == 0 && it.Record.Kind == tracepack.KindTransportEvent:
				g.start++
			case it.Record.Kind == tracepack.KindData:
				g.data++
			default:
			}

			return nil
		})
		_ = obj.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	return g
}
