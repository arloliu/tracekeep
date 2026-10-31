package service

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/migrations"
)

// putting signals every put of a segment.
type putting struct {
	store.ObjectStore
	put chan string
}

func (s *putting) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	err := s.ObjectStore.Put(ctx, key, r, size)
	if err == nil {
		// Never blocks: the shutdown flush puts under a context that does not end, after the test stopped listening.
		select {
		case s.put <- key:
		default:
		}
	}

	return err
}

// The gate's loopback capture (docs/ROADMAP.md phase 1): tracekeep dev runs the simulator's go-secs loopback pair through the SDK,
// ingest registers the capture's segments, and the packs read back with the tracepack reader hold its start and its traffic.
func TestDevLoopbackCapture(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	d, err := NewDev(ctx, DevOptions{Dir: t.TempDir(), Flush: 200 * time.Millisecond, Rate: 50, API: "127.0.0.1:0", IDP: "127.0.0.1:0", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	put := &putting{ObjectStore: d.Env.Store, put: make(chan string, 16)}
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

	got := registered(t, d.Env)
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

// registered reads every registered segment of e's store with the tracepack reader.
func registered(t *testing.T, e *Env) reading {
	t.Helper()
	ctx := t.Context()
	g := reading{captures: map[tracepack.UUID]bool{}}
	if err := e.Store.List(ctx, "staging/", func(o store.ObjectInfo) error { g.keys = append(g.keys, o.Key); return nil }); err != nil {
		t.Fatal(err)
	}
	for _, k := range g.keys {
		if s, err := e.Catalog.KeyStanding(ctx, k); err != nil || s != catalog.KeyRegistered {
			continue // a segment whose flush the stop cut before its registration
		}
		obj, err := e.Store.Open(ctx, k)
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

// tracekeep migrate takes the SQLite catalog's writer lock: beside a running service it is refused and changes nothing.
func TestMigrateCatalogBesideARunningService(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "catalog.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	held, err := catalog.Open(t.Context(), catalog.Config{Engine: catalog.EngineSQLite, DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := MigrateCatalog(t.Context(), &config.Catalog{Engine: catalog.EngineSQLite, DSN: path}); !errors.Is(err, catalog.ErrSecondWriter) {
		t.Fatalf("MigrateCatalog beside a held catalog = %v, want ErrSecondWriter", err)
	}
	_, read := held.Pools()
	var n int
	if err := read.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'").Scan(&n); err != nil || n != 0 {
		t.Errorf("%d tables after a refused migration, %v; want none", n, err)
	}
}

// A dev directory whose catalog an earlier build made, tables and no revisions, is refused with the way out.
func TestDevRefusesACatalogWithoutRevisions(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := sql.Open("sqlite", catalog.SQLiteURI(filepath.Join(dir, "catalog.db")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE captures (a INTEGER)"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	_, err = NewDev(t.Context(), DevOptions{Dir: dir, Flush: time.Second, API: "127.0.0.1:0", IDP: "127.0.0.1:0", Log: slog.New(slog.DiscardHandler)})
	if !errors.Is(err, migrations.ErrOtherTool) || !strings.Contains(err.Error(), "fresh --data") {
		t.Errorf("NewDev over a catalog without revisions = %v, want refused with the way out", err)
	}
}
