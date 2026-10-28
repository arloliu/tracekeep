package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/migrations"
)

// setup writes a configuration over a migrated SQLite catalog, an embedded bus and an fs store, and returns its path.
func setup(t *testing.T) string {
	t.Helper()
	path, dsn := freshConfig(t)
	db, err := sql.Open("sqlite", catalog.SQLiteURI(dsn))
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(t.Context(), db, catalog.EngineSQLite); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	return path
}

// freshConfig writes a configuration over a SQLite catalog not yet created, an embedded bus and an fs store,
// and returns its path and the catalog's.
func freshConfig(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "catalog.db")
	yaml := "catalog:\n  dsn: " + dsn + "\nbus:\n  url: " + testbus.Start(t) +
		"\n  subject_prefix: tk\n  claim_bucket: claims\nstore:\n  kind: fs\n  dir: " + dir + "\nreplica: r1\n"
	path := filepath.Join(dir, "tracekeep.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	return path, dsn
}

// tracekeep migrate creates and migrates a catalog that does not exist yet, then the bus, and a second run has nothing to do.
func TestMigrateCreatesTheCatalog(t *testing.T) {
	t.Parallel()
	path, dsn := freshConfig(t)
	for range 2 {
		if code, _, stderr := tracekeep(t, "migrate", "--config", path); code != 0 {
			t.Fatalf("migrate = %d, %s", code, stderr)
		}
	}
	db, err := sql.Open("sqlite", catalog.SQLiteURI(dsn))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var revs, incarnations int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+migrations.Table+" WHERE applied = total").Scan(&revs); err != nil || revs == 0 {
		t.Errorf("revisions = %d, %v; want the migrations recorded", revs, err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM bus_streams").Scan(&incarnations); err != nil || incarnations != 2 {
		t.Errorf("incarnations = %d, %v; want both streams recorded after the catalog", incarnations, err)
	}
}

func tracekeep(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errs bytes.Buffer
	code = run(t.Context(), args, &out, &errs)

	return code, out.String(), errs.String()
}

// A command line the binary cannot run exits 2 with the usage.
func TestUsage(t *testing.T) {
	t.Parallel()
	path := setup(t)
	for _, args := range [][]string{
		nil, {"serve"}, {"migrate", "--config", path, "extra"}, {"admin", "--config", path},
		{"admin", "--config", path, "capture", "show", "not-a-uuid"}, {"admin", "--config", path, "parked", "terminate"},
		{"dev", "--flush", "0s"}, {"dev", "--data", t.TempDir(), "extra"}, {"admin", "--config", path, "object", "list", "--all"},
		{"admin", "--config", path, "scope", "merge", "01234567-89ab-cdef-0123-456789abcdef", "08:00"},
		{"admin", "--config", path, "conflict", "acknowledge", "01234567-89ab-cdef-0123-456789abcdef", "-1"},
		{"admin", "--config", path, "scope", "exclude-input", "not-a-uuid"},
	} {
		if code, _, stderr := tracekeep(t, args...); code != 2 || !strings.Contains(stderr, "usage:") {
			t.Errorf("tracekeep %v = %d, %q; want 2 with the usage", args, code, stderr)
		}
	}
	if code, _, stderr := tracekeep(t, "migrate", "--config", filepath.Join(t.TempDir(), "none.yaml")); code != 1 || !strings.Contains(stderr, "config") {
		t.Errorf("a missing configuration = %d, %q; want 1 naming it", code, stderr)
	}
}

// migrate, then operator commands, each writing its result as JSON.
func TestMigrateAndAdmin(t *testing.T) {
	t.Parallel()
	path := setup(t)
	if code, _, stderr := tracekeep(t, "migrate", "--config", path); code != 0 {
		t.Fatalf("migrate = %d, %s", code, stderr)
	}
	for _, args := range [][]string{
		{"parked", "list"}, {"object", "list"}, {"object", "list", "--unattributed"}, {"object", "list", "--survivors"},
		{"parked", "terminate", "01234567-89ab-cdef-0123-456789abcdef"},
		{"bus", "adopt-stream", "parking"},
		{"scope", "merge", "01234567-89ab-cdef-0123-456789abcdef", "2026-10-27T08:00:00Z"},
		{"capture", "close", "01234567-89ab-cdef-0123-456789abcdef"},
	} {
		code, stdout, stderr := tracekeep(t, append([]string{"admin", "--config", path, "--actor", "op"}, args...)...)
		if code != 0 || !json.Valid([]byte(stdout)) {
			t.Errorf("admin %v = %d, %q, %s; want JSON", args, code, stdout, stderr)
		}
	}
}

// An operator command the catalog refuses exits 1 with the reason.
func TestAdminRefused(t *testing.T) {
	t.Parallel()
	path := setup(t)
	for args, reason := range map[[5]string]string{
		{"conflict", "acknowledge", "01234567-89ab-cdef-0123-456789abcdef", "7"}: "no conflict",
		{"scope", "exclude-input", "01234567-89ab-cdef-0123-456789abcdef"}:       "unknown pack",
	} {
		line := append([]string{"admin", "--config", path}, slices.DeleteFunc(args[:], func(s string) bool { return s == "" })...)
		if code, _, stderr := tracekeep(t, line...); code != 1 || !strings.Contains(stderr, reason) {
			t.Errorf("tracekeep %v = %d, %q; want 1 naming %q", line, code, stderr, reason)
		}
	}
}

// An admin command on a SQLite catalog a running service holds says why it cannot run, and what to do.
func TestAdminBesideARunningService(t *testing.T) {
	t.Parallel()
	path := setup(t)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	held, err := catalog.Open(t.Context(), catalog.Config{Engine: catalog.EngineSQLite, DSN: cfg.Catalog.DSN})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	code, _, stderr := tracekeep(t, "admin", "--config", path, "parked", "list")
	if code != 1 || !strings.Contains(stderr, "stop the service") || !strings.Contains(stderr, "MariaDB") {
		t.Errorf("admin beside a running service = %d, %q; want the cause and the ways around it", code, stderr)
	}
}
