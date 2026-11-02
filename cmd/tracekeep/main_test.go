package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/service"
	"github.com/arloliu/tracekeep/internal/store"
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

// tracekeep dev refuses a simulator rate it cannot tick at, before it starts anything.
func TestDevRefusesARateItCannotTick(t *testing.T) {
	t.Parallel()
	for _, rate := range []string{"1e300", "+Inf", "1e-300", "-1", "NaN"} {
		code, _, stderr := tracekeep(t, "dev", "--data", t.TempDir(), "--port", "0", "--api", "127.0.0.1:0", "--idp", "127.0.0.1:0", "--rate", rate)
		if code != 2 || !strings.Contains(stderr, "rate") {
			t.Errorf("tracekeep dev --rate %s = %d, %q; want 2 naming the rate", rate, code, stderr)
		}
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
		{"deferred", "list"}, {"deferred", "list", "--capture", "01234567-89ab-cdef-0123-456789abcdef"},
	} {
		code, stdout, stderr := tracekeep(t, append([]string{"admin", "--config", path, "--actor", "op"}, args...)...)
		if code != 0 || !json.Valid([]byte(stdout)) {
			t.Errorf("admin %v = %d, %q, %s; want JSON", args, code, stdout, stderr)
		}
	}
}

// tracekeep status prints the pending captures and the tools as JSON, and takes no arguments.
func TestStatusCommand(t *testing.T) {
	t.Parallel()
	path := setup(t)
	if code, _, stderr := tracekeep(t, "migrate", "--config", path); code != 0 {
		t.Fatalf("migrate = %d, %s", code, stderr)
	}
	code, stdout, stderr := tracekeep(t, "status", "--config", path)
	var st struct {
		Pending []any `json:"pending"`
		Tools   []any `json:"tools"`
	}
	if code != 0 || json.Unmarshal([]byte(stdout), &st) != nil || st.Pending == nil || st.Tools == nil {
		t.Errorf("status = %d, %q, %s; want its JSON", code, stdout, stderr)
	}
	if code, _, _ := tracekeep(t, "status", "--config", path, "extra"); code != 2 {
		t.Errorf("status with an argument = %d, want usage", code)
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

// serviceConfig writes a configuration that runs every role: setup's, with the API over TLS and its download key,
// work directories of the test's own and metrics on a free port, and returns its path and the metrics address.
func serviceConfig(t *testing.T) (string, string) {
	t.Helper()
	path := setup(t)
	dir := filepath.Dir(path)
	cert, key := selfSigned(t, dir)
	dl := secret(t, dir, "dl-1", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))))
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	base, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	yaml := string(base) + "api:\n  listen: 127.0.0.1:0\n  tls_cert_file: " + cert + "\n  tls_key_file: " + key + "\n  public_url: https://127.0.0.1:1" +
		"\nkeys:\n  declared:\n    dl-1: {provider: file, ref: " + dl + "}\n  download:\n    issuing: dl-1\n    accepted: [dl-1]\n" +
		"metrics:\n  listen: " + addr + "\ningest:\n  work_dir: " + filepath.Join(dir, "ingest") + "\nexports:\n  work_dir: " + filepath.Join(dir, "exports") + "\n"

	return secret(t, dir, "service.yaml", yaml), addr
}

// api and all end on a stop signal with exit code 0: the cancellation is how they are asked to stop, not a failure.
func TestRoleStopsCleanly(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"api", "all"} {
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			path, metricsAddr := serviceConfig(t)
			if code, _, stderr := tracekeep(t, "migrate", "--config", path); code != 0 {
				t.Fatalf("migrate = %d, %s", code, stderr)
			}
			ctx, stop := context.WithCancel(t.Context())
			defer stop()
			var stderr bytes.Buffer
			done := make(chan int, 1)
			go func() { done <- run(ctx, []string{command, "--config", path}, io.Discard, &stderr) }()
			// Stopped once every role is ready, so the stop meets a running process, not one still starting.
			if code, ended := waitReady(t, metricsAddr, done); ended {
				t.Fatalf("%s ended before it was ready: %d, %s", command, code, stderr.String())
			}
			stop()
			if code := <-done; code != 0 {
				t.Errorf("%s on a stop signal = %d, %s; want 0", command, code, stderr.String())
			}
			// The bus is plain: the command warns of it once, all once for its three roles.
			if n := strings.Count(stderr.String(), "the bus connection is without TLS"); n != 1 {
				t.Errorf("%s warned %d times of the plain bus, want once: %s", command, n, stderr.String())
			}
		})
	}
}

// closeLogged logs a failed close as one line and a clean one as none.
func TestCloseLogged(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	closeLogged(log, "the environment", closer(func() error { return errors.New("close-sentinel") }))
	if got := buf.String(); strings.Count(got, "\n") != 1 || !strings.Contains(got, "close-sentinel") || !strings.Contains(got, `"level":"ERROR"`) {
		t.Errorf("a failed close logged %q, want one error line", got)
	}
	buf.Reset()
	closeLogged(log, "the environment", closer(func() error { return nil }))
	if buf.Len() != 0 {
		t.Errorf("a clean close logged %q", buf.String())
	}
}

type closer func() error

func (c closer) Close() error { return c() }

// keyedClose is a store whose close fails with an error naming an object's key and the backend's text.
type keyedClose struct{ store.ObjectStore }

func (k keyedClose) Close() error {
	err := errors.New("store: close packs/T1/key-sentinel.tpk: backend-sentinel")
	if c, ok := k.ObjectStore.(io.Closer); ok {
		err = errors.Join(err, c.Close())
	}

	return err
}

// A command whose environment's store fails to close logs one line without the key or the backend's text, its exit code unchanged.
// Not parallel: it replaces the command's open seam.
func TestDispatchLogsAKeylessCloseError(t *testing.T) {
	path := setup(t)
	if code, _, stderr := tracekeep(t, "migrate", "--config", path); code != 0 {
		t.Fatalf("migrate = %d, %s", code, stderr)
	}
	t.Cleanup(func() { openEnv = service.Open })
	openEnv = func(ctx context.Context, cfg *config.Config, log *slog.Logger) (*service.Env, error) {
		e, err := service.Open(ctx, cfg, log)
		if err == nil {
			e.Store = keyedClose{e.Store}
		}

		return e, err
	}
	code, stdout, stderr := tracekeep(t, "status", "--config", path)
	if code != 0 || !json.Valid([]byte(stdout)) {
		t.Errorf("status = %d, %q, %s; want 0 with its JSON", code, stdout, stderr)
	}
	if n := strings.Count(stderr, "tracekeep: close"); n != 1 || strings.Contains(stderr, "key-sentinel") || strings.Contains(stderr, "backend-sentinel") {
		t.Errorf("stderr %q, want one keyless close line", stderr)
	}
}

// waitReady reads addr's /readyz, as a probe does, until it answers ok or the command ends, returning its exit code then.
func waitReady(t *testing.T, addr string, done <-chan int) (int, bool) {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(30 * time.Second) // bounds a hung start only
	for {
		select {
		case code := <-done:
			return code, true
		case <-deadline:
			t.Fatal("the command never became ready")
		case <-tick.C:
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/readyz", http.NoBody)
			if err != nil {
				t.Fatal(err)
			}
			if resp, err := http.DefaultClient.Do(req); err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return 0, false
				}
			}
		}
	}
}
