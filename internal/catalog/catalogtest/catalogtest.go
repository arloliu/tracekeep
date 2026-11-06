// Package catalogtest opens fresh, migrated catalogs (a SQLite one a copy of migrationstest's template) for the tests of the packages that use the catalog,
// one per engine, so their catalog-touching cases run on SQLite and on MariaDB (.agents/rules/300-testing.md *Catalog*).
// MariaDB runs when TRACEKEEP_MARIADB_DSN names a server (make test-catalog starts one) and is skipped otherwise,
// or fails when TRACEKEEP_REQUIRE_MARIADB is 1.
// SeedFleet seeds a fleet of equipment's open captures for other packages' benchmarks, and BenchEngines runs a benchmark on each engine.
package catalogtest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/migrations"
	"github.com/arloliu/tracekeep/migrations/migrationstest"
)

// EnvMariaDB names the MariaDB server's DSN.
const EnvMariaDB = "TRACEKEEP_MARIADB_DSN"

// EnvRequireMariaDB set to 1 fails a MariaDB run without a server instead of skipping it, as the catalog's benchmark smoke run does.
const EnvRequireMariaDB = "TRACEKEEP_REQUIRE_MARIADB"

// Engines runs f once per engine, each in a parallel subtest with a fresh catalog.
func Engines(t *testing.T, f func(t *testing.T, c *catalog.Catalog)) {
	t.Helper()
	for _, engine := range []string{catalog.EngineSQLite, catalog.EngineMariaDB} {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			f(t, Open(t, engine))
		})
	}
}

// Open returns a fresh, migrated catalog on engine, closed when the test ends; MariaDB skips the test without a server.
func Open(t testing.TB, engine string) *catalog.Catalog {
	t.Helper()
	c, _ := OpenClosable(t, engine)

	return c
}

// OpenClosable is Open whose closer the caller also holds: it closes the catalog, forgets it and deletes its database at once,
// as a benchmark does with each iteration's fresh catalog, which would otherwise stay open until the benchmark ends.
// The closer acts once, called early or by the test's end, which calls it in any case.
func OpenClosable(t testing.TB, engine string) (*catalog.Catalog, func()) {
	t.Helper()
	cfg := catalog.Config{Engine: engine}
	var drop func()
	switch engine {
	case catalog.EngineSQLite:
		dir := t.TempDir()
		cfg.DSN = filepath.Join(dir, "catalog.db")
		migrationstest.SQLite(t, cfg.DSN)
		drop = func() { _ = os.RemoveAll(dir) }
	case catalog.EngineMariaDB:
		cfg.DSN, drop = mariadbDatabase(t)
	default:
		t.Fatalf("unknown engine %q", engine)
	}
	c, err := catalog.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	driver, dsn := "mysql", cfg.DSN
	if engine == catalog.EngineSQLite {
		driver, dsn = "sqlite", catalog.SQLiteURI(cfg.DSN)
	}
	opened.Store(c, [2]string{driver, dsn})
	closeFn := sync.OnceFunc(func() {
		_ = c.Close()
		opened.Delete(c)
		drop()
	})
	t.Cleanup(closeFn)

	return c, closeFn
}

// Opened counts the catalogs Open and OpenClosable returned that are not yet closed.
func Opened() int {
	n := 0
	opened.Range(func(any, any) bool {
		n++
		return true
	})

	return n
}

// opened is each open catalog Open returned, with its driver and DSN, for ExecSQL.
var opened sync.Map

// ExecSQL runs stmt on c's database over a connection of its own, as a test breaks the schema under the catalog to make a real statement fail.
func ExecSQL(t testing.TB, c *catalog.Catalog, stmt string) {
	t.Helper()
	v, ok := opened.Load(c)
	if !ok {
		t.Fatal("catalogtest: ExecSQL of a catalog Open did not return")
	}
	conn, _ := v.([2]string)
	db, err := sql.Open(conn[0], conn[1])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), stmt); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
}

// mariadbDatabase creates a database of the test's own on the configured server, migrates it and returns its DSN
// and its drop, which acts once, called early or at the test's end.
func mariadbDatabase(t testing.TB) (string, func()) {
	t.Helper()
	dsn := os.Getenv(EnvMariaDB)
	if dsn == "" && os.Getenv(EnvRequireMariaDB) == "1" {
		t.Fatalf("%s not set, and %s requires MariaDB", EnvMariaDB, EnvRequireMariaDB)
	}
	if dsn == "" {
		t.Skipf("%s not set: no MariaDB to test against", EnvMariaDB)
	}
	mc, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "tk_" + strings.ToLower(rand.Text())
	exec(t, mc.FormatDSN(), "CREATE DATABASE "+name)
	server := mc.FormatDSN()
	drop := sync.OnceFunc(func() { exec(t, server, "DROP DATABASE "+name) })
	t.Cleanup(drop)
	mc.DBName = name
	m := mc.Clone()
	m.MultiStatements = true
	apply(t, "mysql", m.FormatDSN(), catalog.EngineMariaDB)

	return mc.FormatDSN(), drop
}

func exec(t testing.TB, dsn, stmt string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), stmt); err != nil {
		t.Errorf("%s: %v", stmt, err)
	}
}

func apply(t testing.TB, driver, dsn, engine string) {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrations.Apply(t.Context(), db, engine); err != nil {
		t.Fatal(err)
	}
}
