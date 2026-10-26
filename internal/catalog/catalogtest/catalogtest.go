// Package catalogtest opens fresh, migrated catalogs for the tests of the packages that use the catalog,
// one per engine, so their catalog-touching cases run on SQLite and on MariaDB (.agents/rules/300-testing.md *Catalog*).
// MariaDB runs when TRACEKEEP_MARIADB_DSN names a server (make test-catalog starts one) and is skipped otherwise.
package catalogtest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/migrations"
)

// EnvMariaDB names the MariaDB server's DSN.
const EnvMariaDB = "TRACEKEEP_MARIADB_DSN"

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
func Open(t *testing.T, engine string) *catalog.Catalog {
	t.Helper()
	cfg := catalog.Config{Engine: engine}
	switch engine {
	case catalog.EngineSQLite:
		cfg.DSN = filepath.Join(t.TempDir(), "catalog.db")
		apply(t, "sqlite", catalog.SQLiteURI(cfg.DSN), engine)
	case catalog.EngineMariaDB:
		cfg.DSN = mariadbDatabase(t)
	default:
		t.Fatalf("unknown engine %q", engine)
	}
	c, err := catalog.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	return c
}

// mariadbDatabase creates a database of the test's own on the configured server, migrates it and returns its DSN.
func mariadbDatabase(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(EnvMariaDB)
	if dsn == "" {
		t.Skipf("%s not set: no MariaDB to test against", EnvMariaDB)
	}
	mc, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "tk_" + strings.ToLower(rand.Text())
	exec(t, mc.FormatDSN(), "CREATE DATABASE "+name)
	t.Cleanup(func() { exec(t, mc.FormatDSN(), "DROP DATABASE "+name) })
	mc.DBName = name
	m := mc.Clone()
	m.MultiStatements = true
	apply(t, "mysql", m.FormatDSN(), catalog.EngineMariaDB)

	return mc.FormatDSN()
}

func exec(t *testing.T, dsn, stmt string) {
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

func apply(t *testing.T, driver, dsn, engine string) {
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
