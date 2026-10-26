package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/migrations"
)

// envMariaDB names a MariaDB DSN; make test-catalog starts a server and sets it, and without it the MariaDB runs skip.
const envMariaDB = "TRACEKEEP_MARIADB_DSN"

// engines runs f once per engine, each on a fresh, migrated catalog (.agents/rules/300-testing.md *Catalog*).
func engines(t *testing.T, f func(t *testing.T, c *Catalog)) {
	t.Helper()
	for _, engine := range []string{EngineSQLite, EngineMariaDB} {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			f(t, newCatalog(t, engine))
		})
	}
}

func newCatalog(t *testing.T, engine string) *Catalog {
	t.Helper()
	cfg := Config{Engine: engine}
	switch engine {
	case EngineSQLite:
		cfg.DSN = filepath.Join(t.TempDir(), "catalog.db")
		migrate(t, "sqlite", SQLiteURI(cfg.DSN), engine)
	case EngineMariaDB:
		cfg.DSN = mariadbDatabase(t)
	default:
		t.Fatalf("unknown engine %q", engine)
	}
	c, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	return c
}

// mariadbDatabase creates a database of the test's own on the configured server, migrates it and returns its DSN.
func mariadbDatabase(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(envMariaDB)
	if dsn == "" {
		t.Skipf("%s not set: no MariaDB to test against", envMariaDB)
	}
	mc, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("mysql", mc.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := "tk_" + strings.ToLower(rand.Text())
	if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		admin, err := sql.Open("mysql", mc.FormatDSN())
		if err == nil {
			_, _ = admin.ExecContext(context.Background(), "DROP DATABASE "+name)
			_ = admin.Close()
		}
	})
	mc.DBName = name
	m := mc.Clone()
	m.MultiStatements = true
	migrate(t, "mysql", m.FormatDSN(), EngineMariaDB)

	return mc.FormatDSN()
}

func migrate(t *testing.T, driver, dsn, engine string) {
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

func newID(t *testing.T) tracepack.UUID {
	t.Helper()
	var id tracepack.UUID
	_, _ = rand.Read(id[:])

	return id
}
