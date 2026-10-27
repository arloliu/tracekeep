package migrations_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"

	"github.com/arloliu/tracekeep/migrations"
)

// Both engines' migrations apply to an empty database and seed the counters.
func TestApply(t *testing.T) {
	t.Parallel()
	for _, engine := range migrations.Engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			db := open(t, engine)
			if err := migrations.Apply(t.Context(), db, engine); err != nil {
				t.Fatal(err)
			}
			var n int
			if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM counters WHERE name IN ('scope_events', 'membership')").Scan(&n); err != nil || n != 2 {
				t.Errorf("seeded counters = %d, %v; want 2", n, err)
			}
		})
	}
}

// An upgrade over a catalog holding merges gives each the default D's deletion time, never an expired one.
func TestRetentionUpgradeBackfillsDeleteAt(t *testing.T) {
	t.Parallel()
	for _, engine := range migrations.Engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			db := open(t, engine)
			if err := migrations.ApplySplit(t.Context(), db, engine, "close_intent", true); err != nil {
				t.Fatal(err)
			}
			_, err := db.ExecContext(t.Context(), `INSERT INTO merges (merge_id, capture_id, hour, output_pack_id, outcome, at)
				VALUES (X'01', X'02', 1, X'03', 'committed', 1000)`)
			if err != nil {
				t.Fatal(err)
			}
			if err := migrations.ApplySplit(t.Context(), db, engine, "close_intent", false); err != nil {
				t.Fatal(err)
			}
			var at int64
			if err := db.QueryRowContext(t.Context(), `SELECT delete_at FROM merges`).Scan(&at); err != nil || at != 1000+3600000000000 {
				t.Errorf("delete_at = %d, %v; want the merge's time plus an hour", at, err)
			}
		})
	}
}

func open(t *testing.T, engine string) *sql.DB {
	t.Helper()
	driver, dsn := "sqlite", "file:"+t.TempDir()+"/catalog.db"
	if engine == "mariadb" {
		dsn = os.Getenv("TRACEKEEP_MARIADB_DSN")
		if dsn == "" {
			t.Skip("TRACEKEEP_MARIADB_DSN not set: no MariaDB to test against")
		}
		driver, dsn = "mysql", database(t, dsn)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

// database creates a database of the test's own on the server dsn names and returns its DSN, allowing multiple statements.
func database(t *testing.T, dsn string) string {
	t.Helper()
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
		if db, err := sql.Open("mysql", mc.FormatDSN()); err == nil {
			_, _ = db.ExecContext(context.Background(), "DROP DATABASE "+name)
			_ = db.Close()
		}
	})
	mc.DBName, mc.MultiStatements = name, true

	return mc.FormatDSN()
}
