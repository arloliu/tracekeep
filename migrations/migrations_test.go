package migrations_test

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
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

func open(t *testing.T, engine string) *sql.DB {
	t.Helper()
	driver, dsn := "sqlite", "file:"+t.TempDir()+"/catalog.db"
	if engine == "mariadb" {
		dsn = os.Getenv("TRACEKEEP_MARIADB_DSN")
		if dsn == "" {
			t.Skip("TRACEKEEP_MARIADB_DSN not set: no MariaDB to test against")
		}
		driver, dsn = "mysql", dsn+"?multiStatements=true"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}
