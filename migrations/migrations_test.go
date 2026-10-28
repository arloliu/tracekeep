package migrations_test

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	"ariga.io/atlas/sql/migrate"
	"github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"

	"github.com/arloliu/tracekeep/migrations"
)

// engines runs f once per engine on a fresh, empty database.
func engines(t *testing.T, f func(t *testing.T, db *sql.DB, engine string)) {
	t.Helper()
	for _, engine := range migrations.Engines {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			f(t, open(t, engine), engine)
		})
	}
}

func open(t *testing.T, engine string) *sql.DB {
	t.Helper()
	driver, dsn := "sqlite", "file:"+t.TempDir()+"/catalog.db"
	if engine == "mariadb" {
		root := os.Getenv("TRACEKEEP_MARIADB_DSN")
		if root == "" {
			t.Skip("TRACEKEEP_MARIADB_DSN not set: no MariaDB to test against")
		}
		mc, err := mysql.ParseDSN(root)
		if err != nil {
			t.Fatal(err)
		}
		name := "tkm_" + strings.ToLower(rand.Text())
		admin, err := sql.Open("mysql", mc.FormatDSN())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE "+name); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = admin.ExecContext(t.Context(), "DROP DATABASE "+name)
			_ = admin.Close()
		})
		mc.DBName = name
		driver, dsn = "mysql", mc.FormatDSN()
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

type revision struct {
	version        string
	applied, total int
	err, errStmt   string
}

func revisions(t *testing.T, db *sql.DB) []revision {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT version, applied, total, error, error_stmt FROM "+migrations.Table+" ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []revision
	for rows.Next() {
		var r revision
		if err := rows.Scan(&r.version, &r.applied, &r.total, &r.err, &r.errStmt); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	return out
}

// files is engine's embedded migrations, the sum file left out.
func files(t *testing.T, engine string) []migrate.File {
	t.Helper()
	dir, err := migrations.Dir(engine)
	if err != nil {
		t.Fatal(err)
	}
	fs, err := dir.Files()
	if err != nil {
		t.Fatal(err)
	}

	return fs
}

// dirOf is a directory of fs with their sum recomputed.
func dirOf(t *testing.T, fs []migrate.File) *migrate.MemDir {
	t.Helper()
	d := &migrate.MemDir{}
	if err := d.CopyFiles(fs); err != nil {
		t.Fatal(err)
	}

	return d
}

func count(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), query).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}

	return n
}

// A fresh catalog gets every migration, each recorded whole in the service's revision table, and a second run has nothing to do.
func TestApply(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, db *sql.DB, engine string) {
		if err := migrations.Apply(t.Context(), db, engine); err != nil {
			t.Fatal(err)
		}
		if n := count(t, db, "SELECT COUNT(*) FROM counters WHERE name IN ('scope_events', 'membership')"); n != 2 {
			t.Errorf("seeded counters = %d, want 2", n)
		}
		revs := revisions(t, db)
		if len(revs) != len(files(t, engine)) {
			t.Fatalf("revisions %+v, want one per migration", revs)
		}
		for _, r := range revs {
			if r.applied != r.total || r.err != "" {
				t.Errorf("revision %+v, want applied whole", r)
			}
		}
		if err := migrations.Apply(t.Context(), db, engine); err != nil {
			t.Errorf("a second Apply = %v", err)
		}
		if again := revisions(t, db); len(again) != len(revs) {
			t.Errorf("revisions after a second Apply = %+v", again)
		}
	})
}

// A catalog at an earlier release's migrations is upgraded to the rest.
func TestUpgrade(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, db *sql.DB, engine string) {
		all := files(t, engine)
		if err := migrations.ApplyDir(t.Context(), db, engine, dirOf(t, all[:2])); err != nil {
			t.Fatal(err)
		}
		if n := len(revisions(t, db)); n != 2 {
			t.Fatalf("%d revisions after the first release, want 2", n)
		}
		if err := migrations.Apply(t.Context(), db, engine); err != nil {
			t.Fatal(err)
		}
		if n := len(revisions(t, db)); n != len(all) {
			t.Errorf("%d revisions after the upgrade, want %d", n, len(all))
		}
		count(t, db, "SELECT COUNT(*) FROM compact_requests") // a later migration's table
	})
}

// A migration whose statement fails is recorded with the statements it applied, and once the cause is gone
// the next run resumes from the failed statement, never re-running the ones applied.
func TestFailedApplicationResumes(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, db *sql.DB, engine string) {
		fail := migrate.NewLocalFile("20991231000000_fail.sql", []byte(
			"CREATE TABLE t_first (a INTEGER);\nINSERT INTO t_missing (a) VALUES (1);\nCREATE TABLE t_after (a INTEGER);\n"))
		dir := dirOf(t, append(files(t, engine), fail))
		err := migrations.ApplyDir(t.Context(), db, engine, dir)
		if err == nil {
			t.Fatal("ApplyDir with a failing statement succeeded")
		}
		revs := revisions(t, db)
		last := revs[len(revs)-1]
		if last.version != "20991231000000" || last.applied != 1 || last.total != 3 || last.err == "" || !strings.Contains(last.errStmt, "t_missing") {
			t.Fatalf("the failed revision = %+v, want one statement applied and the failed one named", last)
		}
		if _, err := db.ExecContext(t.Context(), "CREATE TABLE t_missing (a INTEGER)"); err != nil {
			t.Fatal(err)
		}
		if err := migrations.ApplyDir(t.Context(), db, engine, dir); err != nil {
			t.Fatalf("ApplyDir once the cause is gone = %v, want the migration resumed (t_first is not created again)", err)
		}
		if last := revisions(t, db)[len(revs)-1]; last.applied != 3 || last.err != "" {
			t.Errorf("the resumed revision = %+v, want applied whole", last)
		}
		count(t, db, "SELECT COUNT(*) FROM t_after")
	})
}

// A catalog holding tables but no revision of the service's own was migrated by another tool, and is refused untouched.
func TestCatalogOfAnotherToolRefused(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, db *sql.DB, engine string) {
		if _, err := db.ExecContext(t.Context(), "CREATE TABLE packs (a INTEGER)"); err != nil {
			t.Fatal(err)
		}
		if err := migrations.Apply(t.Context(), db, engine); !errors.Is(err, migrations.ErrOtherTool) {
			t.Errorf("Apply over another tool's catalog = %v, want ErrOtherTool", err)
		}
		if n := len(revisions(t, db)); n != 0 {
			t.Errorf("%d revisions written over another tool's catalog", n)
		}
	})
}

// A directory whose files no longer match their atlas.sum is refused before any statement runs.
func TestChangedSumRefused(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, db *sql.DB, engine string) {
		dir, err := migrations.Dir(engine)
		if err != nil {
			t.Fatal(err)
		}
		first := files(t, engine)[0]
		if err := dir.WriteFile(first.Name(), append(first.Bytes(), []byte("\n-- edited\n")...)); err != nil {
			t.Fatal(err)
		}
		if err := migrations.ApplyDir(t.Context(), db, engine, dir); !errors.Is(err, migrate.ErrChecksumMismatch) {
			t.Errorf("ApplyDir of an edited migration = %v, want a checksum mismatch", err)
		}
		if n := len(revisions(t, db)); n != 0 {
			t.Errorf("%d revisions written from an edited directory", n)
		}
	})
}
