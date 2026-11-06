package migrationstest

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"ariga.io/atlas/sql/migrate"

	"github.com/arloliu/tracekeep/migrations"
)

// A copy of the template is the catalog a fresh migration makes: the same schema and rows, the revisions' timing aside,
// and migrations.Apply finds nothing to apply on it.
func TestCopyIsAFreshMigration(t *testing.T) {
	t.Parallel()
	fresh := open(t, filepath.Join(t.TempDir(), "fresh.db"))
	if err := migrations.Apply(t.Context(), fresh, "sqlite"); err != nil {
		t.Fatal(err)
	}
	copied := filepath.Join(t.TempDir(), "copy.db")
	SQLite(t, copied)
	db := open(t, copied)
	if err := same(t, fresh, db); err != nil {
		t.Fatal(err)
	}
	before, revs := snapshot(t, db), rowsOf(t, db, "SELECT * FROM "+migrations.Table)
	if len(revs) == 0 {
		t.Fatal("the copy holds no revision")
	}
	if err := migrations.Apply(t.Context(), db, "sqlite"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot(t, db), before) || !reflect.DeepEqual(rowsOf(t, db, "SELECT * FROM "+migrations.Table), revs) {
		t.Fatal("Apply on a copy changed it")
	}
}

// The comparison the copy passes rejects each kind of difference a template could carry.
func TestSameRejectsADifference(t *testing.T) {
	t.Parallel()
	changes := map[string]string{
		"an extra table":            "CREATE TABLE extra (a INTEGER)",
		"a missing index":           "DROP INDEX " + anIndex(t),
		"a changed counter":         "UPDATE counters SET value = value + 1",
		"a changed revision hash":   "UPDATE " + migrations.Table + " SET hash = 'x' WHERE version = (SELECT MIN(version) FROM " + migrations.Table + ")",
		"an extra revision":         "INSERT INTO " + migrations.Table + " SELECT 'z', description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version FROM " + migrations.Table + " LIMIT 1",
		"a changed lease":           "UPDATE leases SET fence_token = 7",
		"an extra row of any table": "INSERT INTO counters (name) VALUES ('extra')",
	}
	for name, stmt := range changes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a, b := filepath.Join(t.TempDir(), "a.db"), filepath.Join(t.TempDir(), "b.db")
			SQLite(t, a)
			SQLite(t, b)
			db := open(t, b)
			if _, err := db.ExecContext(t.Context(), stmt); err != nil {
				t.Fatal(err)
			}
			if err := same(t, open(t, a), db); err == nil {
				t.Fatalf("%s passed the comparison", name)
			}
		})
	}
}

// Copies taken in parallel, and one taken after another test's copy changed and that test ended, are independent of each other and of the template.
func TestCopiesAreIndependent(t *testing.T) { //nolint:tparallel // the late copy is taken after the subtest and its cleanup end
	t.Parallel()
	before, err := template()
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 4)
	t.Run("changed", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := range paths {
			paths[i] = filepath.Join(t.TempDir(), fmt.Sprintf("c%d.db", i))
			wg.Go(func() { SQLite(t, paths[i]) })
		}
		wg.Wait()
		if _, err := open(t, paths[0]).ExecContext(t.Context(), "INSERT INTO counters (name) VALUES ('mine')"); err != nil {
			t.Fatal(err)
		}
		for _, p := range paths[1:] {
			if n := mine(t, p); n != 0 {
				t.Fatalf("%s holds another copy's row", filepath.Base(p))
			}
		}
	})
	late := filepath.Join(t.TempDir(), "late.db")
	SQLite(t, late)
	if n := mine(t, late); n != 0 {
		t.Fatal("a copy taken after another test ended holds its row")
	}
	after, err := template()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("the template changed under a copy's write")
	}
}

// Requests arriving at once build the template once, and a failed build fails every request without a copy.
func TestTemplateBuildsOnce(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail=%v", fail), func(t *testing.T) {
			t.Parallel()
			var builds atomic.Int32
			start := make(chan struct{})
			tmpl := newTemplate(func() ([]byte, error) {
				builds.Add(1)
				if fail {
					return nil, errors.New("broken")
				}
				return template()
			})
			dir := t.TempDir()
			failed := make([]bool, 16)
			var wg sync.WaitGroup
			for i := range failed {
				wg.Go(func() {
					<-start
					rec := &fatalTB{TB: t}
					done := make(chan struct{})
					go func() {
						defer close(done)
						copyTo(rec, filepath.Join(dir, fmt.Sprintf("c%d.db", i)), tmpl)
					}()
					<-done
					failed[i] = rec.failed
				})
			}
			close(start)
			wg.Wait()
			if n := builds.Load(); n != 1 {
				t.Fatalf("%d builds, want 1", n)
			}
			for i, f := range failed {
				if f != fail {
					t.Fatalf("request %d failed = %v, want %v", i, f, fail)
				}
			}
			if left, _ := os.ReadDir(dir); fail && len(left) != 0 {
				t.Fatalf("a failed template left copies %v", left)
			}
		})
	}
}

// A template whose migrations fail returns an error and removes its temporary directory.
func TestBuildFailureLeavesNothing(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	broken := &migrate.MemDir{}
	if err := broken.WriteFile("1_bad.sql", []byte("CREATE TABLE;")); err != nil {
		t.Fatal(err)
	}
	sum, err := broken.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	if err := migrate.WriteSumFile(broken, sum); err != nil {
		t.Fatal(err)
	}
	if _, err := build(t.Context(), broken); err == nil {
		t.Fatal("a failing migration built a template")
	}
	if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
		t.Fatalf("a failed build left %v (%v)", left, err)
	}
}

// fatalTB records a Fatal and ends the goroutine, as testing.T does, so a test can watch another test's failure.
type fatalTB struct {
	testing.TB
	failed bool
}

func (f *fatalTB) Helper() {}

func (f *fatalTB) Fatal(...any) {
	f.failed = true
	runtime.Goexit()
}

func mine(t *testing.T, path string) int {
	t.Helper()
	var n int
	if err := open(t, path).QueryRowContext(t.Context(), "SELECT COUNT(*) FROM counters WHERE name = 'mine'").Scan(&n); err != nil {
		t.Fatal(err)
	}

	return n
}

func open(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", URI(path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func anIndex(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "i.db")
	SQLite(t, p)
	var name string
	err := open(t, p).QueryRow("SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL ORDER BY name LIMIT 1").Scan(&name)
	if err != nil {
		t.Fatal(err)
	}

	return name
}

// snapshot is a database's schema, one entry per sqlite_master object, and its rows, one entry per table,
// the revision table's timing columns left out, since two migrations never run at the same instant.
func snapshot(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	var tables []string
	for _, row := range rowsOf(t, db, "SELECT type, name, COALESCE(sql, '') FROM sqlite_master") {
		out["schema"] = append(out["schema"], row)
		if typ, name, _ := strings.Cut(row, " "); typ == "table" {
			tables = append(tables, strings.SplitN(name, " ", 2)[0])
		}
	}
	for _, table := range tables {
		cols := "*"
		if table == migrations.Table {
			cols = "version, description, type, applied, total, error, error_stmt, hash, partial_hashes, operator_version"
		}
		out["table "+table] = rowsOf(t, db, "SELECT "+cols+" FROM "+table)
	}

	return out
}

func rowsOf(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintln(vals...))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)

	return out
}

func same(t *testing.T, a, b *sql.DB) error {
	t.Helper()
	sa, sb := snapshot(t, a), snapshot(t, b)
	if reflect.DeepEqual(sa, sb) {
		return nil
	}
	var diff []string
	for k := range sa {
		if !reflect.DeepEqual(sa[k], sb[k]) {
			diff = append(diff, k)
		}
	}
	for k := range sb {
		if _, ok := sa[k]; !ok {
			diff = append(diff, k)
		}
	}

	return fmt.Errorf("the databases differ in %s", strings.Join(diff, ", "))
}
