package migrationstest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"ariga.io/atlas/sql/migrate"
	_ "modernc.org/sqlite" // the sqlite driver

	"github.com/arloliu/tracekeep/migrations"
)

// template is the process's template: the bytes of a SQLite file its embedded migrations migrated, built at the first request;
// an error is every request's.
var template = newTemplate(func() ([]byte, error) {
	dir, err := migrations.Dir("sqlite")
	if err != nil {
		return nil, err
	}

	return build(context.Background(), dir)
})

// SQLite writes a migrated SQLite catalog to path, a copy of the template; a template that cannot be built fails tb.
func SQLite(tb testing.TB, path string) {
	tb.Helper()
	copyTo(tb, path, template)
}

// URI is the SQLite URI of the database file at path, its path escaped, as catalog.SQLiteURI writes it.
func URI(path string) string {
	return "file:" + (&url.URL{Path: path}).EscapedPath()
}

// newTemplate returns a template that runs build at its first request only, however many requests arrive at once.
func newTemplate(build func() ([]byte, error)) func() ([]byte, error) {
	return sync.OnceValues(build)
}

func copyTo(tb testing.TB, path string, template func() ([]byte, error)) {
	tb.Helper()
	b, err := template()
	if err != nil {
		tb.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		tb.Fatal(err)
	}
}

// build migrates a new file with dir in a temporary directory, which it removes, and returns the file's bytes.
// The file is left in rollback-journal mode with no -wal or -shm beside it, so its bytes are the whole database.
func build(ctx context.Context, dir migrate.Dir) ([]byte, error) {
	tmp, err := os.MkdirTemp("", "tracekeep-template-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	path := filepath.Join(tmp, "catalog.db")
	db, err := sql.Open("sqlite", URI(path))
	if err != nil {
		return nil, err
	}
	err = migrations.ApplyDir(ctx, db, "sqlite", dir)
	if err == nil {
		_, err = db.ExecContext(ctx, "PRAGMA journal_mode=DELETE")
	}
	if err = errors.Join(err, db.Close()); err != nil {
		return nil, fmt.Errorf("migrationstest: the SQLite template: %w", err)
	}
	for _, side := range []string{"-wal", "-shm", "-journal"} {
		switch _, err := os.Stat(path + side); {
		case err == nil:
			return nil, fmt.Errorf("migrationstest: the SQLite template left %s beside it", side)
		case !errors.Is(err, os.ErrNotExist):
			return nil, fmt.Errorf("migrationstest: the SQLite template's %s: %w", side, err)
		}
	}

	return os.ReadFile(path)
}
