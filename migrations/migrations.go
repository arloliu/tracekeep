// Package migrations embeds the catalog's Atlas migrations, one directory per engine (docs/specs/tracekeep/catalog.md §7.4).
// Deployments apply them with the Atlas CLI; tests apply them with Apply.
// They are generated from schema.hcl by scripts/catalog.sh and never edited by hand,
// except the counters' seed, a MariaDB spelling Atlas gets wrong and a backfill of a new column, each marked in its file.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"
)

//go:embed sqlite/*.sql mariadb/*.sql
var files embed.FS

// Engines are the catalog's engines, each a directory of migrations.
var Engines = []string{"sqlite", "mariadb"}

// Apply runs every migration of engine on db in order, as the Atlas CLI would, without its revision table;
// a MariaDB connection must allow multiple statements per call.
func Apply(ctx context.Context, db *sql.DB, engine string) error {
	return applySelected(ctx, db, engine, func(int, string) bool { return true })
}

// applySelected runs, in order, the migrations of engine that run selects by their index and name.
func applySelected(ctx context.Context, db *sql.DB, engine string, run func(i int, name string) bool) error {
	names, err := fs.Glob(files, path.Join(engine, "*.sql"))
	if err != nil || len(names) == 0 {
		return fmt.Errorf("migrations: no migrations for %q", engine)
	}
	slices.Sort(names) // the version prefix orders them
	for i, name := range names {
		if !run(i, name) {
			continue
		}
		body, err := files.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("migrations: %s: %w", name, err)
		}
	}

	return nil
}

// Latest names the newest embedded migration of engine, the schema version this binary was built for.
func Latest(engine string) (string, error) {
	names, err := fs.Glob(files, path.Join(engine, "*.sql"))
	if err != nil || len(names) == 0 {
		return "", fmt.Errorf("migrations: no migrations for %q", engine)
	}
	slices.Sort(names)

	return strings.TrimSuffix(path.Base(names[len(names)-1]), ".sql"), nil
}
