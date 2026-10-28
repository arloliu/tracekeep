// Package migrations embeds the catalog's Atlas migrations, one directory per engine with its atlas.sum (docs/specs/tracekeep/catalog.md §7.4),
// and applies them with Atlas's executor, recording each revision in the service's own table, so a deployment needs no Atlas binary.
// They are generated from schema.hcl by scripts/catalog.sh and never edited by hand,
// except the counters' seed and a MariaDB spelling Atlas gets wrong, each marked in its file.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/mysql"
	"ariga.io/atlas/sql/sqlite"
)

//go:embed sqlite/*.sql sqlite/atlas.sum mariadb/*.sql mariadb/atlas.sum
var files embed.FS

// Engines are the catalog's engines, each a directory of migrations.
var Engines = []string{"sqlite", "mariadb"}

// Table is the service's revision table, beside which a catalog holds nothing it did not migrate.
const Table = "tracekeep_revisions"

// ErrOtherTool marks a catalog that holds tables but no revision in Table: another tool migrated it, and it is refused.
var ErrOtherTool = errors.New("migrations: the catalog holds tables but no tracekeep revision: another tool migrated it")

// Dir is engine's embedded migration directory, its atlas.sum included.
func Dir(engine string) (*migrate.MemDir, error) {
	names, err := fs.Glob(files, path.Join(engine, "*"))
	if err != nil || len(names) == 0 {
		return nil, fmt.Errorf("migrations: no migrations for %q", engine)
	}
	dir := &migrate.MemDir{}
	for _, name := range names {
		b, err := files.ReadFile(name)
		if err != nil {
			return nil, err
		}
		if err := dir.WriteFile(path.Base(name), b); err != nil {
			return nil, err
		}
	}

	return dir, nil
}

// Apply applies engine's pending embedded migrations to db (catalog.md §7.4).
func Apply(ctx context.Context, db *sql.DB, engine string) error {
	dir, err := Dir(engine)
	if err != nil {
		return err
	}

	return ApplyDir(ctx, db, engine, dir)
}

// ApplyDir applies dir's pending migrations to db with Atlas's executor, statement by statement:
// dir's atlas.sum must match its files, each revision is recorded in Table, a migration that failed resumes from its failed statement,
// and a catalog another tool migrated is refused.
func ApplyDir(ctx context.Context, db *sql.DB, engine string, dir migrate.Dir) error {
	var drv migrate.Driver
	var err error
	switch engine {
	case "sqlite":
		drv, err = sqlite.Open(db)
	case "mariadb":
		drv, err = mysql.Open(db)
	default:
		err = fmt.Errorf("unknown engine %q", engine)
	}
	if err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	if _, err := db.ExecContext(ctx, revisionTable(engine)); err != nil {
		return fmt.Errorf("migrations: %s: %w", Table, err)
	}
	ex, err := migrate.NewExecutor(drv, dir, &revisions{db: db})
	if err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	err = ex.ExecuteN(ctx, 0)
	var dirty *migrate.NotCleanError
	switch {
	case err == nil, errors.Is(err, migrate.ErrNoPendingFiles):
		return nil
	case errors.As(err, &dirty):
		return fmt.Errorf("%w (%s)", ErrOtherTool, dirty.Reason)
	default:
		return fmt.Errorf("migrations: %w", err)
	}
}

// revisionTable creates Table when the catalog has none.
func revisionTable(engine string) string {
	text, key := "TEXT", "VARCHAR(255)"
	if engine == "mariadb" {
		text = "LONGTEXT"
	}

	return `CREATE TABLE IF NOT EXISTS ` + Table + ` (
		version ` + key + ` NOT NULL PRIMARY KEY,
		description ` + text + ` NOT NULL,
		type INTEGER NOT NULL,
		applied INTEGER NOT NULL,
		total INTEGER NOT NULL,
		executed_at BIGINT NOT NULL,
		execution_time BIGINT NOT NULL,
		error ` + text + ` NOT NULL,
		error_stmt ` + text + ` NOT NULL,
		hash ` + key + ` NOT NULL,
		partial_hashes ` + text + ` NOT NULL,
		operator_version ` + key + ` NOT NULL
	)`
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
