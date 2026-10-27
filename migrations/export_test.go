package migrations

import (
	"context"
	"database/sql"
	"strings"
)

// ApplySplit runs engine's migrations through the change named change when before is set, and those after it otherwise:
// the two halves of an upgrade across the migrations after change.
func ApplySplit(ctx context.Context, db *sql.DB, engine, change string, before bool) error {
	at := -1

	return applySelected(ctx, db, engine, func(i int, name string) bool {
		if strings.HasSuffix(name, "_"+change+".sql") {
			at = i

			return before
		}

		return (at < 0) == before
	})
}
