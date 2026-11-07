package migrations_test

import (
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"ariga.io/atlas/sql/migrate"

	"github.com/arloliu/tracekeep/migrations"
)

// The identity reset is refused on a catalog at the previous head that holds a capture of any state, before Atlas runs,
// leaving the schema, the rows and every revision as they were; on one without captures it applies,
// and a later run on the populated new catalog applies nothing (catalog.md §7.4).
func TestIdentityResetRefusedOnACatalogWithCaptures(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, db *sql.DB, engine string) {
		all := files(t, engine)
		reset := slices.IndexFunc(all, func(f migrate.File) bool { return strings.HasSuffix(f.Name(), "_equipment_identity.sql") })
		if reset < 0 {
			t.Fatal("no identity reset among the migrations")
		}
		if err := migrations.ApplyDir(t.Context(), db, engine, dirOf(t, all[:reset])); err != nil {
			t.Fatal(err)
		}
		before := revisions(t, db)
		for _, state := range []string{"pending", "open", "rejected"} {
			_, err := db.ExecContext(t.Context(), "INSERT INTO captures (capture_id, state, tool_id, created_at) VALUES (?, ?, 'T1', 1)",
				[]byte((state + "-0123456789abcdef")[:16]), state)
			if err != nil {
				t.Fatal(err)
			}
			if err := migrations.Apply(t.Context(), db, engine); !errors.Is(err, migrations.ErrIdentityReset) {
				t.Fatalf("Apply over a %s capture = %v, want ErrIdentityReset", state, err)
			}
			if after := revisions(t, db); !reflect.DeepEqual(after, before) {
				t.Fatalf("revisions after the refusal = %+v, want %+v", after, before)
			}
			if n := count(t, db, "SELECT COUNT(*) FROM captures WHERE tool_id = 'T1'"); n != 1 {
				t.Fatalf("%d captures under tool_id after the refusal, want the one inserted", n)
			}
			if _, err := db.ExecContext(t.Context(), "DELETE FROM captures"); err != nil {
				t.Fatal(err)
			}
		}

		if err := migrations.Apply(t.Context(), db, engine); err != nil {
			t.Fatalf("Apply without captures = %v", err)
		}
		_, err := db.ExecContext(t.Context(), "INSERT INTO captures (capture_id, state, equipment_id, device, source, created_at) VALUES (?, 'open', 'T1', 'eqp', 's', 1)",
			[]byte("new-0123456789ab"))
		if err != nil {
			t.Fatal(err)
		}
		count(t, db, "SELECT COUNT(*) FROM packs WHERE equipment_id = 'T1'")
		n := len(revisions(t, db))
		if err := migrations.Apply(t.Context(), db, engine); err != nil {
			t.Fatalf("Apply on the populated new catalog = %v, want nothing to apply", err)
		}
		if got := len(revisions(t, db)); got != n {
			t.Errorf("%d revisions after a second run, want %d", got, n)
		}
	})
}
