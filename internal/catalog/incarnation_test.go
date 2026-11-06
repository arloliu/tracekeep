package catalog

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	atlas "ariga.io/atlas/sql/migrate"
	"github.com/go-sql-driver/mysql"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/migrations"
)

func TestIncarnate(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		first, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 100)
		if err != nil || first.ID != 1 {
			t.Fatalf("first Incarnate = %+v, %v; want incarnation 1", first, err)
		}
		if again, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 100); err != nil || again.ID != first.ID {
			t.Errorf("Incarnate of the same stream = %+v, %v; want incarnation 1", again, err)
		}
		if _, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 200); !errors.Is(err, ErrIncarnationChanged) {
			t.Errorf("Incarnate of a recreated stream = %v, want ErrIncarnationChanged", err)
		}
		if p, err := c.Incarnate(ctx, RoleParking, "PARKED", 300); err != nil || p.ID != 2 {
			t.Errorf("Incarnate of the parking stream = %+v, %v; want incarnation 2", p, err)
		}
	})
}

// The highest last sequence observed is kept: a lower observation may be one another replica overtook.
func TestObserveLastSeqKeepsTheHighest(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		inc, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, seq := range []int64{10, 10, 12, 11} {
			if err := c.ObserveLastSeq(ctx, inc.ID, seq); err != nil {
				t.Fatalf("ObserveLastSeq(%d): %v", seq, err)
			}
		}
		if got, err := c.Incarnate(ctx, RoleTraffic, "TRAFFIC", 100); err != nil || got.LastSeqSeen != 12 {
			t.Errorf("Incarnate = %+v, %v; want 12 observed", got, err)
		}
	})
}

// liveRows reads the live incarnations of role.
func liveRows(t *testing.T, c *Catalog, role string) []Incarnation {
	t.Helper()
	var out []Incarnation
	err := c.read.SelectContext(t.Context(), &out, `SELECT incarnation_id, role, stream_name, created_ns, last_seq_seen FROM bus_streams
		WHERE role = ? AND retired_at IS NULL`, role)
	if err != nil {
		t.Fatal(err)
	}

	return out
}

const insertIncarnation = `INSERT INTO bus_streams (incarnation_id, role, stream_name, created_ns, last_seq_seen, retired_at) VALUES (?, ?, ?, ?, 0, ?)`

// The catalog itself refuses a second live incarnation of a role; a retired one is no obstacle.
func TestOneLiveIncarnationPerRole(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		exec(t, c, insertIncarnation, 1, RoleTraffic, "TRAFFIC", 100, 50)
		exec(t, c, insertIncarnation, 2, RoleTraffic, "TRAFFIC", 200, nil)
		if _, err := c.write.ExecContext(t.Context(), insertIncarnation, 3, RoleTraffic, "TRAFFIC", 300, nil); err == nil {
			t.Error("a second live incarnation of the traffic role was recorded")
		}
		exec(t, c, insertIncarnation, 4, RoleParking, "PARKED", 400, nil)
	})
}

// Replicas of both roles starting together record one live incarnation per role, every caller of a stream getting its id,
// and so one consumer name (tk-ingest-<id>).
func TestIncarnateTogether(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		const n = 8
		ids := make([]int64, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				role, stream := RoleTraffic, "TRAFFIC"
				if i%2 == 1 {
					role, stream = RoleParking, "PARKED"
				}
				inc, err := c.Incarnate(t.Context(), role, stream, 100)
				if err != nil {
					t.Error(err)
				}
				ids[i] = inc.ID
			})
		}
		wg.Wait()
		for i := range ids {
			if ids[i] != ids[i%2] {
				t.Errorf("incarnation ids %v, want one per role", ids)
			}
		}
		if ids[0] == ids[1] {
			t.Errorf("both roles got incarnation %d", ids[0])
		}
		for _, role := range []string{RoleTraffic, RoleParking} {
			if live := liveRows(t, c, role); len(live) != 1 {
				t.Errorf("live %s incarnations %+v, want one", role, live)
			}
		}
	})
}

// A creation that loses a race to another replica reads the row the winner committed, wherever the winner commits:
// between the loser's check and its id, or between its id and its insert, the second also colliding on the id
// when the winner plays the other role. A winner of another stream is a changed incarnation, never adopted.
// SQLite's one writer serializes the transactions, so only MariaDB interleaves them.
func TestIncarnateLosesARace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		hook   int
		role   string
		stream string
	}{
		{"winner between check and id", hookIncarnationChecked, RoleTraffic, "TRAFFIC"},
		{"winner between id and insert", hookIncarnationAllocated, RoleTraffic, "TRAFFIC"},
		{"other role on the same id", hookIncarnationAllocated, RoleParking, "PARKED"},
		{"other stream", hookIncarnationChecked, RoleTraffic, "TRAFFIC-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				if c.Engine() == EngineSQLite {
					t.Skip("SQLite's one writer serializes the transactions")
				}
				held, release := holdAt(c, tc.hook)
				type result struct {
					inc Incarnation
					err error
				}
				done := make(chan result, 1)
				go func() {
					inc, err := c.Incarnate(t.Context(), RoleTraffic, "TRAFFIC", 100)
					done <- result{inc, err}
				}()
				<-held
				winner, err := c.Incarnate(t.Context(), tc.role, tc.stream, 100)
				close(release)
				if err != nil {
					t.Fatal(err)
				}
				loser := <-done
				switch {
				case tc.stream == "TRAFFIC-2":
					if !errors.Is(loser.err, ErrIncarnationChanged) {
						t.Errorf("the loser of another stream = %+v, %v; want ErrIncarnationChanged", loser.inc, loser.err)
					}
				case loser.err != nil:
					t.Errorf("the loser = %v", loser.err)
				case tc.role == RoleTraffic && loser.inc.ID != winner.ID:
					t.Errorf("the loser got incarnation %d, the winner %d: two consumer names", loser.inc.ID, winner.ID)
				case tc.role != RoleTraffic && loser.inc.ID == winner.ID:
					t.Errorf("both roles got incarnation %d", winner.ID)
				default: // the loser read the winner's row, or recorded its own role
				}
				for _, role := range []string{RoleTraffic, tc.role} {
					if live := liveRows(t, c, role); len(live) != 1 {
						t.Errorf("live %s incarnations %+v, want one", role, live)
					}
				}
			})
		})
	}
}

// migratedBefore returns a catalog on engine migrated up to, not including, the migration adding the live role's key,
// and a function applying the rest.
func migratedBefore(t *testing.T, engine string) (*Catalog, func() error) {
	t.Helper()
	cfg := Config{Engine: engine}
	var driver, dsn string
	switch engine {
	case EngineSQLite:
		cfg.DSN = filepath.Join(t.TempDir(), "catalog.db")
		driver, dsn = "sqlite", SQLiteURI(cfg.DSN)
	default:
		cfg.DSN = mariadbEmpty(t)
		mc, err := mysql.ParseDSN(cfg.DSN)
		if err != nil {
			t.Fatal(err)
		}
		mc.MultiStatements = true
		driver, dsn = "mysql", mc.FormatDSN()
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	all, err := migrations.Dir(engine)
	if err != nil {
		t.Fatal(err)
	}
	files, err := all.Files()
	if err != nil {
		t.Fatal(err)
	}
	live := slices.IndexFunc(files, func(f atlas.File) bool { return strings.HasSuffix(f.Name(), "_live_incarnation.sql") })
	if live < 0 {
		t.Fatal("no live incarnation migration")
	}
	older := &atlas.MemDir{}
	if err := older.CopyFiles(files[:live]); err != nil {
		t.Fatal(err)
	}
	if err := migrations.ApplyDir(t.Context(), db, engine, older); err != nil {
		t.Fatal(err)
	}
	c, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	return c, func() error { return migrations.Apply(context.WithoutCancel(t.Context()), db, engine) }
}

// The migration adding the live role's key passes over retired rows; over two live rows of a role it fails naming the role,
// and once the operator adopts the stream, which retires both, it resumes and the roles start.
func TestLiveRoleMigration(t *testing.T) {
	t.Parallel()
	for _, engine := range benchscale.Engines(t, EngineSQLite, EngineMariaDB) {
		t.Run(engine+"/retired rows", func(t *testing.T) {
			t.Parallel()
			c, rest := migratedBefore(t, engine)
			exec(t, c, insertIncarnation, 1, RoleTraffic, "TRAFFIC", 100, 50)
			exec(t, c, insertIncarnation, 2, RoleTraffic, "TRAFFIC", 200, nil)
			exec(t, c, insertIncarnation, 3, RoleParking, "PARKED", 300, nil)
			if err := rest(); err != nil {
				t.Fatal(err)
			}
			if inc, err := c.Incarnate(t.Context(), RoleTraffic, "TRAFFIC", 200); err != nil || inc.ID != 2 {
				t.Errorf("Incarnate = %+v, %v; want the live incarnation 2", inc, err)
			}
		})
		t.Run(engine+"/two live rows", func(t *testing.T) {
			t.Parallel()
			c, rest := migratedBefore(t, engine)
			exec(t, c, insertIncarnation, 1, RoleTraffic, "TRAFFIC", 100, nil)
			exec(t, c, insertIncarnation, 2, RoleTraffic, "TRAFFIC", 200, nil)
			err := rest()
			if err == nil || !strings.Contains(err.Error(), RoleTraffic) {
				t.Fatalf("the migration over two live rows = %v, want a failure naming the role", err)
			}
			if engine == EngineSQLite && !strings.Contains(err.Error(), "1, 2") {
				t.Errorf("the migration's failure %v names no incarnation", err)
			}
			adopted, err := c.AdoptStream(t.Context(), RoleTraffic, "TRAFFIC", 300, "operator")
			if err != nil {
				t.Fatal(err)
			}
			if err := rest(); err != nil {
				t.Fatalf("the migration after the adoption = %v", err)
			}
			if inc, err := c.Incarnate(t.Context(), RoleTraffic, "TRAFFIC", 300); err != nil || inc.ID != adopted.ID {
				t.Errorf("Incarnate = %+v, %v; want the adopted incarnation %d", inc, err, adopted.ID)
			}
			if inc, err := c.Incarnate(t.Context(), RoleParking, "PARKED", 400); err != nil {
				t.Errorf("Incarnate of the parking stream = %+v, %v", inc, err)
			}
			for _, role := range []string{RoleTraffic, RoleParking} {
				if live := liveRows(t, c, role); len(live) != 1 {
					t.Errorf("live %s incarnations %+v, want one", role, live)
				}
			}
		})
	}
}
