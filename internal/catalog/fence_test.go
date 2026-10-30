package catalog

import (
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	atlasmigrate "ariga.io/atlas/sql/migrate"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/arloliu/tracekeep/migrations"
)

// The lease row an earlier release's compactor names, before the masking migration moved it (T-43).
const oldLease = "compactor"

// preFence is a catalog migrated to every migration before the fence, as an earlier release left it,
// and fence applies the fence and every later migration to it through a connection of its own, as tracekeep migrate does.
func preFence(t *testing.T, engine string) (c *Catalog, fence func() error) {
	t.Helper()
	cfg := Config{Engine: engine}
	var driver, dsn string
	switch engine {
	case EngineSQLite:
		cfg.DSN = filepath.Join(t.TempDir(), "catalog.db")
		driver, dsn = "sqlite", SQLiteURI(cfg.DSN)+"?_pragma=busy_timeout(30000)"
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
	dir, err := migrations.Dir(engine)
	if err != nil {
		t.Fatal(err)
	}
	files, err := dir.Files()
	if err != nil {
		t.Fatal(err)
	}
	at := slices.IndexFunc(files, func(f atlasmigrate.File) bool { return strings.HasSuffix(f.Name(), "_fence.sql") })
	if at < 0 {
		t.Fatal("no fence migration")
	}
	earlier := &atlasmigrate.MemDir{}
	if err := earlier.CopyFiles(files[:at]); err != nil {
		t.Fatal(err)
	}
	if err := migrations.ApplyDir(t.Context(), db, engine, earlier); err != nil {
		t.Fatal(err)
	}
	if c, err = Open(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	return c, func() error { return migrations.Apply(t.Context(), db, engine) }
}

func fenceEngines(t *testing.T, f func(t *testing.T, engine string)) {
	t.Helper()
	for _, engine := range []string{EngineSQLite, EngineMariaDB} {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			f(t, engine)
		})
	}
}

// An old compactor holding the lease when the fence lands can neither renew it nor pass a lease check,
// an old standby cannot take it, and this release's compactor takes the moved row once it lapses, its fence token above the old one.
func TestFenceOldCompactor(t *testing.T) {
	t.Parallel()
	fenceEngines(t, func(t *testing.T, engine string) {
		ctx := t.Context()
		c, fence := preFence(t, engine)
		old, ok, err := c.TakeLease(ctx, oldLease, "old", time.Hour)
		if err != nil || !ok {
			t.Fatalf("the old compactor's take before the fence = %v, %v", ok, err)
		}
		if err := fence(); err != nil {
			t.Fatal(err)
		}
		if err := c.RenewLease(ctx, old, time.Hour); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("the old holder's renewal = %v, want ErrLeaseLost", err)
		}
		if err := c.inTx(ctx, func(tx *sqlx.Tx) error { return c.checkLease(ctx, tx, old) }); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("the old holder's lease check = %v, want ErrLeaseLost", err)
		}
		if _, _, err := c.TakeLease(ctx, oldLease, "standby", time.Hour); !errors.Is(err, ErrUnknownLease) {
			t.Errorf("an old standby's take = %v, want ErrUnknownLease", err)
		}
		if _, ok, err := c.TakeLease(ctx, "compactor/2", "new", time.Hour); err != nil || ok {
			t.Errorf("a take of the moved row while the old holder's term runs = %v, %v; want refused", ok, err)
		}
		lapse(t, c, `UPDATE leases SET until_ns = 0 WHERE name = 'compactor/2'`)
		l, ok, err := c.TakeLease(ctx, "compactor/2", "new", time.Hour)
		if err != nil || !ok || l.Fence <= old.Fence {
			t.Errorf("the new compactor's take = %+v, %v, %v; want a fence above %d", l, ok, err, old.Fence)
		}
	})
}

// An old ingest replica's membership statements, which name the tables the fence moved, fail on the migrated catalog:
// its registration, its renewal and the binding read; this release's registration works.
func TestFenceOldConsumer(t *testing.T) {
	t.Parallel()
	fenceEngines(t, func(t *testing.T, engine string) {
		ctx := t.Context()
		c, fence := preFence(t, engine)
		register := `INSERT INTO consumers (replica_id, lease_until, generation, registered_at) VALUES ('old', 0, 1, 0)`
		if _, err := c.write.ExecContext(ctx, register); err != nil {
			t.Fatalf("an old registration before the fence = %v", err)
		}
		if err := fence(); err != nil {
			t.Fatal(err)
		}
		for name, stmt := range map[string]string{
			"registration": strings.Replace(register, "'old'", "'old-2'", 1),
			"its configs":  `INSERT INTO consumer_configs (replica_id, config_id) VALUES ('old', 1)`,
			"renewal":      `UPDATE consumers SET lease_until = 1 WHERE replica_id = 'old'`,
			"binding read": `SELECT COUNT(*) FROM consumers m WHERE NOT EXISTS (SELECT 1 FROM consumer_configs mc WHERE mc.replica_id = m.replica_id)`,
		} {
			if _, err := c.write.ExecContext(ctx, stmt); err == nil {
				t.Errorf("an old %s on the fenced catalog succeeded", name)
			}
		}
		id, err := c.ActivateConfig(ctx, []byte("config"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Register(ctx, "new", []int64{id}, ttl); err != nil {
			t.Errorf("this release's registration = %v", err)
		}
	})
}

// The fence against an old compactor's transaction held before its lease check, and after it:
// before, the check fails once the fence has landed and the transaction writes nothing;
// after, the fence waits for the transaction, which commits under the lease it held, and the lease is gone once the fence lands.
// No write of the old compactor ever commits after the fence.
func TestFenceAgainstAHeldCompactorTransaction(t *testing.T) {
	t.Parallel()
	for _, beforeCheck := range []bool{true, false} {
		fenceEngines(t, func(t *testing.T, engine string) {
			ctx := t.Context()
			c, fence := preFence(t, engine)
			old, ok, err := c.TakeLease(ctx, oldLease, "old", time.Hour)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			held, release := make(chan struct{}), make(chan struct{})
			hold := func() {
				close(held)
				<-release
			}
			var txErr, fenceErr error
			txDone := run(func() {
				txErr = c.inTx(ctx, func(tx *sqlx.Tx) error {
					if beforeCheck {
						hold()
					}
					if err := c.checkLease(ctx, tx, old); err != nil {
						return err
					}
					if err := c.auditIn(ctx, tx, AuditEntry{Actor: "old-compactor", Action: "merge"}); err != nil {
						return err
					}
					if !beforeCheck {
						hold()
					}

					return nil
				})
			})
			<-held
			fenceDone := run(func() { fenceErr = fence() })
			switch {
			case engine == EngineMariaDB && beforeCheck:
				// The transaction holds no row the fence needs: the fence lands first.
				<-fenceDone
			case engine == EngineMariaDB:
				// The transaction holds the lease row: the fence waits on it.
				waitBlocked(t, c, 0, "UPDATE `leases` SET `name`", 1)
			default:
				// SQLite's one writer: the held transaction began first, so the fence waits whatever the point.
			}
			close(release)
			<-txDone
			<-fenceDone
			if fenceErr != nil {
				t.Fatalf("the fence = %v", fenceErr)
			}
			var audited int
			if err := c.read.GetContext(ctx, &audited, `SELECT COUNT(*) FROM audit_log WHERE actor = 'old-compactor'`); err != nil {
				t.Fatal(err)
			}
			heldOutcome(t, engine, beforeCheck, txErr, audited)
			if err := c.RenewLease(ctx, old, time.Hour); !errors.Is(err, ErrLeaseLost) {
				t.Errorf("the old holder's renewal after the fence = %v, want ErrLeaseLost", err)
			}
		})
	}
}

// heldOutcome checks what the held transaction did: committed before the fence under its lease, or refused by it and written nothing;
// only one held before its check on MariaDB, where the fence lands first, may lose the lease.
func heldOutcome(t *testing.T, engine string, beforeCheck bool, txErr error, audited int) {
	t.Helper()
	fenceFirst := engine == EngineMariaDB && beforeCheck
	switch {
	case txErr == nil && audited == 1 && !fenceFirst:
		// Committed before the fence, under the lease it held.
	case errors.Is(txErr, ErrLeaseLost) && audited == 0 && fenceFirst:
		// Refused by the fence; nothing written.
	default:
		t.Errorf("the held transaction (before its check %v, %s) = %v with %d audit rows", beforeCheck, engine, txErr, audited)
	}
}
