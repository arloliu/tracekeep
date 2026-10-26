package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// The prologue inserts a pending row the first time and advances the data revision every time.
func TestPrologue(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		id := newID(t)
		var rows []captureRow
		for range 2 {
			err := c.inTx(t.Context(), func(tx *sqlx.Tx) error {
				row, err := c.lockCapture(t.Context(), tx, id)
				rows = append(rows, row)

				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		if rows[0].State != "pending" || rows[0].DataRevision != 1 || rows[1].DataRevision != 2 {
			t.Errorf("rows under the lock = %+v, want a pending row at revisions 1 then 2", rows)
		}
	})
}

// Events take consecutive seqs in commit order, several in one transaction included.
func TestEmitAllocatesSeqs(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		id := newID(t)
		for _, n := range []int{3, 1} {
			err := c.inTx(t.Context(), func(tx *sqlx.Tx) error {
				evs := make([]event, n)
				for i := range evs {
					evs[i] = event{capture: id, hour: int64(i), kind: "created", revision: 1}
				}

				return c.emit(t.Context(), tx, evs)
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		var seqs []int64
		if err := c.read.SelectContext(t.Context(), &seqs, `SELECT seq FROM scope_events ORDER BY seq`); err != nil {
			t.Fatal(err)
		}
		if len(seqs) != 4 || seqs[0] != 1 || seqs[3] != 4 {
			t.Errorf("event seqs = %v, want 1 to 4", seqs)
		}
	})
}

// A transaction that finds the database busy is run again whole, and succeeds once the lock is free.
func TestBusyTransactionRetries(t *testing.T) {
	t.Parallel()
	c := newCatalogBusy(t, 50*time.Millisecond)
	// Another connection holds SQLite's write lock.
	other, err := sql.Open("sqlite", "file:"+c.path+"?_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	hold, err := other.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	retries := 0
	c.hooks[hookRetry] = func() {
		if retries++; retries == 1 {
			_ = hold.Rollback() // the lock is free for the next attempt
		}
	}
	err = c.inTx(t.Context(), func(tx *sqlx.Tx) error {
		_, err := c.lockCapture(t.Context(), tx, newID(t))

		return err
	})
	if err != nil || retries != 1 {
		t.Errorf("inTx = %v after %d retries, want success after one", err, retries)
	}
}

// A second writer of one SQLite catalog is refused, through a link to the file as well.
func TestSQLiteRefusesASecondWriter(t *testing.T) {
	t.Parallel()
	c := newCatalogBusy(t, 0)
	if _, err := Open(t.Context(), Config{Engine: EngineSQLite, DSN: c.path}); !errors.Is(err, ErrSecondWriter) {
		t.Errorf("second Open = %v, want ErrSecondWriter", err)
	}
	link := filepath.Join(t.TempDir(), "alias.db")
	if err := os.Symlink(c.path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), Config{Engine: EngineSQLite, DSN: link}); !errors.Is(err, ErrSecondWriter) {
		t.Errorf("Open through a link = %v, want ErrSecondWriter", err)
	}
}

// A transaction that fails retryably after it wrote is rolled back and run again whole:
// its guards are read again and its effect is durable once.
func TestRetryReplaysAfterAWrite(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		errDeadlock := errors.New("deadlock")
		c.testRetryable = errDeadlock
		id := newID(t)
		var revisions []int64
		err := c.inTx(t.Context(), func(tx *sqlx.Tx) error {
			row, err := c.lockCapture(t.Context(), tx, id)
			if err != nil {
				return err
			}
			revisions = append(revisions, row.DataRevision)
			if len(revisions) == 1 {
				return errDeadlock // after the prologue wrote
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// The second run read the row afresh: the first run's write was rolled back.
		if len(revisions) != 2 || revisions[0] != 1 || revisions[1] != 1 {
			t.Errorf("revisions read by the runs = %v, want [1 1]", revisions)
		}
		if n := count(t, c, `SELECT data_revision FROM captures WHERE capture_id = ?`, id[:]); n != 1 {
			t.Errorf("data revision %d, want 1: one durable effect", n)
		}
	})
}

// A MariaDB deadlock is retried; a lock wait timeout is not.
func TestMariaDBRetryable(t *testing.T) {
	t.Parallel()
	if !mariadbDialect.retryable(fmt.Errorf("x: %w", &mysql.MySQLError{Number: 1213})) {
		t.Error("a deadlock is not retried")
	}
	if mariadbDialect.retryable(&mysql.MySQLError{Number: 1205}) {
		t.Error("a lock wait timeout is retried")
	}
}

// MariaDB transactions run at READ COMMITTED (§7.4).
func TestMariaDBReadCommitted(t *testing.T) {
	t.Parallel()
	c := newCatalog(t, EngineMariaDB)
	err := c.inTx(t.Context(), func(tx *sqlx.Tx) error {
		var level string
		if err := tx.GetContext(context.Background(), &level, `SELECT @@transaction_isolation`); err != nil {
			return err
		}
		if level != "READ-COMMITTED" {
			t.Errorf("isolation %q, want READ-COMMITTED", level)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type busyCatalog struct {
	*Catalog
	path string
}

// newCatalogBusy opens a SQLite catalog whose statements wait busy for a lock.
func newCatalogBusy(t *testing.T, busy time.Duration) busyCatalog {
	t.Helper()
	path := t.TempDir() + "/catalog.db"
	migrate(t, "sqlite", "file:"+path, EngineSQLite)
	c, err := Open(t.Context(), Config{Engine: EngineSQLite, DSN: path, BusyTimeout: busy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	return busyCatalog{c, path}
}
