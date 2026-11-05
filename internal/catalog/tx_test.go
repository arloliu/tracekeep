package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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
	other, err := sql.Open("sqlite", SQLiteURI(c.path)+"?_txlock=immediate")
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
		var observed []string
		c.OnRetry(func(reason string) { observed = append(observed, reason) })
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
		want := map[string]string{EngineSQLite: "busy", EngineMariaDB: "deadlock"}[c.Engine()]
		if len(observed) != 1 || observed[0] != want {
			t.Errorf("retries observed %v, want one %s", observed, want)
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

// An insert a primary or unique key refuses is a duplicate, on either engine; another failure is not.
func TestDuplicate(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		insert := `INSERT INTO bus_streams (incarnation_id, role, stream_name, created_ns, last_seq_seen) VALUES (?, ?, 'S', 1, 0)`
		exec(t, c, insert, 1, RoleTraffic)
		for _, row := range [][]any{{1, RoleParking}, {2, RoleTraffic}} {
			if _, err := c.write.ExecContext(t.Context(), insert, row...); !c.d.duplicate(err) {
				t.Errorf("insert %v = %v, want a duplicate", row, err)
			}
		}
		if _, err := c.write.ExecContext(t.Context(), insert, 3, "other"); err == nil || c.d.duplicate(err) {
			t.Errorf("insert of a role the check refuses = %v, want another failure", err)
		}
	})
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
	migrate(t, "sqlite", SQLiteURI(path), EngineSQLite)
	c, err := Open(t.Context(), Config{Engine: EngineSQLite, DSN: path, BusyTimeout: busy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	return busyCatalog{c, path}
}

// A database path holding URI characters names its own file: a subtest's "#01" or a "?" in a directory is no fragment or query.
func TestSQLitePathWithURICharacters(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "a#01?b%20c")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "catalog.db")
	migrate(t, "sqlite", SQLiteURI(path), EngineSQLite)
	c, err := Open(t.Context(), Config{Engine: EngineSQLite, DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.ActivateConfig(t.Context(), []byte("c")); err != nil {
		t.Fatal(err)
	}
	if names, _ := filepath.Glob(filepath.Join(filepath.Dir(dir), "*")); len(names) != 1 {
		t.Errorf("files %v beside the directory, want it alone", names)
	}
}

// scriptedPace is a retry's clock, wait and draw scripted by a test: the clock moves only when the test moves it,
// a wait moves it by its duration and then runs onWait, and a draw is draw's, half the cap without one.
type scriptedPace struct {
	at     time.Duration
	caps   []time.Duration
	waits  []time.Duration
	draw   func(limit time.Duration) time.Duration
	onWait func(ctx context.Context) error
}

func (p *scriptedPace) install(c *Catalog) {
	start := time.Unix(0, 0)
	c.pace = pace{
		now: func() time.Time { return start.Add(p.at) },
		wait: func(ctx context.Context, d time.Duration) error {
			p.waits = append(p.waits, d)
			p.at += d
			if p.onWait != nil {
				return p.onWait(ctx)
			}

			return nil
		},
		draw: func(limit time.Duration) time.Duration {
			p.caps = append(p.caps, limit)
			if p.draw != nil {
				return p.draw(limit)
			}

			return limit / 2
		},
	}
}

// failing runs inTx over a transaction that fails retryably every time, each run moving p's clock by took,
// and returns the runs made and inTx's error.
func failing(ctx context.Context, c *Catalog, p *scriptedPace, took time.Duration) (int, error) {
	errBusy := errors.New("busy")
	c.testRetryable = errBusy
	runs := 0
	err := c.inTx(ctx, func(*sqlx.Tx) error {
		runs++
		p.at += took

		return fmt.Errorf("run %d: %w", runs, errBusy)
	})

	return runs, err
}

// Before each further run the helper waits its draw, under a cap doubling from 10 ms to 1 s;
// it stops after 16 runs, returning the last run's error, with no wait after the sixteenth.
func TestRetryBacksOffUntilTheCount(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		p := &scriptedPace{}
		draws := []time.Duration{0, 20 * time.Millisecond, time.Millisecond} // the floor, the cap, between
		p.draw = func(limit time.Duration) time.Duration {
			if i := len(p.caps) - 1; i < len(draws) {
				return draws[i]
			}

			return limit
		}
		p.install(c)
		var observed []string
		c.OnRetry(func(reason string) { observed = append(observed, reason) })
		runs, err := failing(t.Context(), c, p, 0)
		if runs != 16 || err == nil || err.Error() != "run 16: busy" {
			t.Fatalf("inTx = %v after %d runs, want the 16th run's error", err, runs)
		}
		ms := time.Millisecond
		wantCaps := []time.Duration{10 * ms, 20 * ms, 40 * ms, 80 * ms, 160 * ms, 320 * ms, 640 * ms}
		for len(wantCaps) < 15 {
			wantCaps = append(wantCaps, time.Second)
		}
		if !slices.Equal(p.caps, wantCaps) {
			t.Errorf("caps %v, want %v", p.caps, wantCaps)
		}
		if wantWaits := append(slices.Clone(draws), wantCaps[len(draws):]...); !slices.Equal(p.waits, wantWaits) {
			t.Errorf("waits %v, want each its draw %v", p.waits, wantWaits)
		}
		if len(observed) != 15 {
			t.Errorf("%d retries observed, want 15, one per further run", len(observed))
		}
	})
}

// The time bound is checked after each wait, from the first run's start: runs crossing 30 s stop at the next check,
// a wait crossing it stops before another run, and a run begun before it is left to finish.
func TestRetryStopsAtTheTimeBound(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		p := &scriptedPace{}
		p.install(c)
		// Runs of 7 s: the fifth ends past 35 s, and the check after its wait stops.
		if runs, err := failing(ctx, c, p, 7*time.Second); runs != 5 || err == nil || err.Error() != "run 5: busy" {
			t.Errorf("runs of 7 s: inTx = %v after %d runs, want the fifth run's error", err, runs)
		}

		p = &scriptedPace{}
		p.onWait = func(context.Context) error {
			if len(p.waits) == 3 {
				p.at = 30 * time.Second // the third wait crosses the bound
			}

			return nil
		}
		p.install(c)
		if runs, err := failing(ctx, c, p, 0); runs != 3 || err == nil || err.Error() != "run 3: busy" {
			t.Errorf("a wait crossing the bound: inTx = %v after %d runs, want the third run's error", err, runs)
		}

		p = &scriptedPace{}
		p.onWait = func(context.Context) error {
			p.at = 29 * time.Second
			return nil
		}
		p.install(c)
		errBusy := errors.New("busy")
		c.testRetryable = errBusy
		runs := 0
		err := c.inTx(ctx, func(*sqlx.Tx) error {
			if runs++; runs == 1 {
				return errBusy
			}
			p.at += 30 * time.Second // the second run begins at 29 s and ends at 59 s

			return nil
		})
		if err != nil || runs != 2 {
			t.Errorf("a run begun before the bound: inTx = %v after %d runs, want its success", err, runs)
		}
	})
}

// A caller cancelled or past its deadline during a wait gets its context's error at once, with no further run.
func TestRetryWaitEndsWithTheCaller(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
			ctx, cancel := context.WithCancel(t.Context())
			p := &scriptedPace{}
			p.onWait = func(ctx context.Context) error {
				if errors.Is(want, context.Canceled) {
					cancel()
					return ctx.Err()
				}

				return want // the caller's deadline passed during the wait
			}
			p.install(c)
			runs, err := failing(ctx, c, p, 0)
			cancel()
			if !errors.Is(err, want) || runs != 1 || len(p.waits) != 1 {
				t.Errorf("inTx = %v after %d runs and %d waits, want %v after the first wait", err, runs, len(p.waits), want)
			}
		}
	})
}

// The helper's own wait ends with its context, and otherwise after its duration.
func TestSleepEndsWithTheContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("sleep under a cancelled context = %v, want canceled", err)
	}
	if err := sleep(t.Context(), 0); err != nil {
		t.Errorf("sleep of zero = %v, want nil", err)
	}
}

// A commit that fails once its context has ended answers the context's error too, so a stop reads as one:
// database/sql rolls such a transaction back and its Commit answers sql.ErrTxDone.
func TestCommittedAfterACancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	if err := committed(ctx, sql.ErrTxDone); errors.Is(err, context.Canceled) {
		t.Errorf("committed before the cancel = %v, want no cancellation", err)
	}
	cancel()
	err := committed(ctx, sql.ErrTxDone)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("committed after the cancel = %v, want context.Canceled beside sql.ErrTxDone", err)
	}
	if err := committed(ctx, nil); err != nil {
		t.Errorf("a commit that succeeded = %v, want nil", err)
	}
	if err := committed(ctx, context.Canceled); err != context.Canceled { //nolint:errorlint // the same error, not joined twice
		t.Errorf("a commit that answered the cancel = %v, want it alone", err)
	}
}
