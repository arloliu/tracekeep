package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/arloliu/go-secs/tracepack"
)

// A transaction's retries (§7.2 *Retries stay inside the module*, T-53): at most maxRuns runs;
// before each further run the helper waits a random time up to a cap doubling from firstWaitCap to lastWaitCap,
// then stops once maxRetryTime has passed since the first began.
const (
	maxRuns      = 16
	maxRetryTime = 30 * time.Second
	firstWaitCap = 10 * time.Millisecond
	lastWaitCap  = time.Second
)

// pace is the retrying helper's monotonic clock, its wait under the caller's context and its random draw up to a cap;
// the package's tests script it.
type pace struct {
	now  func() time.Time
	wait func(ctx context.Context, d time.Duration) error
	draw func(limit time.Duration) time.Duration
}

var systemPace = pace{
	now:  time.Now,
	wait: sleep,
	draw: func(limit time.Duration) time.Duration { return rand.N(limit + 1) }, //nolint:gosec // jitter, not a secret
}

// sleep waits d, or until ctx ends, returning its error.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// dialect isolates what the two engines spell differently (§7.4).
type dialect struct {
	name string
	// insertIgnore starts an insert that skips a row whose key exists.
	insertIgnore string
	// prologue inserts a pending capture row or advances the existing row's data revision, in one statement,
	// so it takes the row's exclusive lock at once: an insert-ignore followed by an update would hold a shared lock between them on MariaDB,
	// and two transactions doing so on one existing row deadlock.
	prologue string
	// retryable reports an error that re-runs the whole transaction: a deadlock or a busy database.
	retryable func(error) bool
	// retryReason names this engine's retryable error for the retry metric: busy on SQLite, deadlock on MariaDB.
	retryReason string
	// duplicate reports an insert refused by a primary or unique key.
	duplicate func(error) bool
	// nowNS is the database's clock in UTC nanoseconds, which judges every lease (rollouts.md §6.4, T-32).
	nowNS string
}

var sqliteDialect = dialect{
	name:         EngineSQLite,
	retryReason:  "busy",
	insertIgnore: "INSERT OR IGNORE",
	prologue: `INSERT INTO captures (capture_id, state, data_revision, created_at) VALUES (?, 'pending', 1, ?)
		ON CONFLICT (capture_id) DO UPDATE SET data_revision = data_revision + 1`,
	nowNS: "CAST(unixepoch('subsec') * 1000000000 AS INTEGER)",
	retryable: func(err error) bool {
		var e *sqlite.Error
		if !errors.As(err, &e) {
			return false
		}
		code := e.Code() & 0xff

		return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
	},
	duplicate: func(err error) bool {
		var e *sqlite.Error
		return errors.As(err, &e) && (e.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE || e.Code() == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY)
	},
}

var mariadbDialect = dialect{
	name:         EngineMariaDB,
	retryReason:  "deadlock",
	insertIgnore: "INSERT IGNORE",
	prologue: `INSERT INTO captures (capture_id, state, data_revision, created_at) VALUES (?, 'pending', 1, ?)
		ON DUPLICATE KEY UPDATE data_revision = data_revision + 1`,
	nowNS: "CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000000 AS SIGNED)",
	retryable: func(err error) bool {
		var e *mysql.MySQLError
		// 1213 is a deadlock, which rolls the transaction back; a lock wait timeout (1205) is not retried,
		// since a holder stuck that long would keep the caller retrying for minutes.
		return errors.As(err, &e) && e.Number == 1213
	},
	duplicate: func(err error) bool {
		var e *mysql.MySQLError
		return errors.As(err, &e) && e.Number == 1062
	},
}

// captureRow is a capture's row as read under its lock by the prologue.
type captureRow struct {
	State          string         `db:"state"`
	Descriptor     []byte         `db:"descriptor"`
	ToolID         sql.NullString `db:"tool_id"`
	ConfigID       sql.NullInt64  `db:"config_id"`
	DataRevision   int64          `db:"data_revision"`
	HighestSeq     sql.NullInt64  `db:"highest_seq"`
	HighestSeqTS   sql.NullInt64  `db:"highest_seq_ts"`
	StopSeen       bool           `db:"stop_seen"`
	LastRegistered sql.NullInt64  `db:"last_registered_at"`
}

// inTx runs fn in a write transaction, re-running it whole on a deadlock or a busy database,
// so its guards are read again; any other error rolls it back and is returned.
// It makes at most maxRuns runs, with no wait after the last;
// before each further run it waits its backoff under ctx, returning at once with ctx's error if ctx ends,
// and after the wait it stops once maxRetryTime has passed since the first run began; either way it returns the last run's error.
// No run in flight is cut by that bound, only by ctx.
// fn resets on entry every result it records outside itself: a rolled-back attempt's results are not the committed one's.
func (c *Catalog) inTx(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	start := c.pace.now()
	limit := firstWaitCap
	for runs := 1; ; runs++ {
		err := c.attempt(ctx, fn)
		if err == nil || !c.retryable(err) || ctx.Err() != nil || runs >= maxRuns {
			return err
		}
		if werr := c.pace.wait(ctx, c.pace.draw(limit)); werr != nil {
			return errors.Join(werr, err)
		}
		if c.pace.now().Sub(start) >= maxRetryTime {
			return err
		}
		limit = min(2*limit, lastWaitCap)
		if c.retried != nil {
			c.retried(c.d.retryReason)
		}
		c.hook(hookRetry)
	}
}

func (c *Catalog) retryable(err error) bool {
	return c.d.retryable(err) || errors.Is(err, errLostRace) || (c.testRetryable != nil && errors.Is(err, c.testRetryable))
}

// errLostRace is an insert another transaction's committed row refused: the retry reads that row.
var errLostRace = errors.New("catalog: lost a race to another transaction")

// errRollback ends a transaction whose outcome writes nothing, so even the prologue's update is undone;
// the caller reports the outcome its closure recorded.
var errRollback = errors.New("catalog: rolled back")

// outcome turns errRollback, which carries an outcome rather than a failure, into nil.
func outcome(err error) error {
	if errors.Is(err, errRollback) {
		return nil
	}

	return err
}

func (c *Catalog) attempt(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	tx, err := c.write.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return errors.Join(err, rollback(tx))
	}
	c.hook(hookBeforeCommit)
	if c.testCommit != nil {
		if err := c.testCommit(); err != nil {
			return errors.Join(err, rollback(tx))
		}
	}

	return tx.Commit()
}

func rollback(tx *sqlx.Tx) error {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		return err
	}

	return nil
}

// lockCapture is the prologue of every transaction that changes a capture (ingest.md §6.3):
// one statement that inserts a pending row when the catalog has none and otherwise advances the row's data revision,
// which locks it on MariaDB before any predicate is read; it returns the row as read under that lock.
func (c *Catalog) lockCapture(ctx context.Context, tx *sqlx.Tx, id tracepack.UUID) (captureRow, error) {
	var row captureRow
	if _, err := tx.ExecContext(ctx, c.d.prologue, id[:], c.now()); err != nil {
		return row, fmt.Errorf("catalog: lock capture: %w", err)
	}
	err := tx.GetContext(ctx, &row, `SELECT state, descriptor, tool_id, config_id, data_revision, highest_seq, highest_seq_ts,
		stop_seen, last_registered_at FROM captures WHERE capture_id = ?`, id[:])
	if err != nil {
		return row, fmt.Errorf("catalog: lock capture: %w", err)
	}
	c.hook(hookLocked)

	return row, nil
}

// event is a scope event a transaction emits (catalog.md §7.3).
type event struct {
	capture  tracepack.UUID
	hour     int64
	kind     string
	revision int64
}

// emit allocates seqs for evs and inserts them; it is the last step of its transaction,
// so the counters row is locked only across the commit and allocation order is commit order (§7.3).
func (c *Catalog) emit(ctx context.Context, tx *sqlx.Tx, evs []event) error {
	if len(evs) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE counters SET value = value + ? WHERE name = 'scope_events'`, len(evs)); err != nil {
		return fmt.Errorf("catalog: allocate events: %w", err)
	}
	var last int64
	if err := tx.GetContext(ctx, &last, `SELECT value FROM counters WHERE name = 'scope_events'`); err != nil {
		return fmt.Errorf("catalog: allocate events: %w", err)
	}
	at := c.now()
	for i, e := range evs {
		seq := last - int64(len(evs)) + 1 + int64(i)
		_, err := tx.ExecContext(ctx, `INSERT INTO scope_events (seq, capture_id, hour, event, revision, at) VALUES (?, ?, ?, ?, ?, ?)`,
			seq, e.capture[:], e.hour, e.kind, e.revision, at)
		if err != nil {
			return fmt.Errorf("catalog: insert event: %w", err)
		}
	}

	return nil
}
