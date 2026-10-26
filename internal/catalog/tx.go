package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/arloliu/go-secs/tracepack"
)

// maxAttempts bounds a transaction's retries; each attempt re-runs it whole from the prologue.
const maxAttempts = 16

// dialect isolates what the two engines spell differently (§7.4).
type dialect struct {
	name string
	// insertIgnore starts an insert that skips a row whose key exists.
	insertIgnore string
	// retryable reports an error that re-runs the whole transaction: a deadlock or a busy database.
	retryable func(error) bool
	// nowNS is the database's clock in UTC nanoseconds, which judges every lease (rollouts.md §6.4, T-32).
	nowNS string
}

var sqliteDialect = dialect{
	name:         EngineSQLite,
	insertIgnore: "INSERT OR IGNORE",
	nowNS:        "CAST(unixepoch('subsec') * 1000000000 AS INTEGER)",
	retryable: func(err error) bool {
		var e *sqlite.Error
		if !errors.As(err, &e) {
			return false
		}
		code := e.Code() & 0xff

		return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
	},
}

var mariadbDialect = dialect{
	name:         EngineMariaDB,
	insertIgnore: "INSERT IGNORE",
	nowNS:        "CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000000 AS SIGNED)",
	retryable: func(err error) bool {
		var e *mysql.MySQLError
		// 1213 is a deadlock, 1205 a lock wait timeout; both roll the transaction back.
		return errors.As(err, &e) && (e.Number == 1213 || e.Number == 1205)
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
func (c *Catalog) inTx(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	for attempt := 1; ; attempt++ {
		err := c.attempt(ctx, fn)
		if err == nil || !c.d.retryable(err) || attempt == maxAttempts || ctx.Err() != nil {
			return err
		}
		c.hook(hookRetry)
	}
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

	return tx.Commit()
}

func rollback(tx *sqlx.Tx) error {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		return err
	}

	return nil
}

// lockCapture is the prologue of every transaction that changes a capture (ingest.md §6.3):
// a pending row inserted when the catalog has none, then the update of the row, which locks it on MariaDB
// before any predicate is read; it returns the row as read under that lock.
func (c *Catalog) lockCapture(ctx context.Context, tx *sqlx.Tx, id tracepack.UUID) (captureRow, error) {
	var row captureRow
	_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO captures (capture_id, state, created_at) VALUES (?, 'pending', ?)`,
		id[:], c.now())
	if err != nil {
		return row, fmt.Errorf("catalog: lock capture: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE captures SET data_revision = data_revision + 1 WHERE capture_id = ?`, id[:]); err != nil {
		return row, fmt.Errorf("catalog: lock capture: %w", err)
	}
	err = tx.GetContext(ctx, &row, `SELECT state, descriptor, tool_id, config_id, data_revision, highest_seq, highest_seq_ts,
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
