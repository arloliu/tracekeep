package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// The states of an export job and of an attempt's object (api.md §10.1 *Exports*).
const (
	ExportQueued    = "queued"
	ExportRunning   = "running"
	ExportDone      = "done"
	ExportFailed    = "failed"
	ExportCancelled = "cancelled"

	ObjectIntended = "intended"
	ObjectBundle   = "bundle"
	ObjectDeleting = "deleting"
)

var (
	// ErrAttemptLost refuses a step of an attempt whose job is no longer running at its fence with its lease live.
	ErrAttemptLost = errors.New("catalog: the export attempt is lost")
	// ErrExportNotFound answers a job that does not exist or is not the owner's.
	ErrExportNotFound = errors.New("catalog: no such export job")
	// ErrExportEnded refuses to cancel a job that has ended.
	ErrExportEnded = errors.New("catalog: the export job has ended")
)

// ExportJob is an export job's row.
type ExportJob struct {
	ID         tracepack.UUID
	Owner      string
	Request    []byte
	State      string
	Fence      int64
	Holder     *string
	LeaseUntil int64
	Attempts   int64
	ObjectKey  *string
	Size       *int64
	CreatedAt  int64
	EndedAt    *int64
	DeleteAt   *int64
	Reason     *string
}

// Attempt is a worker's hold of a job: the job at a fence, by holder.
type Attempt struct {
	Job    tracepack.UUID
	Fence  int64
	Holder string
}

// Claim is a claimed job with the attempt holding it.
type Claim struct {
	Attempt Attempt
	Job     ExportJob
}

// ExportObject is an attempt's object row.
type ExportObject struct {
	Key   string
	Job   tracepack.UUID
	Fence int64
	State string
}

const exportJobColumns = `job_id, owner, request, state, fence, holder, lease_until, attempts, object_key, size, created_at, ended_at, delete_at, reason`

type exportJobRow struct {
	ID         []byte         `db:"job_id"`
	Owner      string         `db:"owner"`
	Request    string         `db:"request"`
	State      string         `db:"state"`
	Fence      int64          `db:"fence"`
	Holder     sql.NullString `db:"holder"`
	LeaseUntil int64          `db:"lease_until"`
	Attempts   int64          `db:"attempts"`
	ObjectKey  sql.NullString `db:"object_key"`
	Size       sql.NullInt64  `db:"size"`
	CreatedAt  int64          `db:"created_at"`
	EndedAt    sql.NullInt64  `db:"ended_at"`
	DeleteAt   sql.NullInt64  `db:"delete_at"`
	Reason     sql.NullString `db:"reason"`
}

func (r *exportJobRow) job() ExportJob {
	j := ExportJob{
		ID: tracepack.UUID(r.ID), Owner: r.Owner, Request: []byte(r.Request), State: r.State, Fence: r.Fence,
		LeaseUntil: r.LeaseUntil, Attempts: r.Attempts, CreatedAt: r.CreatedAt,
	}
	if r.Holder.Valid {
		j.Holder = &r.Holder.String
	}
	if r.ObjectKey.Valid {
		j.ObjectKey = &r.ObjectKey.String
	}
	if r.Size.Valid {
		j.Size = &r.Size.Int64
	}
	if r.EndedAt.Valid {
		j.EndedAt = &r.EndedAt.Int64
	}
	if r.DeleteAt.Valid {
		j.DeleteAt = &r.DeleteAt.Int64
	}
	if r.Reason.Valid {
		j.Reason = &r.Reason.String
	}

	return j
}

// CreateExport creates job id for owner, queued, with its audit row (catalog.md §7.2 *Export transactions*).
func (c *Catalog) CreateExport(ctx context.Context, id tracepack.UUID, owner string, request []byte, audit AuditEntry) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO export_jobs (job_id, owner, request, state, fence, lease_until, attempts, created_at)
			VALUES (?, ?, ?, 'queued', 0, 0, 0, `+c.d.nowNS+`)`, id[:], owner, string(request))
		if err != nil {
			return err
		}

		return c.auditIn(ctx, tx, audit)
	})
	if err != nil {
		return fmt.Errorf("catalog: create export %s: %w", id, err)
	}

	return nil
}

// ClaimExport claims the oldest queued job, or a running one whose lease has passed, for holder, with a lease of lease;
// a claimable job that has had maxAttempts attempts is failed instead. ok is false when there is none.
func (c *Catalog) ClaimExport(ctx context.Context, holder string, lease time.Duration, maxAttempts int) (Claim, bool, error) {
	claimable := `(state = 'queued' OR (state = 'running' AND lease_until < ` + c.d.nowNS + `))`
	var job ExportJob
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		// Jobs past their attempts are failed first, so the claim below never picks one.
		_, err := tx.ExecContext(ctx, `UPDATE export_jobs SET state = 'failed', ended_at = `+c.d.nowNS+`, holder = NULL,
			reason = 'its attempts are exhausted' WHERE `+claimable+` AND attempts >= ?`, maxAttempts)
		if err != nil {
			return err
		}
		var id []byte
		err = tx.GetContext(ctx, &id, `SELECT job_id FROM export_jobs WHERE `+claimable+` ORDER BY created_at, job_id LIMIT 1`)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE export_jobs SET state = 'running', fence = fence + 1, holder = ?, lease_until = `+c.d.nowNS+` + ?,
			attempts = attempts + 1 WHERE job_id = ? AND `+claimable, holder, lease.Nanoseconds(), id)
		if err := affected(res, err, errRetryClaim); err != nil {
			return err
		}
		var row exportJobRow
		if err := tx.GetContext(ctx, &row, `SELECT `+exportJobColumns+` FROM export_jobs WHERE job_id = ?`, id); err != nil {
			return err
		}
		job = row.job()

		return nil
	})
	switch {
	case errors.Is(err, errRetryClaim):
		return Claim{}, false, nil // another replica claimed it first
	case err != nil:
		return Claim{}, false, fmt.Errorf("catalog: claim an export: %w", err)
	case job.State == "":
		return Claim{}, false, nil
	}

	return Claim{Attempt: Attempt{Job: job.ID, Fence: job.Fence, Holder: holder}, Job: job}, true, nil
}

var errRetryClaim = errors.New("catalog: the claim lost a race")

// attemptGuard is the attempt's guard: its job running at its fence with its lease not passed.
func (c *Catalog) attemptGuard() string {
	return `job_id = ? AND state = 'running' AND fence = ? AND lease_until >= ` + c.d.nowNS
}

// RenewExport moves a's lease to lease from now, or refuses with ErrAttemptLost.
func (c *Catalog) RenewExport(ctx context.Context, a Attempt, lease time.Duration) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE export_jobs SET lease_until = `+c.d.nowNS+` + ? WHERE `+c.attemptGuard(),
			lease.Nanoseconds(), a.Job[:], a.Fence)

		return affected(res, err, ErrAttemptLost)
	})
	if err != nil {
		return fmt.Errorf("catalog: renew export %s at %d: %w", a.Job, a.Fence, err)
	}

	return nil
}

// IntendExport records a's object key, intended, with a put deadline putTimeout from the catalog's now, which it returns.
func (c *Catalog) IntendExport(ctx context.Context, a Attempt, key string, putTimeout time.Duration) (int64, error) {
	var deadline int64
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE export_jobs SET fence = fence WHERE `+c.attemptGuard(), a.Job[:], a.Fence)
		if err := affected(res, err, ErrAttemptLost); err != nil {
			return err
		}
		if err := tx.GetContext(ctx, &deadline, `SELECT `+c.d.nowNS+` + ?`, putTimeout.Nanoseconds()); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO export_objects (object_key, job_id, fence, put_deadline, state) VALUES (?, ?, ?, ?, 'intended')`,
			key, a.Job[:], a.Fence, deadline)

		return err
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: intend export %s at %d: %w", a.Job, a.Fence, err)
	}

	return deadline, nil
}

// CompleteExport makes a's job done with its bundle under key, whose deadline is retention from the catalog's now, which it returns;
// it requires the attempt's guard and its object row intended, which the cleanup's doom excludes.
func (c *Catalog) CompleteExport(ctx context.Context, a Attempt, key string, size int64, retention time.Duration) (int64, error) {
	var deleteAt int64
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE export_objects SET state = 'bundle' WHERE object_key = ? AND job_id = ? AND fence = ? AND state = 'intended'`,
			key, a.Job[:], a.Fence)
		if err := affected(res, err, ErrAttemptLost); err != nil {
			return err
		}
		res, err = tx.ExecContext(ctx, `UPDATE export_jobs SET state = 'done', holder = NULL, object_key = ?, size = ?, ended_at = `+c.d.nowNS+`,
			delete_at = `+c.d.nowNS+` + ? WHERE `+c.attemptGuard(), key, size, retention.Nanoseconds(), a.Job[:], a.Fence)
		if err := affected(res, err, ErrAttemptLost); err != nil {
			return err
		}

		return tx.GetContext(ctx, &deleteAt, `SELECT delete_at FROM export_jobs WHERE job_id = ?`, a.Job[:])
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: complete export %s at %d: %w", a.Job, a.Fence, err)
	}

	return deleteAt, nil
}

// FailExport makes a's job failed with reason, which names no object key.
func (c *Catalog) FailExport(ctx context.Context, a Attempt, reason string) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE export_jobs SET state = 'failed', holder = NULL, ended_at = `+c.d.nowNS+`, reason = ? WHERE `+c.attemptGuard(),
			reason, a.Job[:], a.Fence)

		return affected(res, err, ErrAttemptLost)
	})
	if err != nil {
		return fmt.Errorf("catalog: fail export %s at %d: %w", a.Job, a.Fence, err)
	}

	return nil
}

// CancelExport cancels owner's job when it is queued or running, with its audit row;
// another's job, or none, is ErrExportNotFound, and an ended one ErrExportEnded.
func (c *Catalog) CancelExport(ctx context.Context, job tracepack.UUID, owner string, audit AuditEntry) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE export_jobs SET state = 'cancelled', holder = NULL, ended_at = `+c.d.nowNS+`
			WHERE job_id = ? AND owner = ? AND state IN ('queued', 'running')`, job[:], owner)
		if err := affected(res, err, errNoCancel); errors.Is(err, errNoCancel) {
			var n int
			if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM export_jobs WHERE job_id = ? AND owner = ?`, job[:], owner); err != nil {
				return err
			}
			if n == 0 {
				return ErrExportNotFound
			}

			return ErrExportEnded
		} else if err != nil {
			return err
		}

		return c.auditIn(ctx, tx, audit)
	})
	if err != nil {
		return fmt.Errorf("catalog: cancel export %s: %w", job, err)
	}

	return nil
}

var errNoCancel = errors.New("catalog: nothing cancelled")

// ExportJob reads owner's job, or ErrExportNotFound.
func (c *Catalog) ExportJob(ctx context.Context, job tracepack.UUID, owner string) (ExportJob, error) {
	var row exportJobRow
	err := sqlx.GetContext(ctx, c.read, &row, `SELECT `+exportJobColumns+` FROM export_jobs WHERE job_id = ? AND owner = ?`, job[:], owner)
	if errors.Is(err, sql.ErrNoRows) {
		return ExportJob{}, fmt.Errorf("catalog: export %s: %w", job, ErrExportNotFound)
	} else if err != nil {
		return ExportJob{}, fmt.Errorf("catalog: export %s: %w", job, err)
	}

	return row.job(), nil
}

// ExportJobs reads owner's jobs, newest first, at most limit.
func (c *Catalog) ExportJobs(ctx context.Context, owner string, limit int) ([]ExportJob, error) {
	var rows []exportJobRow
	err := sqlx.SelectContext(ctx, c.read, &rows, `SELECT `+exportJobColumns+` FROM export_jobs WHERE owner = ? ORDER BY created_at DESC, job_id LIMIT ?`, owner, limit)
	if err != nil {
		return nil, fmt.Errorf("catalog: exports of an owner: %w", err)
	}
	out := make([]ExportJob, 0, len(rows))
	for i := range rows {
		out = append(out, rows[i].job())
	}

	return out, nil
}

// DoomExportObjects, under l, makes deleting every intended row past its put deadline plus skew and every bundle row past its job's deadline plus skew,
// and returns every deleting row, the earlier cycles' included, at most limit, whose objects the compactor deletes next.
func (c *Catalog) DoomExportObjects(ctx context.Context, l Lease, skew time.Duration, limit int) ([]ExportObject, error) {
	var out []ExportObject
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE export_objects SET state = 'deleting' WHERE state = 'intended' AND put_deadline + ? < `+c.d.nowNS, skew.Nanoseconds())
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE export_objects SET state = 'deleting' WHERE state = 'bundle'
			AND job_id IN (SELECT job_id FROM export_jobs WHERE state = 'done' AND delete_at + ? < `+c.d.nowNS+`)`, skew.Nanoseconds())
		if err != nil {
			return err
		}
		var rows []struct {
			Key   string `db:"object_key"`
			Job   []byte `db:"job_id"`
			Fence int64  `db:"fence"`
		}
		if err := tx.SelectContext(ctx, &rows, `SELECT object_key, job_id, fence FROM export_objects WHERE state = 'deleting' ORDER BY object_key LIMIT ?`, limit); err != nil {
			return err
		}
		for _, r := range rows {
			out = append(out, ExportObject{Key: r.Key, Job: tracepack.UUID(r.Job), Fence: r.Fence, State: ObjectDeleting})
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("catalog: doom export objects: %w", err)
	}

	return out, nil
}

// ForgetExportObject, under l, deletes key's row once its object is deleted; the row must be deleting.
func (c *Catalog) ForgetExportObject(ctx context.Context, l Lease, key string) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM export_objects WHERE object_key = ? AND state = 'deleting'`, key)

		return err
	})
	if err != nil {
		return fmt.Errorf("catalog: forget an export object: %w", err)
	}

	return nil
}

// ForgetExports, under l, deletes every job no object row is left of: done past its deadline, or failed or cancelled ended past retention;
// it returns how many it deleted.
func (c *Catalog) ForgetExports(ctx context.Context, l Lease, retention time.Duration) (int64, error) {
	var n int64
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM export_jobs WHERE NOT EXISTS (SELECT 1 FROM export_objects o WHERE o.job_id = export_jobs.job_id)
			AND ((state = 'done' AND delete_at < `+c.d.nowNS+`) OR (state IN ('failed', 'cancelled') AND ended_at + ? < `+c.d.nowNS+`))`,
			retention.Nanoseconds())
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()

		return err
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: forget exports: %w", err)
	}

	return n, nil
}
