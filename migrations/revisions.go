package migrations

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"ariga.io/atlas/sql/migrate"
)

// revisions is Atlas's revision storage over Table, on the migrator's connection.
type revisions struct{ db *sql.Conn }

var _ migrate.RevisionReadWriter = (*revisions)(nil)

// Ident implements migrate.RevisionReadWriter.
func (r *revisions) Ident() *migrate.TableIdent { return &migrate.TableIdent{Name: Table} }

const revisionColumns = `version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version`

// ReadRevisions implements migrate.RevisionReadWriter, in version order.
func (r *revisions) ReadRevisions(ctx context.Context) ([]*migrate.Revision, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT `+revisionColumns+` FROM `+Table+` ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*migrate.Revision
	for rows.Next() {
		rev, err := scanRevision(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rev)
	}

	return out, rows.Err()
}

// ReadRevision implements migrate.RevisionReadWriter.
func (r *revisions) ReadRevision(ctx context.Context, version string) (*migrate.Revision, error) {
	rev, err := scanRevision(r.db.QueryRowContext(ctx, `SELECT `+revisionColumns+` FROM `+Table+` WHERE version = ?`, version))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, migrate.ErrRevisionNotExist
	}

	return rev, err
}

// WriteRevision implements migrate.RevisionReadWriter: it replaces the version's row.
func (r *revisions) WriteRevision(ctx context.Context, rev *migrate.Revision) error {
	partial, err := json.Marshal(rev.PartialHashes)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `REPLACE INTO `+Table+` (`+revisionColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rev.Version, rev.Description, int(rev.Type), rev.Applied, rev.Total, rev.ExecutedAt.UnixNano(), int64(rev.ExecutionTime),
		rev.Error, rev.ErrorStmt, rev.Hash, string(partial), rev.OperatorVersion)

	return err
}

// DeleteRevision implements migrate.RevisionReadWriter.
func (r *revisions) DeleteRevision(ctx context.Context, version string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM `+Table+` WHERE version = ?`, version)

	return err
}

func scanRevision(row interface{ Scan(...any) error }) (*migrate.Revision, error) {
	var (
		rev            migrate.Revision
		typ            int
		executedAt, dt int64
		partial        string
	)
	err := row.Scan(&rev.Version, &rev.Description, &typ, &rev.Applied, &rev.Total, &executedAt, &dt,
		&rev.Error, &rev.ErrorStmt, &rev.Hash, &partial, &rev.OperatorVersion)
	if err != nil {
		return nil, err
	}
	rev.Type, rev.ExecutedAt, rev.ExecutionTime = migrate.RevisionType(typ), time.Unix(0, executedAt), time.Duration(dt) //nolint:gosec // a stored type is small
	if err := json.Unmarshal([]byte(partial), &rev.PartialHashes); err != nil {
		return nil, fmt.Errorf("migrations: %s %s: partial hashes: %w", Table, rev.Version, err)
	}

	return &rev, nil
}
