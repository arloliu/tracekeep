package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// ErrNoBindableConfig reports that no activated configuration is supported by every leased member,
// so a capture cannot be admitted yet; nothing was written.
var ErrNoBindableConfig = errors.New("catalog: no configuration every leased member supports")

// ErrUnknownCapture reports a capture the catalog has no row for.
var ErrUnknownCapture = errors.New("catalog: unknown capture")

// The capture states (catalog.md §7.1).
const (
	StatePending  = "pending"
	StateOpen     = "open"
	StateStopped  = "stopped"
	StateClosed   = "closed"
	StateRejected = "rejected"
)

// Outcome is what an admission or a rejection did (ingest.md §6.1).
type Outcome int

const (
	// Admitted: the capture moved to open, its configuration bound.
	Admitted Outcome = iota + 1
	// Unchanged: the capture was already admitted with these descriptor bytes; nothing was written.
	Unchanged
	// DescriptorConflict: the capture holds other descriptor bytes; nothing was written.
	DescriptorConflict
	// Rejected: the capture is rejected with its descriptor bytes, durably.
	Rejected
	// AlreadyAdmitted: a rejection found the capture admitted with these bytes; nothing was written,
	// and the classifier decides again with the bound configuration.
	AlreadyAdmitted
)

// Descriptor is an accepted start descriptor: its bytes and the fields the catalog indexes.
type Descriptor struct {
	Bytes              []byte
	ToolID             string
	RecorderInstanceID tracepack.UUID
	// Device is the descriptor's recorder_instance_name.
	Device            string
	OriginNS          int64
	PreviousCaptureID *tracepack.UUID
}

// Result is an admission's or a rejection's outcome, with the configuration the capture is bound to when it is admitted.
type Result struct {
	Outcome  Outcome
	ConfigID int64
}

// Capture is a capture's row as a reader sees it.
type Capture struct {
	ID            tracepack.UUID
	State         string
	Descriptor    []byte
	ToolID        string
	Device        string
	ConfigID      int64
	HighestSeq    int64
	HasHighestSeq bool
	StopSeen      bool
	DataRevision  int64
}

// Admit admits the capture id with the accepted descriptor d (catalog.md §7.2 `Admit`):
// no row or a pending one moves to open and binds the newest configuration every leased member supports;
// a rejected one does the same when its retained bytes equal d's;
// equal bytes on an admitted capture change nothing, and other bytes are a descriptor conflict.
func (c *Catalog) Admit(ctx context.Context, id tracepack.UUID, d *Descriptor) (Result, error) {
	var res Result
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		row, err := c.lockCapture(ctx, tx, id)
		if err != nil {
			return err
		}
		switch row.State {
		case StatePending:
		case StateRejected:
			if !bytes.Equal(row.Descriptor, d.Bytes) {
				res = Result{Outcome: DescriptorConflict}
				return nil
			}
		default: // admitted
			if !bytes.Equal(row.Descriptor, d.Bytes) {
				res = Result{Outcome: DescriptorConflict}
				return nil
			}
			res = Result{Outcome: Unchanged, ConfigID: row.ConfigID.Int64}

			return nil
		}
		// Binding: the membership sentinel after the capture row, before the members and configurations are read.
		if err := c.lockMembership(ctx, tx); err != nil {
			return err
		}
		cfg, ok, err := c.bindableConfig(ctx, tx)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNoBindableConfig
		}
		var prev []byte
		if d.PreviousCaptureID != nil {
			prev = d.PreviousCaptureID[:]
		}
		_, err = tx.ExecContext(ctx, `UPDATE captures SET state = 'open', descriptor = ?, tool_id = ?, recorder_instance_id = ?, device = ?,
			origin_ns = ?, previous_capture_id = ?, config_id = ? WHERE capture_id = ?`,
			d.Bytes, d.ToolID, d.RecorderInstanceID[:], d.Device, d.OriginNS, prev, cfg, id[:])
		res = Result{Outcome: Admitted, ConfigID: cfg}

		return err
	})
	if err != nil {
		return Result{}, fmt.Errorf("catalog: admit %s: %w", id, err)
	}

	return res, nil
}

// Reject records that the start descriptor of capture id is not accepted (catalog.md §7.2 `Reject`):
// an unknown capture becomes rejected with the bytes, durably, so its start stays deferred whatever its age;
// an admitted capture is never overwritten: equal bytes return AlreadyAdmitted with its configuration, other bytes DescriptorConflict.
func (c *Catalog) Reject(ctx context.Context, id tracepack.UUID, descriptor []byte) (Result, error) {
	var res Result
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		row, err := c.lockCapture(ctx, tx, id)
		if err != nil {
			return err
		}
		switch row.State {
		case StatePending:
			res = Result{Outcome: Rejected}
			_, err := tx.ExecContext(ctx, `UPDATE captures SET state = 'rejected', descriptor = ? WHERE capture_id = ?`, descriptor, id[:])

			return err
		case StateRejected:
			// The first rejected bytes stay, for the operator.
			res = Result{Outcome: Rejected}
		default:
			if !bytes.Equal(row.Descriptor, descriptor) {
				res = Result{Outcome: DescriptorConflict}
				return nil
			}
			res = Result{Outcome: AlreadyAdmitted, ConfigID: row.ConfigID.Int64}
		}

		return nil
	})
	if err != nil {
		return Result{}, fmt.Errorf("catalog: reject %s: %w", id, err)
	}

	return res, nil
}

// Capture looks capture id up; a capture with no row returns ErrUnknownCapture, and a pending row is returned as pending.
func (c *Catalog) Capture(ctx context.Context, id tracepack.UUID) (Capture, error) {
	var row struct {
		State      string         `db:"state"`
		Descriptor []byte         `db:"descriptor"`
		ToolID     sql.NullString `db:"tool_id"`
		Device     sql.NullString `db:"device"`
		ConfigID   sql.NullInt64  `db:"config_id"`
		HighestSeq sql.NullInt64  `db:"highest_seq"`
		StopSeen   bool           `db:"stop_seen"`
		Revision   int64          `db:"data_revision"`
	}
	err := c.read.GetContext(ctx, &row, `SELECT state, descriptor, tool_id, device, config_id, highest_seq, stop_seen, data_revision
		FROM captures WHERE capture_id = ?`, id[:])
	if errors.Is(err, sql.ErrNoRows) {
		return Capture{}, fmt.Errorf("%w: %s", ErrUnknownCapture, id)
	}
	if err != nil {
		return Capture{}, fmt.Errorf("catalog: capture %s: %w", id, err)
	}

	return Capture{
		ID: id, State: row.State, Descriptor: row.Descriptor, ToolID: row.ToolID.String, Device: row.Device.String,
		ConfigID: row.ConfigID.Int64, HighestSeq: row.HighestSeq.Int64, HasHighestSeq: row.HighestSeq.Valid,
		StopSeen: row.StopSeen, DataRevision: row.Revision,
	}, nil
}
