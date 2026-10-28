package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// VersionState is a parked version's state (parking.md §7.1), which moves forward only:
// parking → parked → staged | terminated, with evicted-hour between parked and terminated.
type VersionState string

// The version states.
const (
	VersionParking     VersionState = "parking"
	VersionParked      VersionState = "parked"
	VersionEvictedHour VersionState = "evicted-hour"
	VersionStaged      VersionState = "staged"
	VersionTerminated  VersionState = "terminated"
)

// Resolved reports a state no parking step changes: the version is staged or terminated.
func (s VersionState) Resolved() bool { return s == VersionStaged || s == VersionTerminated }

// ErrUnknownVersion reports a version the catalog has no parked row for.
var ErrUnknownVersion = errors.New("catalog: unknown parked version")

// ErrLeaseLive refuses to adopt a stream while a tracekeep process may still run: a membership or the compactor lease is live.
var ErrLeaseLive = errors.New("catalog: a lease is live")

// Begun is what parking's begin found under the capture row.
type Begun struct {
	// State is the version's state, the copy published only while it is unresolved; empty when nothing was parked.
	State VersionState
	// BeyondRetention reports a new version of an admitted capture whose hour is older than the retention horizon:
	// nothing was written, and the caller terminates the delivery as beyond-retention.
	BeyondRetention bool
}

// BeginParking is step (1) of the parking protocol (parking.md *Parking*, catalog.md §7.2 `BeginParking`) for the delivery d,
// deferred indefinitely for reason: under the capture row, a pending row for an unknown capture,
// the termination re-check of Admit, the horizon guard for a new version of an admitted capture,
// then the version's row inserted parking or found, and the delivery's copy row inserted or found in every branch that parks or resolves;
// a covered seq a registered pack holds resolves staged with no parked row, and nothing is written.
// objectKey is the claim object a claim-check record keeps alive, empty for an inline one.
func (c *Catalog) BeginParking(ctx context.Context, d *Identity, reason, objectKey string, horizonHour int64) (Begun, error) {
	var b Begun
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		row, err := c.lockCapture(ctx, tx, d.Version.Capture)
		if err != nil {
			return err
		}
		r, err := c.recheck(ctx, tx, d)
		switch {
		case err != nil:
			return err
		case r == CoveredByOrder:
			b = Begun{State: VersionTerminated}
			return nil
		}
		state, err := readVersionState(ctx, tx, &d.Version)
		beyond := d.Hour < horizonHour
		if errors.Is(err, ErrUnknownVersion) && isAdmitted(row.State) && !beyond {
			var e error
			if beyond, e = hourEvicted(ctx, tx, d.Version.Capture, d.Hour); e != nil {
				return e
			}
		}
		switch {
		case errors.Is(err, ErrUnknownVersion) && r == ResolvedStaged:
			// Covered by an order but held by a registered pack: staged with no parked row, so nothing is written.
			b = Begun{State: VersionStaged}
			return errRollback
		case errors.Is(err, ErrUnknownVersion) && isAdmitted(row.State) && beyond:
			// Retention may already have evicted the hour (§8.4), or did, so the version is never parked.
			b = Begun{BeyondRetention: true}
			return errRollback
		case errors.Is(err, ErrUnknownVersion):
			v := &d.Version
			_, err = tx.ExecContext(ctx, `INSERT INTO parked (capture_id, seq, header, payload_digest, state, reason, hour, object_key, parked_at)
				VALUES (?, ?, ?, ?, 'parking', ?, ?, ?, ?)`,
				v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:], reason, d.Hour, nullString(objectKey), c.now())
			state = VersionParking
		default: // found, or a failed read
		}
		if err != nil {
			return err
		}
		b = Begun{State: state}

		return c.insertCopy(ctx, tx, d)
	}))
	if err != nil {
		return Begun{}, fmt.Errorf("catalog: begin parking %s.%d: %w", d.Version.Capture, d.Version.Seq, err)
	}

	return b, nil
}

func isAdmitted(state string) bool {
	return state == StateOpen || state == StateStopped || state == StateClosed
}

// readVersionState reads v's state; a version with no row returns ErrUnknownVersion.
func readVersionState(ctx context.Context, q sqlx.QueryerContext, v *VersionKey) (VersionState, error) {
	var s VersionState
	err := sqlx.GetContext(ctx, q, &s, `SELECT state FROM parked WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ?`,
		v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrUnknownVersion
	}

	return s, err
}

// PrepareAck readies the acknowledgement of the delivery d under the capture row (parking.md *Resolution*):
// its copy row inserted or found, so every message of a parked version is accounted for, and the version's state read;
// for a terminated version, audit is written in the same transaction naming the termination's action,
// so the acknowledgement that follows is recorded under the state it was decided on, and a redelivery acknowledged again writes its own.
// A version with no parked row, staged by a receipt alone or by a registered pack holding its seq, writes nothing and returns an empty state.
func (c *Catalog) PrepareAck(ctx context.Context, d *Identity, audit AuditEntry) (VersionState, error) {
	var state VersionState
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, d.Version.Capture); err != nil {
			return err
		}
		var err error
		state, err = readVersionState(ctx, tx, &d.Version)
		if errors.Is(err, ErrUnknownVersion) {
			state = ""
			return errRollback
		}
		if err != nil {
			return err
		}
		if err := c.insertCopy(ctx, tx, d); err != nil || state != VersionTerminated {
			return err
		}
		v := &d.Version
		err = tx.GetContext(ctx, &audit.ActionID, `SELECT terminated_by FROM parked WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ?`,
			v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])
		if err != nil {
			return err
		}
		audit.Capture = &v.Capture

		return c.auditIn(ctx, tx, audit)
	}))
	if err != nil {
		return "", fmt.Errorf("catalog: prepare ack %d/%d: %w", d.Incarnation, d.StreamSeq, err)
	}

	return state, nil
}

// AdvanceParked is step (3), parking → parked by compare-and-set, and returns the version's state:
// a version already past parking is left as it is.
func (c *Catalog) AdvanceParked(ctx context.Context, v *VersionKey) (VersionState, error) {
	return c.moveVersion(ctx, v, VersionParking, VersionParked)
}

// EvictParked moves a version whose hour retention evicted from parked to evicted-hour by compare-and-set,
// for the parked consumer whose registration the horizon guard refused, and returns the version's state.
func (c *Catalog) EvictParked(ctx context.Context, v *VersionKey) (VersionState, error) {
	return c.moveVersion(ctx, v, VersionParked, VersionEvictedHour)
}

func (c *Catalog) moveVersion(ctx context.Context, v *VersionKey, from, to VersionState) (VersionState, error) {
	var state VersionState
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, v.Capture); err != nil {
			return err
		}
		var err error
		if state, err = readVersionState(ctx, tx, v); err != nil {
			return err
		}
		if state != from {
			return errRollback
		}
		state = to
		_, err = tx.ExecContext(ctx, `UPDATE parked SET state = ? WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ? AND state = ?`,
			to, v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:], from)

		return err
	}))
	if err != nil {
		return "", fmt.Errorf("catalog: move %s.%d to %s: %w", v.Capture, v.Seq, to, err)
	}

	return state, nil
}

// RecordCopy inserts or finds the copy row of the parked copy d the parked consumer holds, before it disposes of it,
// and returns the version's state (parking.md *Two tables*).
func (c *Catalog) RecordCopy(ctx context.Context, d *Identity) (VersionState, error) {
	var state VersionState
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, d.Version.Capture); err != nil {
			return err
		}
		var err error
		if state, err = readVersionState(ctx, tx, &d.Version); err != nil {
			return err
		}

		return c.insertCopy(ctx, tx, d)
	}))
	if err != nil {
		return "", fmt.Errorf("catalog: record copy %d/%d: %w", d.Incarnation, d.StreamSeq, err)
	}

	return state, nil
}

// AckCopy marks the copy row of d acked once the server confirmed its acknowledgement, waiting → acked only,
// and sets the version's acked_at, metadata never a protection predicate, when no known copy of it waits any more.
func (c *Catalog) AckCopy(ctx context.Context, d *Identity) error {
	v := &d.Version
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, v.Capture); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE parked_copies SET state = 'acked' WHERE incarnation_id = ? AND stream_seq = ? AND state = 'waiting'`,
			d.Incarnation, d.StreamSeq)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE parked SET acked_at = ? WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ?
			AND NOT EXISTS (SELECT 1 FROM parked_copies k WHERE k.capture_id = parked.capture_id AND k.seq = parked.seq
				AND k.header = parked.header AND k.payload_digest = parked.payload_digest AND k.state = 'waiting')`,
			c.now(), v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])

		return err
	})
	if err != nil {
		return fmt.Errorf("catalog: ack copy %d/%d: %w", d.Incarnation, d.StreamSeq, err)
	}

	return nil
}

// ResolvedWaitingCopies lists the waiting copy rows on incarnation whose version is staged or terminated, by stream sequence:
// the messages the reconciler may find acknowledged (parking.md *resolution*). A copy of an unresolved version is never listed.
func (c *Catalog) ResolvedWaitingCopies(ctx context.Context, incarnation int64) ([]Identity, error) {
	var rows []struct {
		StreamSeq int64  `db:"stream_seq"`
		Capture   []byte `db:"capture_id"`
		Seq       int64  `db:"seq"`
		Header    []byte `db:"header"`
		Digest    []byte `db:"payload_digest"`
		Hour      int64  `db:"hour"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT k.stream_seq, k.capture_id, k.seq, k.header, k.payload_digest, v.hour
		FROM parked_copies k JOIN parked v ON v.capture_id = k.capture_id AND v.seq = k.seq AND v.header = k.header AND v.payload_digest = k.payload_digest
		WHERE k.incarnation_id = ? AND k.state = 'waiting' AND v.state IN ('staged', 'terminated') ORDER BY k.stream_seq`, incarnation)
	if err != nil {
		return nil, fmt.Errorf("catalog: waiting copies of %d: %w", incarnation, err)
	}
	out := make([]Identity, 0, len(rows))
	for _, r := range rows {
		d := Identity{Incarnation: incarnation, StreamSeq: r.StreamSeq, Hour: r.Hour}
		d.Version.Capture, d.Version.Seq = tracepack.UUID(r.Capture), uint64(r.Seq) //nolint:gosec // a stored seq is below 2^63
		copy(d.Version.Header[:], r.Header)
		copy(d.Version.Digest[:], r.Digest)
		out = append(out, d)
	}

	return out, nil
}

// ReconcileCopy marks the waiting copy row of d acked for the reconciler, which found its message gone from a work-queue stream,
// and sets the version's acked_at when no copy of it waits any longer.
// Under the capture row it re-reads that the version is staged or terminated and that d's incarnation is not retired,
// so it never touches a copy of an unresolved version or of a retired stream; ok reports that it marked the row.
// It runs under the compactor lease, which a lost lease ends with ErrLeaseLost.
func (c *Catalog) ReconcileCopy(ctx context.Context, d *Identity, lease Lease) (bool, error) {
	v := &d.Version
	var ok bool
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, v.Capture); err != nil {
			return err
		}
		if err := c.checkLease(ctx, tx, lease); err != nil {
			return err
		}
		state, err := readVersionState(ctx, tx, v)
		if errors.Is(err, ErrUnknownVersion) || (err == nil && !state.Resolved()) {
			return errRollback
		}
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE parked_copies SET state = 'acked'
			WHERE incarnation_id = ? AND stream_seq = ? AND state = 'waiting' AND capture_id = ? AND seq = ? AND header = ? AND payload_digest = ?
			AND EXISTS (SELECT 1 FROM bus_streams s WHERE s.incarnation_id = parked_copies.incarnation_id AND s.retired_at IS NULL)`,
			d.Incarnation, d.StreamSeq, v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])
		if err := affected(res, err, errRollback); err != nil {
			return err
		}
		ok = true
		_, err = tx.ExecContext(ctx, `UPDATE parked SET acked_at = ? WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ?
			AND NOT EXISTS (SELECT 1 FROM parked_copies k WHERE k.capture_id = parked.capture_id AND k.seq = parked.seq
				AND k.header = parked.header AND k.payload_digest = parked.payload_digest AND k.state = 'waiting')`,
			c.now(), v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])

		return err
	}))
	if err != nil {
		return false, fmt.Errorf("catalog: reconcile copy %d/%d: %w", d.Incarnation, d.StreamSeq, err)
	}

	return ok, nil
}

// TerminateParked is the operator's parked terminate (parking.md *Operator termination*):
// every unresolved version of capture, or of its seq when seq is not nil, written terminated under action,
// with the action's audit row and a terminated event for each hour it touches. It returns how many it terminated;
// an action that terminates nothing writes nothing.
func (c *Catalog) TerminateParked(ctx context.Context, capture tracepack.UUID, seq *uint64, actor, action string) (int, error) {
	var n int
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, capture); err != nil {
			return err
		}
		var evs []event
		var err error
		if n, evs, err = c.terminateVersions(ctx, tx, capture, seq, action); err != nil {
			return err
		}
		if n == 0 {
			return errRollback
		}

		return c.auditAndEmit(ctx, tx, capture, actor, "parked-terminate", action, n, evs)
	}))
	if err != nil {
		return 0, fmt.Errorf("catalog: terminate parked %s: %w", capture, err)
	}

	return n, nil
}

// TerminateCapture is the operator's capture terminate: TerminateParked of every unresolved version of capture,
// and an active termination order covering its messages on incarnation up to cutOff,
// the traffic stream's last sequence read by the caller before this transaction.
func (c *Catalog) TerminateCapture(ctx context.Context, capture tracepack.UUID, incarnation, cutOff int64, actor, action string) (int, error) {
	var n int
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		if _, err := c.lockCapture(ctx, tx, capture); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO termination_orders (action_id, capture_id, incarnation_id, cut_off, state) VALUES (?, ?, ?, ?, 'active')`,
			action, capture[:], incarnation, cutOff)
		if err != nil {
			return err
		}
		var evs []event
		if n, evs, err = c.terminateVersions(ctx, tx, capture, nil, action); err != nil {
			return err
		}

		return c.auditAndEmit(ctx, tx, capture, actor, "capture-terminate", action, n, evs)
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: terminate capture %s: %w", capture, err)
	}

	return n, nil
}

// terminateVersions terminates capture's unresolved versions, of seq when it is not nil, under action,
// and returns how many with the terminated events of their hours, which the caller emits last.
func (c *Catalog) terminateVersions(ctx context.Context, tx *sqlx.Tx, capture tracepack.UUID, seq *uint64, action string) (int, []event, error) {
	where := `capture_id = ? AND state IN ('parking', 'parked', 'evicted-hour')`
	args := []any{capture[:]}
	if seq != nil {
		where += ` AND seq = ?`
		args = append(args, int64(*seq)) //nolint:gosec // a seq the catalog stores fits
	}
	var hours []int64
	if err := tx.SelectContext(ctx, &hours, `SELECT hour FROM parked WHERE `+where, args...); err != nil {
		return 0, nil, err
	}
	if len(hours) == 0 {
		return 0, nil, nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE parked SET state = 'terminated', terminated_by = ?, resolved_at = ? WHERE `+where, append([]any{action, c.now()}, args...)...)
	if err != nil {
		return 0, nil, err
	}
	evs, err := c.terminatedScopes(ctx, tx, capture, hours)

	return len(hours), evs, err
}

// auditAndEmit writes the operator action's audit row, then its events, the transaction's last statement.
func (c *Catalog) auditAndEmit(ctx context.Context, tx *sqlx.Tx, capture tracepack.UUID, actor, verb, action string, n int, evs []event) error {
	err := c.auditIn(ctx, tx, AuditEntry{Actor: actor, Action: verb, ActionID: action, Capture: &capture, Detail: fmt.Sprintf("%d versions", n)})
	if err != nil {
		return err
	}

	return c.emit(ctx, tx, evs)
}

// SpendOrders marks spent the active orders of incarnation whose cut-off the stream's first sequence passed:
// no message they cover can exist any more. It returns how many it spent.
func (c *Catalog) SpendOrders(ctx context.Context, incarnation, firstSeq int64) (int, error) {
	var n int64
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE termination_orders SET state = 'spent' WHERE incarnation_id = ? AND state = 'active' AND cut_off < ?`,
			incarnation, firstSeq)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()

		return err
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: spend orders of %d: %w", incarnation, err)
	}

	return int(n), nil
}

// AdoptStream is the catalog's part of bus adopt-stream (parking.md *incarnations*), the last step of a stop-the-world replacement:
// refused while a membership or the compactor lease is live, it retires role's live incarnation, marks its waiting copy rows lost
// and its active orders spent, and records stream, created at createdNS, as the new incarnation.
func (c *Catalog) AdoptStream(ctx context.Context, role, stream string, createdNS int64, actor string) (Incarnation, error) {
	inc := Incarnation{Role: role, Stream: stream, CreatedNS: createdNS}
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		var live int
		err := tx.GetContext(ctx, &live, `SELECT (SELECT COUNT(*) FROM consumers WHERE lease_until >= `+c.d.nowNS+`)
			+ (SELECT COUNT(*) FROM leases WHERE until_ns >= `+c.d.nowNS+`)`)
		if err != nil {
			return err
		}
		if live > 0 {
			return ErrLeaseLive
		}
		var old []int64
		if err := tx.SelectContext(ctx, &old, `SELECT incarnation_id FROM bus_streams WHERE role = ? AND retired_at IS NULL`, role); err != nil {
			return err
		}
		now := c.now()
		for _, id := range old {
			if _, err := tx.ExecContext(ctx, `UPDATE bus_streams SET retired_at = ? WHERE incarnation_id = ?`, now, id); err != nil {
				return err
			}
			// A retired incarnation's messages cannot return; an acked copy stays acked.
			if _, err := tx.ExecContext(ctx, `UPDATE parked_copies SET state = 'lost' WHERE incarnation_id = ? AND state = 'waiting'`, id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE termination_orders SET state = 'spent' WHERE incarnation_id = ? AND state = 'active'`, id); err != nil {
				return err
			}
		}
		if err := tx.GetContext(ctx, &inc.ID, `SELECT COALESCE(MAX(incarnation_id), 0) + 1 FROM bus_streams`); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO bus_streams (incarnation_id, role, stream_name, created_ns, last_seq_seen) VALUES (?, ?, ?, ?, 0)`,
			inc.ID, role, stream, createdNS)
		if err != nil {
			return err
		}

		return c.auditIn(ctx, tx, AuditEntry{Actor: actor, Action: "adopt-stream", Detail: fmt.Sprintf("%s %s: incarnation %d, retired %v", role, stream, inc.ID, old)})
	})
	if err != nil {
		return Incarnation{}, fmt.Errorf("catalog: adopt %s stream %s: %w", role, stream, err)
	}

	return inc, nil
}

// ParkedVersion is a parked version with its messages, as parked list and capture show read it.
type ParkedVersion struct {
	Version      VersionKey
	State        VersionState
	Reason       string
	Hour         int64
	ObjectKey    string
	TerminatedBy string
	ParkedAt     int64
	// ResolvedAt and AckedAt are zero when unset.
	ResolvedAt, AckedAt int64
	Copies              []ParkedCopy
}

// ParkedCopy is one message of a parked version, on either stream.
type ParkedCopy struct {
	Incarnation, StreamSeq int64
	State                  string
}

// ParkedVersions returns the parked versions of capture, or of every capture when capture is nil, each with its copies,
// ordered by capture and seq.
func (c *Catalog) ParkedVersions(ctx context.Context, capture *tracepack.UUID) ([]ParkedVersion, error) {
	where, args := "", []any{}
	if capture != nil {
		where, args = " WHERE p.capture_id = ?", []any{capture[:]}
	}
	var rows []struct {
		Capture      []byte         `db:"capture_id"`
		Seq          int64          `db:"seq"`
		Header       []byte         `db:"header"`
		Digest       []byte         `db:"payload_digest"`
		State        VersionState   `db:"state"`
		Reason       string         `db:"reason"`
		Hour         int64          `db:"hour"`
		ObjectKey    sql.NullString `db:"object_key"`
		TerminatedBy sql.NullString `db:"terminated_by"`
		ParkedAt     int64          `db:"parked_at"`
		ResolvedAt   sql.NullInt64  `db:"resolved_at"`
		AckedAt      sql.NullInt64  `db:"acked_at"`
		Incarnation  sql.NullInt64  `db:"incarnation_id"`
		StreamSeq    sql.NullInt64  `db:"stream_seq"`
		CopyState    sql.NullString `db:"copy_state"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT p.capture_id, p.seq, p.header, p.payload_digest, p.state, p.reason, p.hour, p.object_key,
		p.terminated_by, p.parked_at, p.resolved_at, p.acked_at, k.incarnation_id, k.stream_seq, k.state AS copy_state
		FROM parked p LEFT JOIN parked_copies k ON k.capture_id = p.capture_id AND k.seq = p.seq AND k.header = p.header AND k.payload_digest = p.payload_digest`+
		where+` ORDER BY p.capture_id, p.seq, p.header, p.payload_digest, k.incarnation_id, k.stream_seq`, args...)
	if err != nil {
		return nil, fmt.Errorf("catalog: parked versions: %w", err)
	}
	var out []ParkedVersion
	for _, r := range rows {
		v := VersionKey{Capture: tracepack.UUID(r.Capture), Seq: uint64(r.Seq), Header: [44]byte(r.Header), Digest: [32]byte(r.Digest)} //nolint:gosec // a stored seq is not negative
		if n := len(out); n == 0 || out[n-1].Version != v {
			out = append(out, ParkedVersion{
				Version: v, State: r.State, Reason: r.Reason, Hour: r.Hour, ObjectKey: r.ObjectKey.String, TerminatedBy: r.TerminatedBy.String,
				ParkedAt: r.ParkedAt, ResolvedAt: r.ResolvedAt.Int64, AckedAt: r.AckedAt.Int64,
			})
		}
		if r.Incarnation.Valid {
			last := &out[len(out)-1]
			last.Copies = append(last.Copies, ParkedCopy{Incarnation: r.Incarnation.Int64, StreamSeq: r.StreamSeq.Int64, State: r.CopyState.String})
		}
	}

	return out, nil
}

// TerminationOrder is a capture's termination order (parking.md §7.1).
type TerminationOrder struct {
	ActionID    string `db:"action_id"`
	Incarnation int64  `db:"incarnation_id"`
	CutOff      int64  `db:"cut_off"`
	State       string `db:"state"`
}

// TerminationOrders returns capture's termination orders, as capture show lists them.
func (c *Catalog) TerminationOrders(ctx context.Context, capture tracepack.UUID) ([]TerminationOrder, error) {
	var out []TerminationOrder
	err := c.read.SelectContext(ctx, &out, `SELECT action_id, incarnation_id, cut_off, state FROM termination_orders WHERE capture_id = ? ORDER BY action_id`, capture[:])
	if err != nil {
		return nil, fmt.Errorf("catalog: termination orders of %s: %w", capture, err)
	}

	return out, nil
}

// ParkedCounts counts the parked versions not yet resolved, by the reason they were parked for, for the parked_messages metric.
func (c *Catalog) ParkedCounts(ctx context.Context) (map[string]int, error) {
	var rows []struct {
		Reason string `db:"reason"`
		N      int    `db:"n"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT reason, COUNT(*) AS n FROM parked WHERE state IN ('parking', 'parked', 'evicted-hour') GROUP BY reason`)
	if err != nil {
		return nil, fmt.Errorf("catalog: parked counts: %w", err)
	}
	out := make(map[string]int, len(rows))
	for _, r := range rows {
		out[r.Reason] = r.N
	}

	return out, nil
}
