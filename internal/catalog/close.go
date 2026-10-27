package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// The triggers of a close (compactor.md §8.2).
const (
	// CloseInactivity: no registration for the inactivity period I.
	CloseInactivity = "inactivity"
	// CloseSuccessor: a later capture of the recorder instance registered for at least Q, and none of the capture's own for Q.
	CloseSuccessor = "successor"
	// CloseOperator: capture close, an operator's request (operations.md §10.4).
	CloseOperator = "operator"
)

// CloseCandidate is a capture the close scan found eligible, by its trigger.
type CloseCandidate struct {
	Capture tracepack.UUID
	Trigger string
	// TriggeredAt is the earliest successor's origin for the successor trigger, the close record's time; 0 for inactivity,
	// which closes at the compactor's clock.
	TriggeredAt int64
}

// registeredOrSeen is when the capture c was last registered, its first sighting for a capture never registered.
const registeredOrSeen = `COALESCE(c.last_registered_at, c.created_at)`

// successorOf joins to c every later capture n of its recorder instance with a registration made at or before the bound given.
const successorOf = `JOIN captures n ON n.tool_id = c.tool_id AND n.recorder_instance_id = c.recorder_instance_id AND n.origin_ns > c.origin_ns
	AND EXISTS (SELECT 1 FROM packs p WHERE p.capture_id = n.capture_id AND p.registered_at <= ?)`

// CloseCandidates lists the captures a close may end at now (compactor.md §8.2): open, without a stop,
// by successor when a later capture of the recorder instance was registered at least quiet ago and the capture not since,
// else by inactivity when it was not registered for inactivity. It is a read; CloseIntent judges again under the capture row.
func (c *Catalog) CloseCandidates(ctx context.Context, now int64, inactivity, quiet time.Duration) ([]CloseCandidate, error) {
	var bySuccessor []struct {
		Capture   []byte `db:"capture_id"`
		Successor int64  `db:"successor"`
	}
	err := c.read.SelectContext(ctx, &bySuccessor, `SELECT c.capture_id, MIN(n.origin_ns) AS successor FROM captures c `+successorOf+`
		WHERE c.state = 'open' AND NOT c.stop_seen AND `+registeredOrSeen+` < ? GROUP BY c.capture_id`,
		now-quiet.Nanoseconds(), now-quiet.Nanoseconds())
	if err != nil {
		return nil, fmt.Errorf("catalog: close candidates: %w", err)
	}
	out := make([]CloseCandidate, 0, len(bySuccessor))
	seen := map[tracepack.UUID]bool{}
	for _, r := range bySuccessor {
		id := tracepack.UUID(r.Capture)
		out = append(out, CloseCandidate{Capture: id, Trigger: CloseSuccessor, TriggeredAt: r.Successor})
		seen[id] = true
	}
	var inactive [][]byte
	err = c.read.SelectContext(ctx, &inactive, `SELECT c.capture_id FROM captures c
		WHERE c.state = 'open' AND NOT c.stop_seen AND `+registeredOrSeen+` < ?`, now-inactivity.Nanoseconds())
	if err != nil {
		return nil, fmt.Errorf("catalog: close candidates: %w", err)
	}
	for _, r := range inactive {
		if id := tracepack.UUID(r); !seen[id] {
			out = append(out, CloseCandidate{Capture: id, Trigger: CloseInactivity})
		}
	}

	return out, nil
}

// Intent is a close to begin: the trigger, judged at Now against Inactivity or Quiet, and every byte-affecting input the close is written with.
type Intent struct {
	Capture tracepack.UUID
	Trigger string
	// TriggeredAt is the close record's time: the successor's origin, or the compactor's clock.
	TriggeredAt int64
	// Now is the time the trigger is judged at, with the inactivity period and the quiet period.
	Now               int64
	Inactivity, Quiet time.Duration
	// PackID, WriterTime and Meta (the serialized pack metadata) fix the close's bytes;
	// ToleranceNS is the metadata's clock_step_tolerance_ns, which bounds the crash gap.
	PackID      tracepack.UUID
	WriterTime  int64
	Meta        []byte
	ToleranceNS int64
	// Request is the operator's request this close executes, ended in the intent's commit; nil otherwise.
	Request *tracepack.UUID
}

// CloseOutcome is what a close intent did (catalog.md §7.2 `CloseIntent`).
type CloseOutcome int

// The close intent's outcomes.
const (
	// Intended: the intent is recorded and the capture closed.
	Intended CloseOutcome = iota + 1
	// CloseIneligible: the capture is not, or no longer, eligible for the trigger; nothing was written.
	CloseIneligible
)

// CloseIntent records in under l (compactor.md §8.2 step 1): under the capture row, its state, stop and last registration re-read
// for the trigger, then the intent with the highest seq, its time and the crash gap they bound, and the capture closed;
// an operator's request ends in the same commit. An ineligible capture writes nothing; a lost lease returns ErrLeaseLost.
func (c *Catalog) CloseIntent(ctx context.Context, l Lease, in *Intent) (CloseOutcome, error) {
	var out CloseOutcome
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		row, err := c.lockCapture(ctx, tx, in.Capture)
		if err != nil {
			return err
		}
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		ok, err := c.closeEligible(ctx, tx, in, row)
		if err != nil {
			return err
		}
		if !ok {
			out = CloseIneligible
			return errRollback
		}
		out = Intended

		return c.intend(ctx, tx, in, row)
	}))
	if err != nil {
		return 0, fmt.Errorf("catalog: close intent of %s: %w", in.Capture, err)
	}

	return out, nil
}

// closeEligible re-reads, under the capture row, the trigger's conditions for in.
func (c *Catalog) closeEligible(ctx context.Context, tx *sqlx.Tx, in *Intent, row captureRow) (bool, error) {
	if row.State != StateOpen || row.StopSeen {
		return false, nil
	}
	var seen int64
	if err := tx.GetContext(ctx, &seen, `SELECT `+registeredOrSeen+` FROM captures c WHERE c.capture_id = ?`, in.Capture[:]); err != nil {
		return false, err
	}
	switch in.Trigger {
	case CloseOperator:
		return true, nil
	case CloseInactivity:
		return seen < in.Now-in.Inactivity.Nanoseconds(), nil
	case CloseSuccessor:
		if seen >= in.Now-in.Quiet.Nanoseconds() {
			return false, nil
		}
		var n int
		err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM captures c `+successorOf+` WHERE c.capture_id = ?`, in.Now-in.Quiet.Nanoseconds(), in.Capture[:])

		return n > 0, err
	default:
		return false, fmt.Errorf("catalog: unknown close trigger %q", in.Trigger)
	}
}

// intend writes the intent of in and closes the capture.
func (c *Catalog) intend(ctx context.Context, tx *sqlx.Tx, in *Intent, row captureRow) error {
	var gapStart, gapEnd *int64
	if row.HighestSeqTS.Valid {
		gapStart = checkedSub(row.HighestSeqTS.Int64, in.ToleranceNS)
	}
	gapEnd = checkedAdd(in.TriggeredAt, in.ToleranceNS)
	_, err := tx.ExecContext(ctx, `INSERT INTO capture_closes (capture_id, trigger_kind, triggered_at, highest_seq, highest_seq_ts, gap_start, gap_end,
		pack_id, writer_time, pack_meta, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')`,
		in.Capture[:], in.Trigger, in.TriggeredAt, row.HighestSeq, row.HighestSeqTS, gapStart, gapEnd, in.PackID[:], in.WriterTime, in.Meta)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE captures SET state = 'closed' WHERE capture_id = ? AND state = 'open' AND data_revision = ?`,
		in.Capture[:], row.DataRevision)
	if err := rowsWant(res, err, 1); err != nil {
		return err
	}
	if in.Request != nil {
		_, err := tx.ExecContext(ctx, `UPDATE compact_requests SET state = 'done', outcome = 'closed', done_at = ? WHERE request_id = ? AND state = 'pending'`,
			c.now(), in.Request[:])
		if err != nil {
			return err
		}
	}

	return nil
}

func checkedSub(a, b int64) *int64 {
	if (b > 0 && a < math.MinInt64+b) || (b < 0 && a > math.MaxInt64+b) {
		return nil
	}
	v := a - b

	return &v
}

func checkedAdd(a, b int64) *int64 {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return nil
	}
	v := a + b

	return &v
}

// Close is a pending close intent: every input its bytes are written from.
type Close struct {
	Capture     tracepack.UUID
	ToolID      string
	Trigger     string
	TriggeredAt int64
	// HighestSeq and HighestSeqTS are the capture's highest producer seq and its record's time at the intent; nil when none was staged.
	HighestSeq, HighestSeqTS *int64
	PackID                   tracepack.UUID
	WriterTime               int64
	Meta                     []byte
}

// PendingCloses lists the close intents not yet published or abandoned, which every compactor cycle retries.
func (c *Catalog) PendingCloses(ctx context.Context) ([]Close, error) {
	var rows []struct {
		Capture      []byte         `db:"capture_id"`
		ToolID       sql.NullString `db:"tool_id"`
		Trigger      string         `db:"trigger_kind"`
		TriggeredAt  int64          `db:"triggered_at"`
		HighestSeq   sql.NullInt64  `db:"highest_seq"`
		HighestSeqTS sql.NullInt64  `db:"highest_seq_ts"`
		PackID       []byte         `db:"pack_id"`
		WriterTime   int64          `db:"writer_time"`
		Meta         []byte         `db:"pack_meta"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT k.capture_id, c.tool_id, k.trigger_kind, k.triggered_at, k.highest_seq, k.highest_seq_ts,
		k.pack_id, k.writer_time, k.pack_meta FROM capture_closes k JOIN captures c ON c.capture_id = k.capture_id
		WHERE k.state = 'pending' ORDER BY k.triggered_at, k.capture_id`)
	if err != nil {
		return nil, fmt.Errorf("catalog: pending closes: %w", err)
	}
	out := make([]Close, len(rows))
	for i, r := range rows {
		out[i] = Close{
			Capture: tracepack.UUID(r.Capture), ToolID: r.ToolID.String, Trigger: r.Trigger, TriggeredAt: r.TriggeredAt,
			HighestSeq: nullable(r.HighestSeq), HighestSeqTS: nullable(r.HighestSeqTS), PackID: tracepack.UUID(r.PackID),
			WriterTime: r.WriterTime, Meta: r.Meta,
		}
	}

	return out, nil
}

// PublishClose publishes the pending close intent of p's capture under l (compactor.md §8.2 step 2), on the intent's pack_id,
// in one transaction under the capture row: the close registered as a pack holding no range, or found already registered
// (the reconciler may adopt the close's object first), and the intent published, the scope's event allocated last.
// The horizon guard refusing the pack tombstones its key and abandons the intent, and a key already tombstoned abandons it too;
// the caller then deletes the object. It returns the intent's state: published, abandoned, or the state it already had.
func (c *Catalog) PublishClose(ctx context.Context, l Lease, p *Pack, horizonHour int64) (string, error) {
	if p.Role != RoleClose || p.SeqStart != closeSentinel || len(p.Ranges) != 0 {
		return "", fmt.Errorf("%w: pack %s is no close", ErrInvalidRegistration, p.ID)
	}
	var state string
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		out, evs, err := c.registerIn(ctx, tx, p, Registration{Lease: &l, HorizonHour: horizonHour})
		if err != nil {
			return err
		}
		var intent struct {
			State  string `db:"state"`
			PackID []byte `db:"pack_id"`
		}
		if err := tx.GetContext(ctx, &intent, `SELECT state, pack_id FROM capture_closes WHERE capture_id = ?`, p.Capture[:]); err != nil {
			return fmt.Errorf("the close intent: %w", err)
		}
		if tracepack.UUID(intent.PackID) != p.ID {
			return fmt.Errorf("the intent names pack %s, not %s", tracepack.UUID(intent.PackID), p.ID)
		}
		if state = intent.State; state != "pending" {
			return errRollback
		}
		if out == AlreadyRegistered {
			// The registration's idempotence answers first; a key tombstoned since, its pack row deleted, still abandons the close.
			var n int
			if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, p.Key); err != nil {
				return err
			}
			if n > 0 {
				out = Tombstoned
			}
		}
		switch out {
		case Registered, AlreadyRegistered:
			state = "published"
		case Tombstoned, BeyondHorizon:
			state = "abandoned"
		case FoundAdopted, UnadmittedCapture, LostToTermination:
			// Outcomes of evidence or of an adoption, which a close's registration has neither.
			return fmt.Errorf("unexpected registration outcome %d", out)
		default:
			return fmt.Errorf("unknown registration outcome %d", out)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE capture_closes SET state = ? WHERE capture_id = ? AND state = 'pending'`, state, p.Capture[:]); err != nil {
			return err
		}

		return c.emit(ctx, tx, evs)
	}))
	if err != nil {
		return "", fmt.Errorf("catalog: publish the close of %s: %w", p.Capture, err)
	}

	return state, nil
}
