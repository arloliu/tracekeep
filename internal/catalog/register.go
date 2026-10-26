package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// closeSentinel is the seq of a close record, which is never a range (ingest.md §6.3).
const closeSentinel = 1<<63 - 1

// PackOutcome is what a registration did (catalog.md §7.2 `RegisterPack`).
type PackOutcome int

// The registration's outcomes.
const (
	// Registered: the pack and its rows are in the catalog.
	Registered PackOutcome = iota + 1
	// AlreadyRegistered: the pack_id was registered before; the registration that committed it won every race.
	AlreadyRegistered
	// FoundAdopted: a caller with evidence found its pack registered by the reconciler, which wrote no receipts:
	// it leaves its batch unacknowledged, so the redelivery writes the evidence into another segment (T-32).
	FoundAdopted
	// Tombstoned: the key has a tombstone; nothing was registered.
	Tombstoned
	// BeyondHorizon: the pack's hour is past the retention horizon; the key is now tombstoned and the caller deletes the object.
	BeyondHorizon
	// PendingCapture: the reconciler's orphan belongs to a capture no start has admitted; it stays unregistered (storage.md §5.4).
	PendingCapture
)

// The roles a pack plays.
const (
	RoleSegment  = "segment"
	RoleConflict = "conflict"
	RoleArchive  = "archive"
	RoleClose    = "close"
)

// Range is an exact run of seqs a pack holds, with the times of its first and last record.
type Range struct {
	SeqFrom, SeqTo uint64
	TSFrom, TSTo   int64
}

// Loss is one of a pack's coverage entries: a declared loss.
type Loss struct {
	SeqFrom, SeqTo uint64
	TSFrom, TSTo   *int64
	Reason         string
}

// Version names the packs a batch saw holding another version of seq: a conflict the batch split (ingest.md §6.2).
type Version struct {
	Seq  uint64
	Pack tracepack.UUID
}

// Receipt is the staged proof of a record that travelled by claim-check: its version and its claim object.
type Receipt struct {
	Seq           uint64
	Header        [44]byte
	PayloadSHA256 [32]byte
	ObjectKey     string
	ObjectSize    int64
}

// Evidence is what only ingest passes: the receipts of its claim-check records.
// A registration without evidence (the reconciler's, a close's) writes coverage only.
type Evidence struct {
	Receipts []Receipt
}

// Pack is a pack to register.
type Pack struct {
	ID          tracepack.UUID
	Capture     tracepack.UUID
	ToolID      string
	Hour        int64
	Role        string
	Key         string
	Size        int64
	SeqStart    uint64
	FormatMinor int
	Ranges      []Range
	Losses      []Loss
	// Stop reports a stop boundary in the pack.
	Stop bool
	// Conflicts are the other versions the batch put in other packs.
	Conflicts []Version
}

// Registration is how a pack is registered.
type Registration struct {
	// Evidence is ingest's; nil for the reconciler and a close.
	Evidence *Evidence
	// Lease is the compactor's, which the reconciler and a close register under; nil for ingest.
	Lease *Lease
	// Adopted marks the reconciler's registration of an orphan.
	Adopted bool
	// HorizonHour is the oldest hour retention keeps; a pack of an older hour is never registered.
	HorizonHour int64
}

// RegisterPack registers p, idempotent on its pack_id, in the transaction ingest and the reconciler share (ingest.md §6.3).
func (c *Catalog) RegisterPack(ctx context.Context, p *Pack, r Registration) (PackOutcome, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	var out PackOutcome
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		var err error
		out, err = c.registerIn(ctx, tx, p, r)

		return err
	})
	if err != nil {
		return 0, fmt.Errorf("catalog: register pack %s: %w", p.ID, err)
	}

	return out, nil
}

func (p *Pack) check() error {
	if len(p.Ranges) == 0 {
		return fmt.Errorf("catalog: pack %s holds no range", p.ID)
	}
	for _, rg := range p.Ranges {
		if rg.SeqFrom > rg.SeqTo || rg.SeqTo >= closeSentinel {
			return fmt.Errorf("catalog: pack %s: invalid range %d-%d", p.ID, rg.SeqFrom, rg.SeqTo)
		}
	}

	return nil
}

func (c *Catalog) registerIn(ctx context.Context, tx *sqlx.Tx, p *Pack, r Registration) (PackOutcome, error) {
	row, err := c.lockCapture(ctx, tx, p.Capture)
	if err != nil {
		return 0, err
	}
	if r.Lease != nil {
		if err := c.checkLease(ctx, tx, *r.Lease); err != nil {
			return 0, err
		}
	}
	if out, done, err := c.guards(ctx, tx, p, r, row.State); done || err != nil {
		return out, err
	}
	if err := c.insertPack(ctx, tx, p, r); err != nil {
		return 0, err
	}
	scope, err := c.upsertScope(ctx, tx, p)
	if err != nil {
		return 0, err
	}
	evs := scope.events
	conflicts, err := c.recordConflicts(ctx, tx, p, scope.revision)
	if err != nil {
		return 0, err
	}
	evs = append(evs, conflicts...)
	if r.Evidence != nil {
		if err := c.insertReceipts(ctx, tx, p, r.Evidence); err != nil {
			return 0, err
		}
	}
	if err := c.updateCapture(ctx, tx, p, row); err != nil {
		return 0, err
	}

	return Registered, c.emit(ctx, tx, evs)
}

// guards runs the checks that end a registration before it writes the pack: done reports that out is final.
func (c *Catalog) guards(ctx context.Context, tx *sqlx.Tx, p *Pack, r Registration, state string) (PackOutcome, bool, error) {
	var adopted bool
	err := tx.GetContext(ctx, &adopted, `SELECT adopted FROM packs WHERE pack_id = ?`, p.ID[:])
	switch {
	case err == nil && adopted && r.Evidence != nil:
		return FoundAdopted, true, nil
	case err == nil:
		return AlreadyRegistered, true, nil
	case !errors.Is(err, sql.ErrNoRows):
		return 0, true, err
	}
	var n int
	if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, p.Key); err != nil {
		return 0, true, err
	}
	if n > 0 {
		return Tombstoned, true, nil
	}
	if p.Hour < r.HorizonHour {
		// The tombstone commits alone, before the caller deletes the object, so a delayed put never registers.
		_, err := tx.ExecContext(ctx, `INSERT INTO tombstones (object_key, reason, at) VALUES (?, 'beyond-retention', ?)`, p.Key, c.now())

		return BeyondHorizon, true, err
	}
	if r.Evidence == nil && r.Adopted && state == StatePending {
		return PendingCapture, true, nil
	}

	return 0, false, nil
}

func (c *Catalog) insertPack(ctx context.Context, tx *sqlx.Tx, p *Pack, r Registration) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO packs (pack_id, capture_id, tool_id, hour, role, object_key, size, seq_start, format_minor,
		state, adopted, registered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'staged', ?, ?)`,
		p.ID[:], p.Capture[:], p.ToolID, p.Hour, p.Role, p.Key, p.Size, int64(p.SeqStart), p.FormatMinor, r.Adopted, c.now())
	if err != nil {
		return err
	}
	for _, rg := range p.Ranges {
		_, err := tx.ExecContext(ctx, `INSERT INTO pack_ranges (pack_id, capture_id, seq_from, seq_to, ts_from, ts_to) VALUES (?, ?, ?, ?, ?, ?)`,
			p.ID[:], p.Capture[:], int64(rg.SeqFrom), int64(rg.SeqTo), rg.TSFrom, rg.TSTo)
		if err != nil {
			return err
		}
	}
	for i, l := range p.Losses {
		_, err := tx.ExecContext(ctx, `INSERT INTO pack_losses (pack_id, ord, seq_from, seq_to, ts_from, ts_to, reason) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			p.ID[:], i, int64(l.SeqFrom), int64(l.SeqTo), l.TSFrom, l.TSTo, l.Reason)
		if err != nil {
			return err
		}
	}

	return nil
}

type scopeChange struct {
	revision int64
	events   []event
}

// upsertScope advances the pack's scope: every registration increments its revision;
// a new scope is open and emits created, a settled one becomes reopened and emits reopened (§7.3).
func (c *Catalog) upsertScope(ctx context.Context, tx *sqlx.Tx, p *Pack) (scopeChange, error) {
	var cur struct {
		State    string `db:"state"`
		Revision int64  `db:"revision"`
	}
	err := tx.GetContext(ctx, &cur, `SELECT state, revision FROM scopes WHERE capture_id = ? AND hour = ?`, p.Capture[:], p.Hour)
	if errors.Is(err, sql.ErrNoRows) {
		_, err := tx.ExecContext(ctx, `INSERT INTO scopes (capture_id, hour, state, revision) VALUES (?, ?, 'open', 1)`, p.Capture[:], p.Hour)

		return scopeChange{revision: 1, events: []event{{p.Capture, p.Hour, "created", 1}}}, err
	}
	if err != nil {
		return scopeChange{}, err
	}
	ch := scopeChange{revision: cur.Revision + 1}
	state := cur.State
	if state == "settled" {
		state = "reopened"
		ch.events = append(ch.events, event{p.Capture, p.Hour, "reopened", ch.revision})
	}
	_, err = tx.ExecContext(ctx, `UPDATE scopes SET state = ?, revision = ? WHERE capture_id = ? AND hour = ?`, state, ch.revision, p.Capture[:], p.Hour)

	return ch, err
}

// recordConflicts writes the conflicts the batch split, confirmed, and a suspect for every range of another live pack of the capture,
// in any hour, that overlaps one of the pack's (ingest.md §6.2);
// each scope a conflict touches advances its revision and emits a conflict event, the pack's own scope at the revision upsertScope gave it.
func (c *Catalog) recordConflicts(ctx context.Context, tx *sqlx.Tx, p *Pack, ownRevision int64) ([]event, error) {
	touched := map[int64]bool{}
	for _, v := range p.Conflicts {
		var hour int64
		if err := tx.GetContext(ctx, &hour, `SELECT hour FROM packs WHERE pack_id = ?`, v.Pack[:]); err != nil {
			return nil, fmt.Errorf("conflicting pack %s: %w", v.Pack, err)
		}
		if err := c.insertConflict(ctx, tx, p, int64(v.Seq), int64(v.Seq), v.Pack, "confirmed"); err != nil {
			return nil, err
		}
		touched[hour] = true
	}
	for _, rg := range p.Ranges {
		var overlaps []struct {
			Pack []byte `db:"pack_id"`
			From int64  `db:"seq_from"`
			To   int64  `db:"seq_to"`
			Hour int64  `db:"hour"`
		}
		err := tx.SelectContext(ctx, &overlaps, `SELECT r.pack_id, r.seq_from, r.seq_to, k.hour FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
			WHERE r.capture_id = ? AND r.pack_id <> ? AND k.state <> 'deleted' AND r.seq_from <= ? AND r.seq_to >= ?`,
			p.Capture[:], p.ID[:], int64(rg.SeqTo), int64(rg.SeqFrom))
		if err != nil {
			return nil, err
		}
		for _, o := range overlaps {
			other := tracepack.UUID(o.Pack)
			from, to := max(o.From, int64(rg.SeqFrom)), min(o.To, int64(rg.SeqTo))
			if err := c.insertConflict(ctx, tx, p, from, to, other, "suspect"); err != nil {
				return nil, err
			}
			touched[o.Hour] = true
		}
	}
	if len(touched) == 0 {
		return nil, nil
	}
	evs := []event{{p.Capture, p.Hour, "conflict", ownRevision}}
	for hour := range touched {
		if hour == p.Hour {
			continue
		}
		var rev int64
		if _, err := tx.ExecContext(ctx, `UPDATE scopes SET revision = revision + 1 WHERE capture_id = ? AND hour = ?`, p.Capture[:], hour); err != nil {
			return nil, err
		}
		if err := tx.GetContext(ctx, &rev, `SELECT revision FROM scopes WHERE capture_id = ? AND hour = ?`, p.Capture[:], hour); err != nil {
			return nil, err
		}
		evs = append(evs, event{p.Capture, hour, "conflict", rev})
	}

	return evs, nil
}

func (c *Catalog) insertConflict(ctx context.Context, tx *sqlx.Tx, p *Pack, from, to int64, other tracepack.UUID, state string) error {
	_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO conflicts (capture_id, seq_from, seq_to, pack_id, other_pack_id, state) VALUES (?, ?, ?, ?, ?, ?)`,
		p.Capture[:], from, to, p.ID[:], other[:], state)

	return err
}

// insertReceipts writes a receipt and a claim object row for every record of the pack that travelled by claim-check.
func (c *Catalog) insertReceipts(ctx context.Context, tx *sqlx.Tx, p *Pack, e *Evidence) error {
	for _, rc := range e.Receipts {
		_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO claim_records (capture_id, seq, header, payload_sha256, hour, first_pack_id)
			VALUES (?, ?, ?, ?, ?, ?)`, p.Capture[:], int64(rc.Seq), rc.Header[:], rc.PayloadSHA256[:], p.Hour, p.ID[:])
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, c.d.insertIgnore+` INTO claim_objects (object_key, capture_id, seq, hour, size, state)
			VALUES (?, ?, ?, ?, ?, 'live')`, rc.ObjectKey, p.Capture[:], int64(rc.Seq), p.Hour, rc.ObjectSize)
		if err != nil {
			return err
		}
	}

	return nil
}

// updateCapture records the registration on the capture: its time, its highest producer seq with that record's time,
// and a stop, which moves an open capture to stopped (a closed one stays closed).
func (c *Catalog) updateCapture(ctx context.Context, tx *sqlx.Tx, p *Pack, row captureRow) error {
	top := p.Ranges[0]
	for _, rg := range p.Ranges[1:] {
		if rg.SeqTo > top.SeqTo {
			top = rg
		}
	}
	seq, ts := row.HighestSeq, row.HighestSeqTS
	if !seq.Valid || int64(top.SeqTo) > seq.Int64 {
		seq, ts = sql.NullInt64{Int64: int64(top.SeqTo), Valid: true}, sql.NullInt64{Int64: top.TSTo, Valid: true}
	}
	state := row.State
	stop := row.StopSeen || p.Stop
	if p.Stop && state == StateOpen {
		state = StateStopped
	}
	_, err := tx.ExecContext(ctx, `UPDATE captures SET last_registered_at = ?, highest_seq = ?, highest_seq_ts = ?, stop_seen = ?, state = ?
		WHERE capture_id = ?`, c.now(), seq, ts, stop, state, p.Capture[:])

	return err
}
