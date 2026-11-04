package catalog

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

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
	// UnadmittedCapture: the reconciler's orphan belongs to a capture no start has admitted, pending or rejected;
	// it stays unregistered (storage.md §5.4).
	UnadmittedCapture
	// LostToTermination: a version of the batch is terminated, or an active termination order covers one of its messages (T-32);
	// the key is now tombstoned, the caller deletes the object and returns the batch for its messages to be decided again.
	LostToTermination
)

// The roles a pack plays.
const (
	// RoleSegment is a flush's segment.
	RoleSegment = "segment"
	// RoleConflict is a conflict segment: a further version of seqs the flush's segment holds.
	RoleConflict = "conflict"
	// RoleArchive is a merge's output.
	RoleArchive = "archive"
	// RoleClose is a close, which holds only the close sentinel.
	RoleClose = "close"
)

// Range is an exact run of seqs a pack holds, with the times of its first and last record.
type Range struct {
	SeqFrom, SeqTo uint64
	TSFrom, TSTo   int64
}

// Loss is one of a pack's coverage entries: a declared loss, each bound as the entry has it;
// SeqTo is nil when the upper bound of the lost seqs is unknown, and a carried entry may lack any bound.
type Loss struct {
	SeqFrom, SeqTo *uint64
	TSFrom, TSTo   *int64
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

// Evidence is what only ingest passes: the identity of every delivery whose version the pack holds, equal copies included,
// and the receipts of its claim-check records.
// A registration without evidence (the reconciler's, a close's) writes coverage only.
type Evidence struct {
	Deliveries []Identity
	Receipts   []Receipt
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

// ErrInvalidRegistration refuses a pack or a registration mode the transaction cannot take; nothing was written.
var ErrInvalidRegistration = errors.New("catalog: invalid registration")

// RegisterPack registers p, idempotent on its pack_id, in the transaction ingest and the reconciler share (ingest.md §6.3).
// An outcome other than Registered writes nothing, except the tombstones of BeyondHorizon and LostToTermination.
func (c *Catalog) RegisterPack(ctx context.Context, p *Pack, r Registration) (PackOutcome, error) {
	out, _, err := c.RegisterPackGaps(ctx, p, r)

	return out, err
}

// RegisterPackGaps is RegisterPack, also returning the holes in the producer coverage p opened
// above the capture's highest seq before it, a missing prefix included (operations.md §10.4 gap_observations_total):
// zero for any outcome but Registered, and for a registration that did not commit.
func (c *Catalog) RegisterPackGaps(ctx context.Context, p *Pack, r Registration) (PackOutcome, int, error) {
	if err := p.check(r); err != nil {
		return 0, 0, err
	}
	var out PackOutcome
	var gaps int
	err := outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		var evs []event
		var err error
		out, gaps = 0, 0
		out, evs, err = c.registerIn(ctx, tx, p, r, &gaps)
		if err == nil && out != Registered && out != BeyondHorizon && out != LostToTermination {
			err = errRollback
		}
		if err != nil {
			return err
		}

		return c.emit(ctx, tx, evs)
	}))
	if err != nil {
		return 0, 0, fmt.Errorf("catalog: register pack %s: %w", p.ID, err)
	}

	return out, gaps, nil // gaps is set only on the path that registers, and returned only once it committed
}

// check refuses a pack whose seqs do not fit the catalog, and a registration mode no caller has:
// ingest passes evidence and no lease; the reconciler and a close register under the lease without evidence, and only the reconciler adopts.
func (p *Pack) check(r Registration) error {
	switch {
	case r.Evidence != nil && (r.Lease != nil || r.Adopted):
		return fmt.Errorf("%w: evidence with a lease or an adoption", ErrInvalidRegistration)
	case r.Evidence == nil && r.Lease == nil:
		return fmt.Errorf("%w: neither evidence nor the lease", ErrInvalidRegistration)
	case len(p.Ranges) == 0 && !p.closeOnly():
		// A close holds only the sentinel, which is never a range; so does an archive merged from closes alone.
		return fmt.Errorf("%w: pack %s holds no range", ErrInvalidRegistration, p.ID)
	}
	// A close starts at the sentinel, its one record; every other seq fits a signed BIGINT below it (catalog.md §7.1).
	if p.SeqStart > closeSentinel || (p.SeqStart == closeSentinel && !p.closeOnly()) {
		return fmt.Errorf("%w: pack %s: seq start %d", ErrInvalidRegistration, p.ID, p.SeqStart)
	}
	var seqs []uint64
	for _, rg := range p.Ranges {
		if rg.SeqFrom > rg.SeqTo {
			return fmt.Errorf("%w: pack %s: range %d-%d", ErrInvalidRegistration, p.ID, rg.SeqFrom, rg.SeqTo)
		}
		seqs = append(seqs, rg.SeqFrom, rg.SeqTo)
	}
	for _, l := range p.Losses {
		for _, b := range []*uint64{l.SeqFrom, l.SeqTo} {
			if b != nil {
				seqs = append(seqs, *b)
			}
		}
	}
	for _, v := range p.Conflicts {
		seqs = append(seqs, v.Seq)
	}
	if r.Evidence != nil {
		evSeqs, err := r.Evidence.seqs(p)
		if err != nil {
			return err
		}
		seqs = append(seqs, evSeqs...)
	}
	for _, seq := range seqs {
		if seq >= closeSentinel {
			return fmt.Errorf("%w: pack %s: seq %d", ErrInvalidRegistration, p.ID, seq)
		}
	}

	return nil
}

// closeOnly reports a pack holding only a close's record: a close, or an archive merged from closes alone, which starts at the sentinel.
func (p *Pack) closeOnly() bool {
	return p.Role == RoleClose || p.Role == RoleArchive && p.SeqStart == closeSentinel && len(p.Ranges) == 0
}

// seqs are the seqs the evidence names; a delivery of another capture than p's is refused.
func (e *Evidence) seqs(p *Pack) ([]uint64, error) {
	var seqs []uint64
	for _, rc := range e.Receipts {
		seqs = append(seqs, rc.Seq)
	}
	for _, d := range e.Deliveries {
		if d.Version.Capture != p.Capture {
			return nil, fmt.Errorf("%w: pack %s: a delivery of capture %s", ErrInvalidRegistration, p.ID, d.Version.Capture)
		}
		seqs = append(seqs, d.Version.Seq)
	}

	return seqs, nil
}

// registerIn runs the registration's statements and returns the events it emits, which the caller allocates last (§7.3);
// a registration sets *gaps, when gaps is not nil, to the holes its pack opens.
func (c *Catalog) registerIn(ctx context.Context, tx *sqlx.Tx, p *Pack, r Registration, gaps *int) (PackOutcome, []event, error) {
	row, err := c.lockCapture(ctx, tx, p.Capture)
	if err != nil {
		return 0, nil, err
	}
	if r.Lease != nil {
		if err := c.checkLease(ctx, tx, *r.Lease); err != nil {
			return 0, nil, err
		}
	}
	if out, done, err := c.guards(ctx, tx, p, r, row); done || err != nil {
		return out, nil, err
	}
	var stage []*VersionKey
	if r.Evidence != nil {
		// The last guard: check refuses evidence with an adoption, so it runs where guards ran it, after the adoption's.
		var out PackOutcome
		if out, stage, err = c.evidenceGuard(ctx, tx, p, r.Evidence); out != 0 || err != nil {
			return out, nil, err
		}
	}
	if err := c.insertPack(ctx, tx, p, r); err != nil {
		return 0, nil, err
	}
	scope, err := c.upsertScope(ctx, tx, p)
	if err != nil {
		return 0, nil, err
	}
	evs := scope.events
	conflicts, err := c.recordConflicts(ctx, tx, p, scope.revision)
	if err != nil {
		return 0, nil, err
	}
	evs = append(evs, conflicts...)
	if r.Evidence != nil {
		if err := c.insertReceipts(ctx, tx, p, r.Evidence); err != nil {
			return 0, nil, err
		}
		if err := c.stageParked(ctx, tx, stage); err != nil {
			return 0, nil, err
		}
	}
	if gaps != nil {
		*gaps = openedGaps(row.HighestSeq, p.Ranges)
	}
	if err := c.updateCapture(ctx, tx, p, row); err != nil {
		return 0, nil, err
	}

	return Registered, evs, nil
}

// guards runs the checks that end a registration before it writes the pack, the evidence's apart: done reports that out is final.
func (c *Catalog) guards(ctx context.Context, tx *sqlx.Tx, p *Pack, r Registration, row captureRow) (PackOutcome, bool, error) {
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
	if out, done, err := c.horizonGuard(ctx, tx, p, r, row); done || err != nil {
		return out, done, err
	}
	if r.Adopted {
		if out, done, err := c.adoptionGuard(ctx, tx, p); done || err != nil {
			return out, done, err
		}
	}
	if r.Adopted && row.State != StateOpen && row.State != StateStopped && row.State != StateClosed {
		return UnadmittedCapture, true, nil
	}

	return 0, false, nil
}

// horizonGuard refuses a pack of an hour older than the horizon, or of one retention evicted ranges of, as BeyondHorizon,
// and tombstones its key in this transaction.
func (c *Catalog) horizonGuard(ctx context.Context, tx *sqlx.Tx, p *Pack, r Registration, row captureRow) (PackOutcome, bool, error) {
	beyond := p.Hour < r.HorizonHour
	if !beyond {
		var err error
		if beyond, err = hourEvicted(ctx, tx, p.Capture, p.Hour); err != nil || !beyond {
			return 0, err != nil, err
		}
	}
	// The tombstone commits under the capture row, before the caller deletes the object,
	// so a put delayed across eviction never registers and no registration of the key slips in between.
	_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO tombstones (object_key, reason, at) VALUES (?, 'beyond-retention', ?)`, p.Key, c.now())
	if err == nil && row.DataRevision == 1 {
		// Revision 1 under the lock means this transaction inserted the placeholder: a stray leaves none.
		_, err = tx.ExecContext(ctx, `DELETE FROM captures WHERE capture_id = ? AND state = 'pending' AND data_revision = 1`, p.Capture[:])
	}

	return BeyondHorizon, true, err
}

// hourEvicted reports whether retention evicted ranges of capture's hour, which a caller's horizon read before that eviction still keeps:
// read under the capture row eviction takes, it refuses the hour whatever horizon the caller read (ingest.md §6.3).
func hourEvicted(ctx context.Context, tx *sqlx.Tx, capture tracepack.UUID, hour int64) (bool, error) {
	var n int
	err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM evicted_ranges WHERE capture_id = ? AND hour = ?`, capture[:], hour)

	return n > 0, err
}

// adoptionGuard refuses the reconciler's orphan holding a seq with a terminated parked row, or of a capture with a termination order in any state,
// and tombstones its key in this transaction: the orphan's messages were never acknowledged and are decided again at their redelivery (storage.md §5.4).
func (c *Catalog) adoptionGuard(ctx context.Context, tx *sqlx.Tx, p *Pack) (PackOutcome, bool, error) {
	var n int
	if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM termination_orders WHERE capture_id = ?`, p.Capture[:]); err != nil {
		return 0, true, err
	}
	for _, rg := range p.Ranges {
		if n > 0 {
			break
		}
		err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM parked WHERE capture_id = ? AND state = 'terminated' AND seq BETWEEN ? AND ?`,
			p.Capture[:], int64(rg.SeqFrom), int64(rg.SeqTo))
		if err != nil {
			return 0, true, err
		}
	}
	if n == 0 {
		return 0, false, nil
	}
	_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO tombstones (object_key, reason, at) VALUES (?, 'terminated', ?)`, p.Key, c.now())

	return LostToTermination, true, err
}

// KeyStanding is what the catalog knows of an object key.
type KeyStanding int

// The standings of a key.
const (
	// KeyUnknown: no pack names the key and no tombstone holds it.
	KeyUnknown KeyStanding = iota + 1
	// KeyRegistered: a pack names the key.
	KeyRegistered
	// KeyTombstoned: the key was deleted by retention, deletion or a refused registration, and is never registered.
	KeyTombstoned
)

// KeyStanding reads key's standing, the reconciler's first look at a listed key (storage.md §5.4).
// It is a read: the registration's own guards decide under the capture row.
func (c *Catalog) KeyStanding(ctx context.Context, key string) (KeyStanding, error) {
	var n int
	if err := c.read.GetContext(ctx, &n, `SELECT COUNT(*) FROM tombstones WHERE object_key = ?`, key); err != nil {
		return 0, fmt.Errorf("catalog: key %s: %w", key, err)
	}
	if n > 0 {
		return KeyTombstoned, nil
	}
	if err := c.read.GetContext(ctx, &n, `SELECT COUNT(*) FROM packs WHERE object_key = ?`, key); err != nil {
		return 0, fmt.Errorf("catalog: key %s: %w", key, err)
	}
	if n > 0 {
		return KeyRegistered, nil
	}

	return KeyUnknown, nil
}

// MaxKeyStandings is the most keys one KeyStandings asks.
const MaxKeyStandings = 128

// KeyStandings reads the standings of keys, in their order, in one statement on the read pool:
// the reconciler's look at a chunk of listed keys, each standing what KeyStanding reads, a tombstone before a pack.
// It asks at most MaxKeyStandings keys; it is a read, and the registration's own guards decide under the capture row.
func (c *Catalog) KeyStandings(ctx context.Context, keys []string) ([]KeyStanding, error) {
	if len(keys) > MaxKeyStandings {
		return nil, fmt.Errorf("catalog: standings of %d keys, above %d", len(keys), MaxKeyStandings)
	}
	out := make([]KeyStanding, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	in := " IN (?" + strings.Repeat(", ?", len(keys)-1) + ")"
	args := make([]any, 0, 2*len(keys))
	for range 2 {
		for _, k := range keys {
			args = append(args, k)
		}
	}
	var rows []struct {
		Key      string      `db:"object_key"`
		Standing KeyStanding `db:"standing"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT object_key, `+strconv.Itoa(int(KeyTombstoned))+` AS standing FROM tombstones WHERE object_key`+in+`
		UNION ALL SELECT object_key, `+strconv.Itoa(int(KeyRegistered))+` AS standing FROM packs WHERE object_key`+in, args...)
	if err != nil {
		return nil, fmt.Errorf("catalog: standings of %d keys: %w", len(keys), err)
	}
	found := make(map[string]KeyStanding, len(rows))
	for _, r := range rows {
		if found[r.Key] != KeyTombstoned {
			found[r.Key] = r.Standing
		}
	}
	for i, k := range keys {
		out[i] = cmp.Or(found[k], KeyUnknown)
	}

	return out, nil
}

// activeOrder is an active termination order of a capture: its incarnation and cut-off.
type activeOrder struct {
	Incarnation int64 `db:"incarnation_id"`
	CutOff      int64 `db:"cut_off"`
}

// evidenceGuard re-reads, under the capture row, the version of every delivery of the evidence, in sets (T-55):
// the capture's parked rows over the deliveries' seqs and its active termination orders, one read each,
// and a held probe for each delivery an order covers.
// In the deliveries' order, the first whose version is terminated, or whose message an active order covers with its seq not held,
// wins over the registration (T-32); so does the first whose version's hour retention evicted, which can no longer be staged (parking.md *Parking*).
// Either way out is that outcome and the key is tombstoned, in this transaction, before the caller deletes the object.
// Otherwise it returns the versions it found parking or parked, each once, for stageParked: the capture row keeps their state meanwhile.
func (c *Catalog) evidenceGuard(ctx context.Context, tx *sqlx.Tx, p *Pack, e *Evidence) (PackOutcome, []*VersionKey, error) {
	if len(e.Deliveries) == 0 {
		return 0, nil, nil
	}
	states, err := versionStates(ctx, tx, p.Capture, e.Deliveries)
	if err != nil {
		return 0, nil, err
	}
	var orders []activeOrder
	err = tx.SelectContext(ctx, &orders, `SELECT incarnation_id, cut_off FROM termination_orders WHERE capture_id = ? AND state = 'active'`, p.Capture[:])
	if err != nil {
		return 0, nil, err
	}
	var stage []*VersionKey
	staging := map[VersionKey]bool{}
	for i := range e.Deliveries {
		d := &e.Deliveries[i]
		state := states[d.Version]
		var out PackOutcome
		switch {
		case state == VersionStaged:
			continue
		case state == VersionTerminated:
			out = LostToTermination
		case slices.ContainsFunc(orders, func(o activeOrder) bool { return o.Incarnation == d.Incarnation && o.CutOff >= d.StreamSeq }):
			// An order never undoes a registration: a seq a registered pack holds is staged, as resolveIn reads it.
			var held bool
			seq := int64(d.Version.Seq) //nolint:gosec // a checked seq is below 2^63
			if err := tx.GetContext(ctx, &held, heldSeqQuery, p.Capture[:], seq, seq, seq, p.Capture[:]); err != nil {
				return 0, nil, err
			}
			if !held {
				out = LostToTermination
			}
		case state == VersionEvictedHour:
			out = BeyondHorizon
		default:
			// Unresolved, with no order covering its message: it registers, and a parking or parked version is staged.
		}
		if out != 0 {
			reason := "terminated"
			if out == BeyondHorizon {
				reason = "beyond-retention"
			}
			_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO tombstones (object_key, reason, at) VALUES (?, ?, ?)`, p.Key, reason, c.now())

			return out, nil, err
		}
		if (state == VersionParking || state == VersionParked) && !staging[d.Version] {
			staging[d.Version] = true
			stage = append(stage, &d.Version)
		}
	}

	return 0, stage, nil
}

// versionStates reads the states of capture's parked versions over the seqs ds name, in one read on the primary key's prefix;
// a version with no row is absent.
func versionStates(ctx context.Context, tx *sqlx.Tx, capture tracepack.UUID, ds []Identity) (map[VersionKey]VersionState, error) {
	lo, hi := ds[0].Version.Seq, ds[0].Version.Seq
	for i := range ds {
		lo, hi = min(lo, ds[i].Version.Seq), max(hi, ds[i].Version.Seq)
	}
	var rows []struct {
		Seq    int64        `db:"seq"`
		Header []byte       `db:"header"`
		Digest []byte       `db:"payload_digest"`
		State  VersionState `db:"state"`
	}
	err := tx.SelectContext(ctx, &rows, `SELECT seq, header, payload_digest, state FROM parked WHERE capture_id = ? AND seq BETWEEN ? AND ?`,
		capture[:], int64(lo), int64(hi)) //nolint:gosec // checked seqs are below 2^63
	if err != nil {
		return nil, err
	}
	states := make(map[VersionKey]VersionState, len(rows))
	for _, r := range rows {
		states[VersionKey{Capture: capture, Seq: uint64(r.Seq), Header: [44]byte(r.Header), Digest: [32]byte(r.Digest)}] = r.State //nolint:gosec // a stored seq is not negative
	}

	return states, nil
}

func (c *Catalog) insertPack(ctx context.Context, tx *sqlx.Tx, p *Pack, r Registration) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO packs (pack_id, capture_id, tool_id, hour, role, object_key, size, seq_start, format_minor,
		state, adopted, registered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'staged', ?, ?)`,
		p.ID[:], p.Capture[:], p.ToolID, p.Hour, p.Role, p.Key, p.Size, int64(p.SeqStart), p.FormatMinor, r.Adopted, c.now())
	if err != nil {
		return err
	}
	ranges := make([]any, 0, 6*len(p.Ranges))
	for _, rg := range p.Ranges {
		ranges = append(ranges, p.ID[:], p.Capture[:], int64(rg.SeqFrom), int64(rg.SeqTo), rg.TSFrom, rg.TSTo) //nolint:gosec // checked seqs are below 2^63
	}
	if err := insertValues(ctx, tx, `INSERT INTO pack_ranges (pack_id, capture_id, seq_from, seq_to, ts_from, ts_to)`, 6, ranges); err != nil {
		return err
	}
	losses := make([]any, 0, 6*len(p.Losses))
	for i, l := range p.Losses {
		losses = append(losses, p.ID[:], i, signed(l.SeqFrom), signed(l.SeqTo), l.TSFrom, l.TSTo)
	}

	return insertValues(ctx, tx, `INSERT INTO pack_losses (pack_id, ord, seq_from, seq_to, ts_from, ts_to)`, 6, losses)
}

// maxParams is the most placeholders a multi-row statement binds, every one of them counted.
const maxParams = 900

// insertValues runs insert, which names cols columns, with the rows args holds, cols values a row,
// in multi-row statements of at most maxParams placeholders on x, the transaction they all commit in.
func insertValues(ctx context.Context, x sqlx.ExecerContext, insert string, cols int, args []any) error {
	tuple := "(?" + strings.Repeat(", ?", cols-1) + ")"
	per := maxParams / cols * cols
	for len(args) > 0 {
		n := min(per, len(args))
		if _, err := x.ExecContext(ctx, insert+" VALUES "+tuple+strings.Repeat(", "+tuple, n/cols-1), args[:n]...); err != nil {
			return err
		}
		args = args[n:]
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

// spanBound bounds a probe of a capture's ranges r from below (catalog.md §7.1, T-55):
// a range meeting seqs at or above first has seq_to >= first and a span of at most the capture's largest stored one,
// so it starts at or above first minus that span, which the probe's own statement reads, so no range stored before it is missed.
// Its arguments are first and the capture; SQL subtracts in signed BIGINT, and a capture with no range has no span and matches nothing.
const spanBound = ` AND r.seq_from >= ? - (SELECT MAX(m.span) FROM pack_ranges m WHERE m.capture_id = ?)`

// overlapQuery finds the ranges of a capture's other live packs overlapping a range,
// its arguments the capture, the pack, the range's last and first seq, then spanBound's: the first seq and the capture.
const overlapQuery = `SELECT r.pack_id, r.seq_from, r.seq_to, k.hour FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
			WHERE r.capture_id = ? AND r.pack_id <> ? AND k.state <> 'deleted' AND r.seq_from <= ? AND r.seq_to >= ?` + spanBound

// recordConflicts writes the conflicts the batch split, confirmed, and a suspect for every range of another live pack of the capture,
// in any hour, that overlaps one of the pack's (ingest.md §6.2);
// each scope a conflict touches advances its revision and emits a conflict event, the pack's own scope at the revision upsertScope gave it.
func (c *Catalog) recordConflicts(ctx context.Context, tx *sqlx.Tx, p *Pack, ownRevision int64) ([]event, error) {
	touched := map[int64]bool{}
	// The batch's other versions are in conflict segments of the same scope (ingest.md §6.2), registered in any order.
	for _, v := range p.Conflicts {
		if err := c.insertConflict(ctx, tx, p, int64(v.Seq), int64(v.Seq), v.Pack, "confirmed"); err != nil {
			return nil, err
		}
		touched[p.Hour] = true
	}
	for _, rg := range p.Ranges {
		var overlaps []struct {
			Pack []byte `db:"pack_id"`
			From int64  `db:"seq_from"`
			To   int64  `db:"seq_to"`
			Hour int64  `db:"hour"`
		}
		err := tx.SelectContext(ctx, &overlaps, overlapQuery, p.Capture[:], p.ID[:], int64(rg.SeqTo), int64(rg.SeqFrom), int64(rg.SeqFrom), p.Capture[:])
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

// insertReceipts writes a receipt and a claim object row for every record of the pack that travelled by claim-check;
// a receipt already recorded keeps its first pack, and a claim object its row.
func (c *Catalog) insertReceipts(ctx context.Context, tx *sqlx.Tx, p *Pack, e *Evidence) error {
	records, objects := make([]any, 0, 6*len(e.Receipts)), make([]any, 0, 6*len(e.Receipts))
	for i := range e.Receipts {
		rc := &e.Receipts[i]
		seq := int64(rc.Seq) //nolint:gosec // a checked seq is below 2^63
		records = append(records, p.Capture[:], seq, rc.Header[:], rc.PayloadSHA256[:], p.Hour, p.ID[:])
		objects = append(objects, rc.ObjectKey, p.Capture[:], seq, p.Hour, rc.ObjectSize, "live")
	}
	err := insertValues(ctx, tx, c.d.insertIgnore+` INTO claim_records (capture_id, seq, header, payload_sha256, hour, first_pack_id)`, 6, records)
	if err != nil {
		return err
	}

	return insertValues(ctx, tx, c.d.insertIgnore+` INTO claim_objects (object_key, capture_id, seq, hour, size, state)`, 6, objects)
}

// stageParked moves the versions evidenceGuard found parking or parked to staged, forward only:
// the resolution a later copy is acknowledged from, committed with the receipts (parking.md *Resolution*).
func (c *Catalog) stageParked(ctx context.Context, tx *sqlx.Tx, vs []*VersionKey) error {
	now := c.now()
	for _, v := range vs {
		_, err := tx.ExecContext(ctx, `UPDATE parked SET state = 'staged', resolved_at = ?
			WHERE capture_id = ? AND seq = ? AND header = ? AND payload_digest = ? AND state IN ('parking', 'parked')`,
			now, v.Capture[:], int64(v.Seq), v.Header[:], v.Digest[:])
		if err != nil {
			return err
		}
	}

	return nil
}

// updateCapture records the registration on the capture: its time, its highest producer seq with that record's time,
// and a stop, which moves an open capture to stopped (a closed one stays closed); a close, which holds no range, moves no seq.
func (c *Catalog) updateCapture(ctx context.Context, tx *sqlx.Tx, p *Pack, row captureRow) error {
	seq, ts := row.HighestSeq, row.HighestSeqTS
	for _, rg := range p.Ranges {
		if !seq.Valid || int64(rg.SeqTo) > seq.Int64 {
			seq, ts = sql.NullInt64{Int64: int64(rg.SeqTo), Valid: true}, sql.NullInt64{Int64: rg.TSTo, Valid: true}
		}
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

// openedGaps counts the holes ranges open above highest, the capture's highest seq before them (none: below seq 0):
// a range starting past the next seq expected opens one, and ranges at or below it fill or repeat what is known.
func openedGaps(highest sql.NullInt64, ranges []Range) int {
	next := uint64(0)
	if highest.Valid {
		next = uint64(highest.Int64) + 1 //nolint:gosec // a stored seq is below 2^63
	}
	sorted := slices.SortedFunc(slices.Values(ranges), func(a, b Range) int { return cmp.Compare(a.SeqFrom, b.SeqFrom) })
	n := 0
	for _, r := range sorted {
		if r.SeqTo < next {
			continue
		}
		if r.SeqFrom > next {
			n++
		}
		next = r.SeqTo + 1
	}

	return n
}

// signed is a checked seq as its column stores it, nil when absent.
func signed(seq *uint64) *int64 {
	if seq == nil {
		return nil
	}
	v := int64(*seq)

	return &v
}
