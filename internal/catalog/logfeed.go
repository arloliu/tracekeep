package catalog

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// The host log feed's transactions (catalog.md §7.2 AdmitLog, RegisterLogFlush, StopLogCapture; logfeed.md §15.6, §15.7, §15.11).
// Each locks its capture rows first, in ascending capture_id, then its lease group's row, then its files' rows in ascending key (logfeed.md §15.7).

// ErrFileStopped refuses an admission or a flush of a log file that is stopped; nothing was written.
var ErrFileStopped = errors.New("catalog: log file stopped")

// ErrWatermarkMoved refuses an idle stop whose file was registered since the stop read its watermark; nothing was written.
var ErrWatermarkMoved = errors.New("catalog: log file's watermark moved")

// ErrNotLogCapture refuses a log-feed operation on a capture that is not a log capture; nothing was written.
var ErrNotLogCapture = errors.New("catalog: not a log capture")

// LogFile names a host program's log file as the feed does: its source, its equipment and its dated base name.
type LogFile struct {
	Source, EquipmentID, File string
}

// LogKey is a log capture's key: one device's capture of a log file (logfeed.md §15.6).
type LogKey struct {
	LogFile
	Device string
}

// LogGroupLease is the name of lease group n's lease row.
func LogGroupLease(n int) string { return fmt.Sprintf("logfeed/%d", n) }

// ErrNotLogGroup refuses a log-feed transaction under a lease that is not a lease group's; nothing was written.
var ErrNotLogGroup = errors.New("catalog: not a log lease group")

// logGroupOf is the group whose lease l is, so the group a transaction records is the one whose lease it checked.
func logGroupOf(l Lease) (int, error) {
	var n int
	if _, err := fmt.Sscanf(l.Name, "logfeed/%d", &n); err != nil || LogGroupLease(n) != l.Name {
		return 0, fmt.Errorf("%w: %s", ErrNotLogGroup, l.Name)
	}

	return n, nil
}

// EnsureLogGroups inserts the lease rows of groups 0 to groups-1 that are missing; rows of other groups are left.
func (c *Catalog) EnsureLogGroups(ctx context.Context, groups int) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		for n := range groups {
			_, err := tx.ExecContext(ctx, c.d.insertIgnore+` INTO leases (name, holder, fence_token, until_ns) VALUES (?, '', 0, 0)`, LogGroupLease(n))
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("catalog: ensure log groups: %w", err)
	}

	return nil
}

// SyncLogGroups makes the lease groups' rows those of groups 0 to groups-1, in one transaction, judged from the rows alone (logfeed.md §15.4):
// with no row, it inserts them; with rows of another set, after a change of G on a drained stream with every log stage stopped,
// it increments every remaining group's fence and lapses its lease, so no converter of the old G registers again,
// deletes the rows of groups at or above groups, and inserts the missing ones; with exactly those rows it changes nothing.
// A migrate whose stream changed and whose catalog did not finds the old rows on its next run and completes the change.
func (c *Catalog) SyncLogGroups(ctx context.Context, groups int) error {
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		var names []string
		if err := tx.SelectContext(ctx, &names, `SELECT name FROM leases WHERE name LIKE 'logfeed/%'`); err != nil {
			return err
		}
		have := map[int]bool{}
		for _, name := range names {
			n, err := logGroupOf(Lease{Name: name})
			if err != nil || n >= groups {
				n = -1
			}
			have[n] = true
		}
		same := len(names) == groups && !have[-1]
		for _, name := range names {
			if same {
				break
			}
			n, err := logGroupOf(Lease{Name: name})
			if err == nil && n < groups {
				_, err = tx.ExecContext(ctx, `UPDATE leases SET holder = '', fence_token = fence_token + 1, until_ns = 0 WHERE name = ?`, name)
			} else {
				_, err = tx.ExecContext(ctx, `DELETE FROM leases WHERE name = ?`, name)
			}
			if err != nil {
				return err
			}
		}
		for n := range groups {
			if have[n] {
				continue
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO leases (name, holder, fence_token, until_ns) VALUES (?, '', 0, 0)`, LogGroupLease(n)); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("catalog: sync log groups: %w", err)
	}

	return nil
}

// LogAdmission is AdmitLog's outcome: the capture of the key, new or found, and the configuration it is bound to.
type LogAdmission struct {
	Capture tracepack.UUID
	// Found reports that the key named a capture already, which returns and nothing was written.
	Found    bool
	ConfigID int64
}

// AdmitLog admits id as the capture of key under lease l, its group's, which the key records, with the descriptor d the log stage built (catalog.md §7.2 AdmitLog):
// the capture row open with the newest configuration bindable admits, the file's row inserted when it has none, and the key's row.
// A key that names a capture already returns it and writes nothing; a stopped file returns ErrFileStopped,
// a lost lease ErrLeaseLost, and no bindable configuration ErrNoBindableConfig, each writing nothing.
func (c *Catalog) AdmitLog(ctx context.Context, l Lease, key LogKey, id tracepack.UUID, d *Descriptor, bindable func(configID int64) bool) (LogAdmission, error) {
	group, err := logGroupOf(l)
	if err != nil {
		return LogAdmission{}, err
	}
	var res LogAdmission
	err = c.inTx(ctx, func(tx *sqlx.Tx) error {
		res = LogAdmission{}
		if _, err := c.lockCapture(ctx, tx, id); err != nil {
			return err
		}
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		stopped, _, err := c.lockLogFile(ctx, tx, key.LogFile, true)
		if err != nil {
			return err
		}
		if stopped {
			return ErrFileStopped
		}
		var found []byte
		err = tx.GetContext(ctx, &found, `SELECT capture_id FROM logfeed_captures WHERE source = ? AND equipment_id = ? AND device = ? AND file = ?`,
			key.Source, key.EquipmentID, key.Device, key.File)
		switch {
		case err == nil:
			copy(res.Capture[:], found)
			res.Found = true

			return errRollback
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		if err := c.lockMembership(ctx, tx); err != nil {
			return err
		}
		cfg, ok, err := c.bindableConfig(ctx, tx, bindable)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNoBindableConfig
		}
		if err := c.openCapture(ctx, tx, id, d, cfg); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO logfeed_captures (source, equipment_id, device, file, capture_id, lease_group) VALUES (?, ?, ?, ?, ?, ?)`,
			key.Source, key.EquipmentID, key.Device, key.File, id[:], group)
		res = LogAdmission{Capture: id, ConfigID: cfg}

		return err
	})
	if err = outcome(err); err != nil {
		return LogAdmission{}, fmt.Errorf("catalog: admit log capture %s: %w", id, err)
	}

	return res, nil
}

// lockLogFile locks file's row by an update before reading it, inserting it with no watermark when create is set and it has none,
// and returns whether it is stopped and its watermark; a missing row with create unset is ErrUnknownLogFile.
func (c *Catalog) lockLogFile(ctx context.Context, tx *sqlx.Tx, f LogFile, create bool) (stopped bool, watermark int64, err error) {
	if create {
		_, err = tx.ExecContext(ctx, c.d.logFileUpsert, f.Source, f.EquipmentID, f.File)
	} else {
		var res sql.Result
		res, err = tx.ExecContext(ctx, `UPDATE logfeed_files SET watermark = watermark WHERE source = ? AND equipment_id = ? AND file = ?`, f.Source, f.EquipmentID, f.File)
		err = affected(res, err, ErrUnknownLogFile)
	}
	if err != nil {
		return false, 0, err
	}
	var row struct {
		Stopped   bool  `db:"stopped"`
		Watermark int64 `db:"watermark"`
	}
	err = tx.GetContext(ctx, &row, `SELECT stopped, watermark FROM logfeed_files WHERE source = ? AND equipment_id = ? AND file = ?`, f.Source, f.EquipmentID, f.File)

	return row.Stopped, row.Watermark, err
}

// ErrUnknownLogFile reports a flush naming a log file no admission created; nothing was written.
var ErrUnknownLogFile = errors.New("catalog: unknown log file")

// NoWatermark is a log file's watermark before any of its entries is settled; an entry's offset is never negative.
const NoWatermark = -1

// LogFileMark is what a flush settles of one file: its new watermark, and whether the flush stops it.
type LogFileMark struct {
	LogFile
	Watermark int64
	Stop      bool
	// IdleSince, for an idle stop, is the watermark the stop read; the flush is refused with ErrWatermarkMoved when the file's differs.
	IdleSince *int64
}

// LogFlush is one equipment's flush (logfeed.md §15.7): its segments, every batch of the equipment's, and the files it settles.
type LogFlush struct {
	Packs []*Pack
	Files []LogFileMark
	// HorizonHour is the oldest hour retention keeps, as for RegisterPack.
	HorizonHour int64
}

// RegisterLogFlush registers one equipment's flush under lease l in one transaction (catalog.md §7.2 RegisterLogFlush):
// each segment as RegisterPack registers it without evidence, each file's watermark raised and never lowered, a stopping file stopped,
// and each capture's key moved to the group whose lease l is.
// It returns Registered once committed, AlreadyRegistered when the flush committed before (its first pack registered),
// or the first segment's outcome other than Registered, writing nothing; a lost lease, a stopped file or a moved watermark is an error, writing nothing.
func (c *Catalog) RegisterLogFlush(ctx context.Context, l Lease, f *LogFlush) (PackOutcome, error) {
	if len(f.Packs) == 0 {
		return 0, fmt.Errorf("%w: a flush without a segment", ErrInvalidRegistration)
	}
	group, err := logGroupOf(l)
	if err != nil {
		return 0, err
	}
	// The group's lease, which registerIn checks again per segment under the row this transaction already holds.
	r := Registration{Lease: &l, HorizonHour: f.HorizonHour}
	for _, p := range f.Packs {
		if err := p.check(r); err != nil {
			return 0, err
		}
	}
	captures := make([]tracepack.UUID, 0, len(f.Packs))
	for _, p := range f.Packs {
		captures = append(captures, p.Capture)
	}
	slices.SortFunc(captures, func(a, b tracepack.UUID) int { return slices.Compare(a[:], b[:]) })
	captures = slices.Compact(captures)
	files := slices.Clone(f.Files)
	slices.SortFunc(files, func(a, b LogFileMark) int {
		return cmp.Or(cmp.Compare(a.Source, b.Source), cmp.Compare(a.EquipmentID, b.EquipmentID), cmp.Compare(a.File, b.File))
	})
	var out PackOutcome
	err = outcome(c.inTx(ctx, func(tx *sqlx.Tx) error {
		out = 0
		for _, id := range captures {
			if _, err := c.lockCapture(ctx, tx, id); err != nil {
				return err
			}
		}
		if err := c.checkLease(ctx, tx, l); err != nil {
			return err
		}
		var n int
		if err := tx.GetContext(ctx, &n, `SELECT COUNT(*) FROM packs WHERE pack_id = ?`, f.Packs[0].ID[:]); err != nil {
			return err
		}
		if n > 0 {
			out = AlreadyRegistered
			return errRollback
		}
		if err := c.lockLogFiles(ctx, tx, files); err != nil {
			return err
		}
		var evs []event
		for _, p := range f.Packs {
			o, e, err := c.registerIn(ctx, tx, p, r, nil)
			if err != nil {
				return err
			}
			if o != Registered {
				out = o
				return errRollback
			}
			evs = append(evs, e...)
		}
		if err := c.settleLogFiles(ctx, tx, files, captures, group); err != nil {
			return err
		}
		out = Registered

		return c.emit(ctx, tx, evs)
	}))
	if err != nil {
		return 0, fmt.Errorf("catalog: register log flush: %w", err)
	}

	return out, nil
}

// lockLogFiles locks each file's row, in the order given, refusing a stopped file and an idle stop whose watermark moved.
func (c *Catalog) lockLogFiles(ctx context.Context, tx *sqlx.Tx, files []LogFileMark) error {
	for _, m := range files {
		stopped, watermark, err := c.lockLogFile(ctx, tx, m.LogFile, false)
		switch {
		case err != nil:
			return err
		case stopped:
			return ErrFileStopped
		case m.IdleSince != nil && *m.IdleSince != watermark:
			return ErrWatermarkMoved
		}
	}

	return nil
}

// settleLogFiles raises each file's watermark, never lowering it, stops a stopping file, and moves each capture's key to group.
func (c *Catalog) settleLogFiles(ctx context.Context, tx *sqlx.Tx, files []LogFileMark, captures []tracepack.UUID, group int) error {
	for _, m := range files {
		_, err := tx.ExecContext(ctx, `UPDATE logfeed_files SET watermark = CASE WHEN watermark > ? THEN watermark ELSE ? END, stopped = stopped OR ?
			WHERE source = ? AND equipment_id = ? AND file = ?`, m.Watermark, m.Watermark, m.Stop, m.Source, m.EquipmentID, m.File)
		if err != nil {
			return err
		}
	}
	for _, id := range captures {
		if _, err := tx.ExecContext(ctx, `UPDATE logfeed_captures SET lease_group = ? WHERE capture_id = ?`, group, id[:]); err != nil {
			return err
		}
	}

	return nil
}

// LogStop is StopLogCapture's outcome.
type LogStop int

const (
	// StopRequested: the group's lease is held, and its converter writes the stop at its next flush.
	StopRequested LogStop = iota + 1
	// StopWritten: no converter held the group's lease, and the capture is stopped.
	StopWritten
	// StopAlready: the capture was stopped or closed already; nothing was written.
	StopAlready
)

// StopLogCapture records an operator's stop of the log capture id (catalog.md §7.2 StopLogCapture): with its group's lease held,
// a stop request its converter writes at its next flush; with the lease lapsed, the capture stopped at once.
// A capture that is not a log capture is ErrNotLogCapture, writing nothing.
func (c *Catalog) StopLogCapture(ctx context.Context, id tracepack.UUID) (LogStop, error) {
	var out LogStop
	err := c.inTx(ctx, func(tx *sqlx.Tx) error {
		out = 0
		row, err := c.lockCapture(ctx, tx, id)
		if err != nil {
			return err
		}
		var lc struct {
			Method sql.NullString `db:"capture_method"`
			Group  sql.NullInt64  `db:"lease_group"`
		}
		err = tx.GetContext(ctx, &lc, `SELECT c.capture_method, k.lease_group FROM captures c LEFT JOIN logfeed_captures k ON k.capture_id = c.capture_id
			WHERE c.capture_id = ?`, id[:])
		if err != nil {
			return err
		}
		switch {
		case lc.Method.String != tracepack.CaptureMethodLog.String() || !lc.Group.Valid:
			return ErrNotLogCapture
		case row.State != StateOpen:
			out = StopAlready
			return errRollback
		}
		// The group's row locked whatever its state, so a converter taking the lease waits for this commit, then judged held by the database's clock.
		name := LogGroupLease(int(lc.Group.Int64))
		res, err := tx.ExecContext(ctx, `UPDATE leases SET until_ns = until_ns WHERE name = ?`, name)
		held := affected(res, err, ErrLeaseLost)
		if held == nil {
			var live int
			if err := tx.GetContext(ctx, &live, `SELECT COUNT(*) FROM leases WHERE name = ? AND until_ns >= `+c.d.nowNS, name); err != nil {
				return err
			}
			if live == 0 {
				held = ErrLeaseLost
			}
		}
		switch {
		case held == nil:
			out = StopRequested
			_, err = tx.ExecContext(ctx, `UPDATE captures SET stop_requested_at = ? WHERE capture_id = ?`, c.now(), id[:])
		case errors.Is(held, ErrLeaseLost):
			// Lapsed, or a group no lease row names, which no converter can take: no converter holds it, so the stop is written here.
			out = StopWritten
			// stop_seen stays unset: it names a stop boundary in a pack, which this stop has none of.
			_, err = tx.ExecContext(ctx, `UPDATE captures SET state = 'stopped', stop_requested_at = ? WHERE capture_id = ?`, c.now(), id[:])
		default:
			err = held
		}

		return err
	})
	if err = outcome(err); err != nil {
		return 0, fmt.Errorf("catalog: stop log capture %s: %w", id, err)
	}

	return out, nil
}

// FileState is a log file's watermark and whether it is stopped; Found is false for a file no admission created, whose watermark is NoWatermark.
type FileState struct {
	Watermark      int64 `db:"watermark"`
	Stopped, Found bool
}

// LogFileState is f's state.
func (c *Catalog) LogFileState(ctx context.Context, f LogFile) (FileState, error) {
	var row struct {
		Watermark int64 `db:"watermark"`
		Stopped   bool  `db:"stopped"`
	}
	err := c.read.GetContext(ctx, &row, `SELECT watermark, stopped FROM logfeed_files WHERE source = ? AND equipment_id = ? AND file = ?`,
		f.Source, f.EquipmentID, f.File)
	if errors.Is(err, sql.ErrNoRows) {
		return FileState{Watermark: NoWatermark}, nil
	}
	if err != nil {
		return FileState{}, fmt.Errorf("catalog: log file %s/%s: %w", f.Source, f.File, err)
	}

	return FileState{Watermark: row.Watermark, Stopped: row.Stopped, Found: true}, nil
}

// LogCapture is a log capture as its converter reads it back: the capture, and its predecessor, which its segments name.
type LogCapture struct {
	Capture
	Previous *tracepack.UUID
}

// LogCaptureOf is the capture key names, false when no admission created it.
func (c *Catalog) LogCaptureOf(ctx context.Context, key LogKey) (LogCapture, bool, error) {
	var row struct {
		ID       []byte `db:"capture_id"`
		Previous []byte `db:"previous_capture_id"`
	}
	err := c.read.GetContext(ctx, &row, `SELECT k.capture_id, c.previous_capture_id FROM logfeed_captures k JOIN captures c ON c.capture_id = k.capture_id
		WHERE k.source = ? AND k.equipment_id = ? AND k.device = ? AND k.file = ?`, key.Source, key.EquipmentID, key.Device, key.File)
	if errors.Is(err, sql.ErrNoRows) {
		return LogCapture{}, false, nil
	}
	if err != nil {
		return LogCapture{}, false, fmt.Errorf("catalog: log capture key: %w", err)
	}
	cp, err := c.Capture(ctx, tracepack.UUID(row.ID))
	if err != nil {
		return LogCapture{}, false, err
	}
	lc := LogCapture{Capture: cp}
	if row.Previous != nil {
		p := tracepack.UUID(row.Previous)
		lc.Previous = &p
	}

	return lc, true, nil
}

// LogPredecessor is the latest capture of the series (equipment, instance) admitted with an origin before origin, nil for none:
// the previous_capture_id a log capture of the series' next file names (logfeed.md §15.6).
func (c *Catalog) LogPredecessor(ctx context.Context, equipment string, instance tracepack.UUID, origin int64) (*tracepack.UUID, error) {
	var ids [][]byte
	err := c.read.SelectContext(ctx, &ids, `SELECT capture_id FROM captures WHERE equipment_id = ? AND recorder_instance_id = ? AND origin_ns < ?
		ORDER BY origin_ns DESC LIMIT 1`, equipment, instance[:], origin)
	if err != nil {
		return nil, fmt.Errorf("catalog: log predecessor: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil //nolint:nilnil // no predecessor is not an error
	}
	id := tracepack.UUID(ids[0])

	return &id, nil
}

// LogStopRequests are the open log captures of group with an operator's stop request, which its converter writes at its next flush (§15.11).
func (c *Catalog) LogStopRequests(ctx context.Context, group int) ([]tracepack.UUID, error) {
	var ids [][]byte
	err := c.read.SelectContext(ctx, &ids, `SELECT c.capture_id FROM captures c JOIN logfeed_captures k ON k.capture_id = c.capture_id
		WHERE k.lease_group = ? AND c.state = 'open' AND c.stop_requested_at IS NOT NULL`, group)
	if err != nil {
		return nil, fmt.Errorf("catalog: log stop requests: %w", err)
	}
	out := make([]tracepack.UUID, len(ids))
	for i, id := range ids {
		out[i] = tracepack.UUID(id)
	}

	return out, nil
}
