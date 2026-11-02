package catalog

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// Completeness is one capture's completeness (catalog.md §7.3): every seq up to the frontier classified once,
// retained > evicted > deferred > declared loss > gap, with the unknown tail above it and the declared losses, crash gaps and conflicts beside.
// It is a domain result: a caller reads it, never reclassifies rows.
type Completeness struct {
	// Frontier is the higher of the highest producer seq and the highest parked seq; HasFrontier is false when neither exists.
	Frontier    uint64
	HasFrontier bool
	// Retained are the seq runs registered packs hold, with the times of their endpoints.
	Retained []Range
	// Evicted are the runs retention evicted that no retained pack also holds; an endpoint a retained run cut has no time of its own.
	Evicted []Run
	// Deferred are the seqs an unresolved parked version holds and nothing retained or evicted does, with the carried record's time.
	Deferred []Deferred
	// Gaps are the holes up to the frontier no class explains.
	Gaps []Gap
	// UnknownTailFrom is the first seq above the frontier, 0 when there is none: whatever the capture's state, its seqs are unknown.
	UnknownTailFrom uint64
	// Losses are every declared loss, from packs' coverage entries and terminated versions,
	// reported even beside another version of the seq that is retained (the precedence classifies seqs, not reports).
	Losses []DeclaredLoss
	// CrashGaps are the crash gaps closes recorded.
	CrashGaps []CrashGap
	// Conflicts are the conflicting versions the catalog recorded, suspects included.
	Conflicts []ConflictRange
}

// Run is a seq run with the times of its endpoint records, nil where the record's time is not known.
type Run struct {
	SeqFrom, SeqTo uint64
	TSFrom, TSTo   *int64
}

// Deferred is a deferred seq: its version is parking, parked or evicted-hour.
type Deferred struct {
	Seq   uint64
	TS    int64
	State VersionState
}

// Gap is a hole in the producer coverage, with the times of the records on either side:
// Start is the record before it, or for a missing prefix the capture's origin with OpenStart set; End is the record after it.
// Either time is nil when its neighbour carries none.
type Gap struct {
	From, To   uint64
	Start, End *int64
	OpenStart  bool
}

// DeclaredLoss is a declared loss: a pack's coverage entry, with the bounds it declares, or a terminated version with its action.
type DeclaredLoss struct {
	SeqFrom, SeqTo *uint64
	TSFrom, TSTo   *int64
	// ActionID names the termination; empty for a pack's coverage entry.
	ActionID string
}

// CrashGap is a crash gap a close recorded.
type CrashGap struct {
	Start, End *int64
}

// ConflictRange is a recorded conflict between two packs over a seq run.
type ConflictRange struct {
	SeqFrom, SeqTo uint64
	State          string
}

// Completeness reads capture's completeness (catalog.md §7.3), every row from one snapshot,
// so a registration committing meanwhile can never turn a deferred seq into a gap.
func (c *Catalog) Completeness(ctx context.Context, capture tracepack.UUID) (Completeness, error) {
	var ins []completenessInputs
	err := c.snapshot(ctx, func(tx *sqlx.Tx) error {
		var err error
		ins, err = c.completenessInputs(ctx, tx, []tracepack.UUID{capture})

		return err
	})
	if err != nil {
		return Completeness{}, fmt.Errorf("catalog: completeness of %s: %w", capture, err)
	}

	return ins[0].classify(), nil
}

// classify classifies the inputs of one capture read from one snapshot (catalog.md §7.3).
func (in *completenessInputs) classify() Completeness {
	var out Completeness
	// The diagnostics stand on their own: a capture with no producer seq may still have a close or a declared loss.
	out.Losses, out.CrashGaps, out.Conflicts = slices.Clone(in.losses), in.crashGaps, in.conflicts
	out.Retained = coalesce(in.retained)
	out.Evicted = subtract(asRuns(coalesce(in.evicted)), out.Retained)
	frontier, ok := in.highest, in.hasHighest
	for _, p := range in.parked {
		if !ok || p.seq > frontier {
			frontier, ok = p.seq, true
		}
	}
	if !ok {
		return out
	}
	out.Frontier, out.HasFrontier, out.UnknownTailFrom = frontier, true, frontier+1
	covered := slices.Concat(out.Retained, in.evicted)
	// Gaps are timed by the records beside them: the original runs, never a cut endpoint.
	explained := slices.Concat(asRuns(out.Retained), asRuns(in.evicted))
	for _, p := range in.parked {
		ts := p.ts
		switch {
		case p.state == VersionTerminated:
			s := p.seq
			out.Losses = append(out.Losses, DeclaredLoss{SeqFrom: &s, SeqTo: &s, TSFrom: &ts, TSTo: &ts, ActionID: p.action})
			explained = append(explained, Run{SeqFrom: p.seq, SeqTo: p.seq, TSFrom: &ts, TSTo: &ts})
		case p.state != VersionStaged && !holds(covered, p.seq):
			out.Deferred = append(out.Deferred, Deferred{Seq: p.seq, TS: p.ts, State: p.state})
			explained = append(explained, Run{SeqFrom: p.seq, SeqTo: p.seq, TSFrom: &ts, TSTo: &ts})
		default:
			// Staged, or a seq a retained or evicted run already classifies.
		}
	}
	out.Deferred = slices.CompactFunc(out.Deferred, func(a, b Deferred) bool { return a.Seq == b.Seq })
	for _, l := range in.losses {
		if l.SeqFrom != nil && l.SeqTo != nil && *l.SeqFrom <= *l.SeqTo {
			explained = append(explained, Run{SeqFrom: *l.SeqFrom, SeqTo: *l.SeqTo, TSFrom: l.TSFrom, TSTo: l.TSTo})
		}
	}
	out.Gaps = gaps(coalesceRuns(explained), frontier, in.origin)

	return out
}

// parkedSeq is a parked version's seq, state, record time and terminating action.
type parkedSeq struct {
	seq    uint64
	ts     int64
	state  VersionState
	action string
}

type completenessInputs struct {
	highest    uint64
	hasHighest bool
	origin     *int64
	retained   []Range
	evicted    []Range
	parked     []parkedSeq
	losses     []DeclaredLoss
	crashGaps  []CrashGap
	conflicts  []ConflictRange
}

// snapshot runs f in one read-only snapshot: SQLite's from the first read of the transaction, MariaDB's under REPEATABLE READ,
// since the catalog's sessions otherwise read committed.
func (c *Catalog) snapshot(ctx context.Context, f func(tx *sqlx.Tx) error) (err error) {
	opts := &sql.TxOptions{ReadOnly: true}
	if c.d.name == EngineMariaDB {
		opts.Isolation = sql.LevelRepeatableRead
	}
	tx, err := c.read.BeginTxx(ctx, opts)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, tx.Rollback()) }()

	return f(tx)
}

// completenessChunk is the most captures one read of completeness inputs names, each one bound parameter:
// a listing reads its captures' inputs a chunk at a time, seven statements a chunk, not seven a capture.
const completenessChunk = 128

// The queries of a completeness's inputs (completenessInputs), each over the captures its IN (?) names:
// one as written, a chunk once inCaptures widens it. Each orders what classify keeps in order.
const (
	frontierQuery = `SELECT capture_id, highest_seq, origin_ns FROM captures WHERE capture_id IN (?)`
	retainedQuery = `SELECT r.capture_id, r.seq_from, r.seq_to, r.ts_from, r.ts_to FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
		WHERE r.capture_id IN (?) AND k.state <> 'deleted'`
	evictedQuery = `SELECT capture_id, seq_from, seq_to, ts_from, ts_to FROM evicted_ranges WHERE capture_id IN (?)`
	parkedQuery  = `SELECT capture_id, seq, header, state, terminated_by FROM parked WHERE capture_id IN (?) ORDER BY capture_id, seq`
	lossesQuery  = `SELECT k.capture_id, l.seq_from, l.seq_to, l.ts_from, l.ts_to FROM pack_losses l JOIN packs k ON k.pack_id = l.pack_id
		WHERE k.capture_id IN (?) AND k.state <> 'deleted' ORDER BY l.pack_id, l.ord`
	crashGapsQuery = `SELECT capture_id, gap_start, gap_end FROM capture_closes
		WHERE capture_id IN (?) AND (gap_start IS NOT NULL OR gap_end IS NOT NULL)`
	conflictsQuery = `SELECT capture_id, seq_from, seq_to, state FROM conflicts WHERE capture_id IN (?) ORDER BY capture_id, seq_from`
)

// inCaptures widens query's IN (?) to n placeholders.
func inCaptures(query string, n int) string {
	return strings.Replace(query, "IN (?)", "IN (?"+strings.Repeat(", ?", n-1)+")", 1)
}

// completenessInputs reads the rows the completeness of each of captures classifies, at most completenessChunk, inside the snapshot tx,
// in the order of captures; a capture the catalog does not hold fails the read with sql.ErrNoRows.
func (c *Catalog) completenessInputs(ctx context.Context, tx *sqlx.Tx, captures []tracepack.UUID) ([]completenessInputs, error) {
	ins := make([]completenessInputs, len(captures))
	of := make(map[tracepack.UUID]*completenessInputs, len(captures))
	args := make([]any, len(captures))
	for i, id := range captures {
		of[id], args[i] = &ins[i], id[:]
	}
	in := func(id []byte) *completenessInputs { return of[tracepack.UUID(id)] }
	// read runs one input's query over the chunk into rows; the hook after it lets a test commit between any two reads, or two chunks.
	read := func(rows any, query string) error {
		if err := tx.SelectContext(ctx, rows, inCaptures(query, len(captures)), args...); err != nil {
			return err
		}
		c.hook(hookCompletenessRead)

		return nil
	}
	var frontiers []struct {
		ID      []byte        `db:"capture_id"`
		Highest sql.NullInt64 `db:"highest_seq"`
		Origin  sql.NullInt64 `db:"origin_ns"`
	}
	if err := read(&frontiers, frontierQuery); err != nil {
		return nil, err
	}
	if len(frontiers) != len(of) {
		return nil, sql.ErrNoRows
	}
	for _, r := range frontiers {
		p := in(r.ID)
		p.highest, p.hasHighest, p.origin = uint64(r.Highest.Int64), r.Highest.Valid, nullable(r.Origin) //nolint:gosec // a stored seq is below 2^63
	}
	for _, q := range []struct {
		query string
		into  func(*completenessInputs) *[]Range
	}{
		{retainedQuery, func(p *completenessInputs) *[]Range { return &p.retained }},
		{evictedQuery, func(p *completenessInputs) *[]Range { return &p.evicted }},
	} {
		var rows []struct {
			ID     []byte `db:"capture_id"`
			From   int64  `db:"seq_from"`
			To     int64  `db:"seq_to"`
			TSFrom int64  `db:"ts_from"`
			TSTo   int64  `db:"ts_to"`
		}
		if err := read(&rows, q.query); err != nil {
			return nil, err
		}
		for _, r := range rows {
			rs := q.into(in(r.ID))
			*rs = append(*rs, Range{SeqFrom: uint64(r.From), SeqTo: uint64(r.To), TSFrom: r.TSFrom, TSTo: r.TSTo}) //nolint:gosec // stored seqs are below 2^63
		}
	}
	var parked []struct {
		ID     []byte         `db:"capture_id"`
		Seq    int64          `db:"seq"`
		Header []byte         `db:"header"`
		State  VersionState   `db:"state"`
		By     sql.NullString `db:"terminated_by"`
	}
	if err := read(&parked, parkedQuery); err != nil {
		return nil, err
	}
	for _, r := range parked {
		p := in(r.ID)
		p.parked = append(p.parked, parkedSeq{seq: uint64(r.Seq), ts: headerTS(r.Header), state: r.State, action: r.By.String}) //nolint:gosec // a stored seq is below 2^63
	}
	var losses []struct {
		ID     []byte        `db:"capture_id"`
		From   sql.NullInt64 `db:"seq_from"`
		To     sql.NullInt64 `db:"seq_to"`
		TSFrom sql.NullInt64 `db:"ts_from"`
		TSTo   sql.NullInt64 `db:"ts_to"`
	}
	if err := read(&losses, lossesQuery); err != nil {
		return nil, err
	}
	for _, r := range losses {
		p := in(r.ID)
		p.losses = append(p.losses, DeclaredLoss{SeqFrom: unsigned(r.From), SeqTo: unsigned(r.To), TSFrom: nullable(r.TSFrom), TSTo: nullable(r.TSTo)})
	}
	var gapRows []struct {
		ID    []byte        `db:"capture_id"`
		Start sql.NullInt64 `db:"gap_start"`
		End   sql.NullInt64 `db:"gap_end"`
	}
	if err := read(&gapRows, crashGapsQuery); err != nil {
		return nil, err
	}
	for _, r := range gapRows {
		p := in(r.ID)
		p.crashGaps = append(p.crashGaps, CrashGap{Start: nullable(r.Start), End: nullable(r.End)})
	}
	var conflicts []struct {
		ID    []byte `db:"capture_id"`
		From  int64  `db:"seq_from"`
		To    int64  `db:"seq_to"`
		State string `db:"state"`
	}
	if err := read(&conflicts, conflictsQuery); err != nil {
		return nil, err
	}
	for _, r := range conflicts {
		p := in(r.ID)
		p.conflicts = append(p.conflicts, ConflictRange{SeqFrom: uint64(r.From), SeqTo: uint64(r.To), State: r.State}) //nolint:gosec // stored seqs are below 2^63
	}

	return ins, nil
}

// headerTS is the ts_utc_ns a carried 44-byte record header holds, signed little-endian at bytes 8–15.
func headerTS(h []byte) int64 {
	if len(h) < 16 {
		return 0
	}

	return int64(binary.LittleEndian.Uint64(h[8:16])) //nolint:gosec // ts_utc_ns is signed
}

// coalesce sorts runs and merges the overlapping and adjacent ones, keeping the outer endpoints' times.
func coalesce(rs []Range) []Range {
	rs = slices.Clone(rs)
	slices.SortFunc(rs, func(a, b Range) int { return cmp.Compare(a.SeqFrom, b.SeqFrom) })
	var out []Range
	for _, r := range rs {
		if n := len(out); n > 0 && r.SeqFrom <= out[n-1].SeqTo+1 {
			if r.SeqTo > out[n-1].SeqTo {
				out[n-1].SeqTo, out[n-1].TSTo = r.SeqTo, r.TSTo
			}
			continue
		}
		out = append(out, r)
	}

	return out
}

// asRuns are rs with their known endpoint times.
func asRuns(rs []Range) []Run {
	out := make([]Run, 0, len(rs))
	for _, r := range rs {
		from, to := r.TSFrom, r.TSTo
		out = append(out, Run{SeqFrom: r.SeqFrom, SeqTo: r.SeqTo, TSFrom: &from, TSTo: &to})
	}

	return out
}

// coalesceRuns sorts runs and merges the overlapping and adjacent ones, keeping the outer endpoints' times;
// where two runs end on one seq, a time either knows is kept.
func coalesceRuns(rs []Run) []Run {
	rs = slices.Clone(rs)
	slices.SortFunc(rs, func(a, b Run) int { return cmp.Compare(a.SeqFrom, b.SeqFrom) })
	var out []Run
	for _, r := range rs {
		n := len(out)
		if n == 0 || r.SeqFrom > out[n-1].SeqTo+1 {
			out = append(out, r)
			continue
		}
		last := &out[n-1]
		if r.SeqFrom == last.SeqFrom && last.TSFrom == nil {
			last.TSFrom = r.TSFrom
		}
		switch {
		case r.SeqTo > last.SeqTo:
			last.SeqTo, last.TSTo = r.SeqTo, r.TSTo
		case r.SeqTo == last.SeqTo && last.TSTo == nil:
			last.TSTo = r.TSTo
		default:
		}
	}

	return out
}

// subtract returns the parts of the coalesced runs a that no run of the coalesced b holds;
// an endpoint b cuts has no time of its own, since no record of a sits there.
func subtract(a []Run, b []Range) []Run {
	var out []Run
	for _, r := range a {
		pieces := []Run{r}
		for _, s := range b {
			var next []Run
			for _, p := range pieces {
				if s.SeqTo < p.SeqFrom || s.SeqFrom > p.SeqTo {
					next = append(next, p)
					continue
				}
				if s.SeqFrom > p.SeqFrom {
					left := Run{SeqFrom: p.SeqFrom, SeqTo: s.SeqFrom - 1, TSFrom: p.TSFrom}
					if left.SeqTo == left.SeqFrom { // one seq: its end is the record its start names
						left.TSTo = p.TSFrom
					}
					next = append(next, left)
				}
				if s.SeqTo < p.SeqTo {
					right := Run{SeqFrom: s.SeqTo + 1, SeqTo: p.SeqTo, TSTo: p.TSTo}
					if right.SeqFrom == right.SeqTo {
						right.TSFrom = p.TSTo
					}
					next = append(next, right)
				}
			}
			pieces = next
		}
		out = append(out, pieces...)
	}

	return out
}

func holds(rs []Range, seq uint64) bool {
	return slices.ContainsFunc(rs, func(r Range) bool { return r.SeqFrom <= seq && seq <= r.SeqTo })
}

// gaps are the holes of the coalesced explained runs between seq 0 and frontier, timed by the runs on either side;
// a missing prefix starts at origin, open-ended.
func gaps(explained []Run, frontier uint64, origin *int64) []Gap {
	var out []Gap
	next := uint64(0)
	var before *int64
	for _, r := range explained {
		if r.SeqFrom > frontier {
			break
		}
		if r.SeqFrom > next {
			g := Gap{From: next, To: r.SeqFrom - 1, Start: before, End: r.TSFrom}
			if next == 0 {
				g.Start, g.OpenStart = origin, true
			}
			out = append(out, g)
		}
		before, next = r.TSTo, r.SeqTo+1
	}
	if next <= frontier {
		g := Gap{From: next, To: frontier, Start: before}
		if next == 0 {
			g.Start, g.OpenStart = origin, true
		}
		out = append(out, g)
	}

	return out
}

func nullable(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}

	return &v.Int64
}

func unsigned(v sql.NullInt64) *uint64 {
	if !v.Valid {
		return nil
	}
	u := uint64(v.Int64) //nolint:gosec // a stored seq is below 2^63

	return &u
}
