package catalog

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"slices"

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
	// Evicted are the runs retention evicted that no retained pack also holds.
	Evicted []Range
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

// Completeness reads capture's completeness (catalog.md §7.3).
func (c *Catalog) Completeness(ctx context.Context, capture tracepack.UUID) (Completeness, error) {
	var out Completeness
	in, err := c.completenessInputs(ctx, capture)
	if err != nil {
		return out, fmt.Errorf("catalog: completeness of %s: %w", capture, err)
	}
	out.Retained = coalesce(in.retained)
	out.Evicted = subtract(coalesce(in.evicted), out.Retained)
	frontier, ok := in.highest, in.hasHighest
	for _, p := range in.parked {
		if !ok || p.seq > frontier {
			frontier, ok = p.seq, true
		}
	}
	if !ok {
		return out, nil
	}
	out.Frontier, out.HasFrontier, out.UnknownTailFrom = frontier, true, frontier+1
	covered := slices.Concat(out.Retained, out.Evicted)
	var lost []Range
	for _, p := range in.parked {
		switch {
		case p.state == VersionTerminated:
			s := p.seq
			out.Losses = append(out.Losses, DeclaredLoss{SeqFrom: &s, SeqTo: &s, TSFrom: &p.ts, TSTo: &p.ts, ActionID: p.action})
			lost = append(lost, Range{SeqFrom: p.seq, SeqTo: p.seq, TSFrom: p.ts, TSTo: p.ts})
		case p.state != VersionStaged && !holds(covered, p.seq):
			out.Deferred = append(out.Deferred, Deferred{Seq: p.seq, TS: p.ts, State: p.state})
		default:
			// Staged, or a seq a retained or evicted run already classifies.
		}
	}
	out.Deferred = slices.CompactFunc(out.Deferred, func(a, b Deferred) bool { return a.Seq == b.Seq })
	for _, l := range in.losses {
		out.Losses = append(out.Losses, l)
		if l.SeqFrom != nil && l.SeqTo != nil && *l.SeqFrom <= *l.SeqTo {
			lost = append(lost, Range{SeqFrom: *l.SeqFrom, SeqTo: *l.SeqTo, TSFrom: deref(l.TSFrom), TSTo: deref(l.TSTo)})
		}
	}
	explained := slices.Concat(covered, lost)
	for _, d := range out.Deferred {
		explained = append(explained, Range{SeqFrom: d.Seq, SeqTo: d.Seq, TSFrom: d.TS, TSTo: d.TS})
	}
	out.Gaps = gaps(coalesce(explained), frontier, in.origin)
	out.CrashGaps, out.Conflicts = in.crashGaps, in.conflicts

	return out, nil
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

func (c *Catalog) completenessInputs(ctx context.Context, capture tracepack.UUID) (completenessInputs, error) {
	var in completenessInputs
	var row struct {
		Highest sql.NullInt64 `db:"highest_seq"`
		Origin  sql.NullInt64 `db:"origin_ns"`
	}
	if err := c.read.GetContext(ctx, &row, `SELECT highest_seq, origin_ns FROM captures WHERE capture_id = ?`, capture[:]); err != nil {
		return in, err
	}
	in.highest, in.hasHighest = uint64(row.Highest.Int64), row.Highest.Valid //nolint:gosec // a stored seq is below 2^63
	if row.Origin.Valid {
		in.origin = &row.Origin.Int64
	}
	var err error
	in.retained, err = c.ranges(ctx, `SELECT r.seq_from, r.seq_to, r.ts_from, r.ts_to FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
		WHERE r.capture_id = ? AND k.state <> 'deleted'`, capture)
	if err != nil {
		return in, err
	}
	if in.evicted, err = c.ranges(ctx, `SELECT seq_from, seq_to, ts_from, ts_to FROM evicted_ranges WHERE capture_id = ?`, capture); err != nil {
		return in, err
	}
	var parked []struct {
		Seq    int64          `db:"seq"`
		Header []byte         `db:"header"`
		State  VersionState   `db:"state"`
		By     sql.NullString `db:"terminated_by"`
	}
	if err := c.read.SelectContext(ctx, &parked, `SELECT seq, header, state, terminated_by FROM parked WHERE capture_id = ? ORDER BY seq`, capture[:]); err != nil {
		return in, err
	}
	for _, p := range parked {
		in.parked = append(in.parked, parkedSeq{seq: uint64(p.Seq), ts: headerTS(p.Header), state: p.State, action: p.By.String}) //nolint:gosec // a stored seq is below 2^63
	}
	if in.losses, err = c.packLosses(ctx, capture); err != nil {
		return in, err
	}
	var gapRows []struct {
		Start sql.NullInt64 `db:"gap_start"`
		End   sql.NullInt64 `db:"gap_end"`
	}
	err = c.read.SelectContext(ctx, &gapRows, `SELECT gap_start, gap_end FROM capture_closes
		WHERE capture_id = ? AND (gap_start IS NOT NULL OR gap_end IS NOT NULL)`, capture[:])
	if err != nil {
		return in, err
	}
	for _, g := range gapRows {
		in.crashGaps = append(in.crashGaps, CrashGap{Start: nullable(g.Start), End: nullable(g.End)})
	}
	var conflicts []struct {
		From  int64  `db:"seq_from"`
		To    int64  `db:"seq_to"`
		State string `db:"state"`
	}
	if err := c.read.SelectContext(ctx, &conflicts, `SELECT seq_from, seq_to, state FROM conflicts WHERE capture_id = ? ORDER BY seq_from`, capture[:]); err != nil {
		return in, err
	}
	for _, k := range conflicts {
		in.conflicts = append(in.conflicts, ConflictRange{SeqFrom: uint64(k.From), SeqTo: uint64(k.To), State: k.State}) //nolint:gosec // stored seqs are below 2^63
	}

	return in, nil
}

func (c *Catalog) ranges(ctx context.Context, query string, capture tracepack.UUID) ([]Range, error) {
	var rows []struct {
		From   int64 `db:"seq_from"`
		To     int64 `db:"seq_to"`
		TSFrom int64 `db:"ts_from"`
		TSTo   int64 `db:"ts_to"`
	}
	if err := c.read.SelectContext(ctx, &rows, query, capture[:]); err != nil {
		return nil, err
	}
	rs := make([]Range, 0, len(rows))
	for _, r := range rows {
		rs = append(rs, Range{SeqFrom: uint64(r.From), SeqTo: uint64(r.To), TSFrom: r.TSFrom, TSTo: r.TSTo}) //nolint:gosec // stored seqs are below 2^63
	}

	return rs, nil
}

func (c *Catalog) packLosses(ctx context.Context, capture tracepack.UUID) ([]DeclaredLoss, error) {
	var rows []struct {
		From   sql.NullInt64 `db:"seq_from"`
		To     sql.NullInt64 `db:"seq_to"`
		TSFrom sql.NullInt64 `db:"ts_from"`
		TSTo   sql.NullInt64 `db:"ts_to"`
	}
	err := c.read.SelectContext(ctx, &rows, `SELECT l.seq_from, l.seq_to, l.ts_from, l.ts_to FROM pack_losses l JOIN packs k ON k.pack_id = l.pack_id
		WHERE k.capture_id = ? AND k.state <> 'deleted'`, capture[:])
	if err != nil {
		return nil, err
	}
	ls := make([]DeclaredLoss, 0, len(rows))
	for _, r := range rows {
		ls = append(ls, DeclaredLoss{SeqFrom: unsigned(r.From), SeqTo: unsigned(r.To), TSFrom: nullable(r.TSFrom), TSTo: nullable(r.TSTo)})
	}

	return ls, nil
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

// subtract returns the parts of the coalesced runs a that no run of the coalesced b holds; a cut endpoint has no time of its own.
func subtract(a, b []Range) []Range {
	var out []Range
	for _, r := range a {
		pieces := []Range{r}
		for _, s := range b {
			var next []Range
			for _, p := range pieces {
				if s.SeqTo < p.SeqFrom || s.SeqFrom > p.SeqTo {
					next = append(next, p)
					continue
				}
				if s.SeqFrom > p.SeqFrom {
					next = append(next, Range{SeqFrom: p.SeqFrom, SeqTo: s.SeqFrom - 1, TSFrom: p.TSFrom, TSTo: s.TSFrom})
				}
				if s.SeqTo < p.SeqTo {
					next = append(next, Range{SeqFrom: s.SeqTo + 1, SeqTo: p.SeqTo, TSFrom: s.TSTo, TSTo: p.TSTo})
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
func gaps(explained []Range, frontier uint64, origin *int64) []Gap {
	var out []Gap
	next := uint64(0)
	var before *int64
	for _, r := range explained {
		if r.SeqFrom > frontier {
			break
		}
		if r.SeqFrom > next {
			end := r.TSFrom
			g := Gap{From: next, To: r.SeqFrom - 1, Start: before, End: &end}
			if next == 0 {
				g.Start, g.OpenStart = origin, true
			}
			out = append(out, g)
		}
		ts := r.TSTo
		before, next = &ts, r.SeqTo+1
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

func deref(v *int64) int64 {
	if v == nil {
		return 0
	}

	return *v
}
