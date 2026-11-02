package catalog

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
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
	// Each is put in one order of its own, so an answer never depends on the plan or the form of the read that found its rows.
	out.Losses = slices.SortedFunc(slices.Values(in.losses), func(a, b DeclaredLoss) int {
		return cmp.Or(cmpPtr(a.SeqFrom, b.SeqFrom), cmpPtr(a.SeqTo, b.SeqTo), cmpPtr(a.TSFrom, b.TSFrom), cmpPtr(a.TSTo, b.TSTo))
	})
	out.CrashGaps = slices.SortedFunc(slices.Values(in.crashGaps), func(a, b CrashGap) int {
		return cmp.Or(cmpPtr(a.Start, b.Start), cmpPtr(a.End, b.End))
	})
	out.Conflicts = slices.SortedFunc(slices.Values(in.conflicts), func(a, b ConflictRange) int {
		return cmp.Or(cmp.Compare(a.SeqFrom, b.SeqFrom), cmp.Compare(a.SeqTo, b.SeqTo), cmp.Compare(a.State, b.State))
	})
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

// The queries of a completeness's inputs (completenessInputs), each over the captures its IN (?) names, selecting the key first.
// The parked rows come in seq order; classify orders the rest itself.
var (
	frontierQuery = keyed(`SELECT capture_id, highest_seq, origin_ns FROM captures WHERE capture_id IN (?)`)
	retainedQuery = keyed(`SELECT r.capture_id, r.seq_from, r.seq_to, r.ts_from, r.ts_to FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
		WHERE r.capture_id IN (?) AND k.state <> 'deleted'`)
	evictedQuery = keyed(`SELECT capture_id, seq_from, seq_to, ts_from, ts_to FROM evicted_ranges WHERE capture_id IN (?)`)
	parkedQuery  = keyed(`SELECT capture_id, seq, header, state, terminated_by FROM parked WHERE capture_id IN (?) ORDER BY capture_id, seq`)
	lossesQuery  = keyed(`SELECT k.capture_id, l.seq_from, l.seq_to, l.ts_from, l.ts_to FROM pack_losses l JOIN packs k ON k.pack_id = l.pack_id
		WHERE k.capture_id IN (?) AND k.state <> 'deleted'`)
	crashGapsQuery = keyed(`SELECT capture_id, gap_start, gap_end FROM capture_closes
		WHERE capture_id IN (?) AND (gap_start IS NOT NULL OR gap_end IS NOT NULL)`)
	conflictsQuery = keyed(`SELECT capture_id, seq_from, seq_to, state FROM conflicts WHERE capture_id IN (?) ORDER BY capture_id, seq_from`)
)

// inputQuery is an input's query for a chunk of captures, and for one capture without the key column,
// so Completeness, one capture, reads each row as it did before listings were chunked.
type inputQuery struct{ many, one string }

// keyed derives many's one-capture form: its key column dropped from the selection and the order, its IN (?) an equality.
func keyed(many string) inputQuery {
	one := regexp.MustCompile(`^SELECT (\w+\.)?capture_id, `).ReplaceAllString(many, "SELECT ")
	one = strings.Replace(one, "ORDER BY capture_id, ", "ORDER BY ", 1)

	return inputQuery{many: many, one: strings.Replace(one, "IN (?)", "= ?", 1)}
}

// sql is q for n captures: its one-capture form, or its IN (?) widened to n placeholders.
func (q inputQuery) sql(n int) string {
	if n == 1 {
		return q.one
	}

	return strings.Replace(q.many, "IN (?)", "IN (?"+strings.Repeat(", ?", n-1)+")", 1)
}

// captureKey scans a capture_id column in place, copying the driver's bytes into the array.
type captureKey tracepack.UUID

// Scan copies the driver's bytes into k.
func (k *captureKey) Scan(src any) error {
	b, ok := src.([]byte)
	if !ok || len(b) != len(k) {
		return fmt.Errorf("catalog: capture_id is %T of %d bytes", src, len(b))
	}
	copy(k[:], b)

	return nil
}

// completenessInputs reads the rows the completeness of each of captures classifies, at most completenessChunk, inside the snapshot tx,
// in the order of captures; a capture the catalog does not hold fails the read with sql.ErrNoRows.
// Each row is scanned straight into its capture's inputs, with no buffer of rows between.
func (c *Catalog) completenessInputs(ctx context.Context, tx *sqlx.Tx, captures []tracepack.UUID) ([]completenessInputs, error) {
	ins := make([]completenessInputs, len(captures))
	of := make(map[tracepack.UUID]int, len(captures))
	args := make([]any, len(captures))
	for i, id := range captures {
		of[id], args[i] = i, id[:]
	}
	var key captureKey
	// read runs query over the chunk, scans each row's columns into dest, after its key when the chunk is wider than one capture,
	// and hands add the row's capture's inputs; the hook after it lets a test commit between any two reads, or two chunks.
	query := func(q inputQuery, add func(*completenessInputs), dest []any) (n int, err error) {
		rows, err := tx.QueryContext(ctx, q.sql(len(captures)), args...)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		if len(captures) > 1 {
			dest = append([]any{&key}, dest...)
		}
		for ; rows.Next(); n++ {
			if err := rows.Scan(dest...); err != nil {
				return n, err
			}
			i, ok := 0, true
			if len(captures) > 1 {
				i, ok = of[tracepack.UUID(key)]
			}
			if !ok {
				return n, fmt.Errorf("row of capture %s, not one read", tracepack.UUID(key))
			}
			add(&ins[i])
		}

		return n, rows.Err()
	}
	read := func(q inputQuery, add func(*completenessInputs), dest ...any) (int, error) {
		n, err := query(q, add, dest)
		if err == nil {
			c.hook(hookCompletenessRead)
		}

		return n, err
	}
	var highest, origin, from, to, tsFrom, tsTo sql.NullInt64
	n, err := read(frontierQuery, func(p *completenessInputs) {
		p.highest, p.hasHighest, p.origin = uint64(highest.Int64), highest.Valid, nullable(origin) //nolint:gosec // a stored seq is below 2^63
	}, &highest, &origin)
	if err != nil {
		return nil, err
	}
	if n != len(of) {
		return nil, sql.ErrNoRows
	}
	var seqFrom, seqTo, ts, tsEnd, seq int64
	addRange := func(rs *[]Range) {
		*rs = append(*rs, Range{SeqFrom: uint64(seqFrom), SeqTo: uint64(seqTo), TSFrom: ts, TSTo: tsEnd}) //nolint:gosec // stored seqs are below 2^63
	}
	if _, err := read(retainedQuery, func(p *completenessInputs) { addRange(&p.retained) }, &seqFrom, &seqTo, &ts, &tsEnd); err != nil {
		return nil, err
	}
	if _, err := read(evictedQuery, func(p *completenessInputs) { addRange(&p.evicted) }, &seqFrom, &seqTo, &ts, &tsEnd); err != nil {
		return nil, err
	}
	var header sql.RawBytes
	var state VersionState
	var by sql.NullString
	var conflictState string
	addParked := func(p *completenessInputs) {
		p.parked = append(p.parked, parkedSeq{seq: uint64(seq), ts: headerTS(header), state: state, action: by.String}) //nolint:gosec // a stored seq is below 2^63
	}
	addLoss := func(p *completenessInputs) {
		p.losses = append(p.losses, DeclaredLoss{SeqFrom: unsigned(from), SeqTo: unsigned(to), TSFrom: nullable(tsFrom), TSTo: nullable(tsTo)})
	}
	addGap := func(p *completenessInputs) {
		p.crashGaps = append(p.crashGaps, CrashGap{Start: nullable(from), End: nullable(to)})
	}
	addConflict := func(p *completenessInputs) {
		p.conflicts = append(p.conflicts, ConflictRange{SeqFrom: uint64(seqFrom), SeqTo: uint64(seqTo), State: conflictState}) //nolint:gosec // stored seqs are below 2^63
	}
	if _, err := read(parkedQuery, addParked, &seq, &header, &state, &by); err != nil {
		return nil, err
	}
	if _, err := read(lossesQuery, addLoss, &from, &to, &tsFrom, &tsTo); err != nil {
		return nil, err
	}
	if _, err := read(crashGapsQuery, addGap, &from, &to); err != nil {
		return nil, err
	}
	if _, err := read(conflictsQuery, addConflict, &seqFrom, &seqTo, &conflictState); err != nil {
		return nil, err
	}

	return ins, nil
}

// cmpPtr orders two optional values, an absent one first.
func cmpPtr[T cmp.Ordered](a, b *T) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	default:
		return cmp.Compare(*a, *b)
	}
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
