// Package records reads a capture's masked records for people (api.md §10.1 *Records*):
// an hour at a time from its read set (catalog.md §7.3), packs whose envelopes overlap merged together in seq order,
// every version of a seq yielded together.
package records

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"slices"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
)

// Sentinel is the close record's seq, 2^63 − 1, which no pack range names (catalog.md §7.3).
const Sentinel = uint64(math.MaxInt64)

const nsPerHour = int64(3600e9)

// The reasons of an Incomplete (operations.md §10.4, record_reads_incomplete_total).
const (
	ReasonGone         = "gone"
	ReasonCorrupt      = "corrupt"
	ReasonCrashGap     = "crash-gap"
	ReasonFooter       = "footer"
	ReasonReadLimit    = "read-limit"
	ReasonTooManyPacks = "too-many-packs"
	ReasonEvicted      = "evicted"
)

// ErrWithheld refuses a read of a withheld capture: outside tracekeep dev, one whose configuration has no policy (T-48).
var ErrWithheld = errors.New("records: the capture is withheld")

// ReadSets reads an hour's read set; *catalog.Catalog is one.
type ReadSets interface {
	ReadSet(ctx context.Context, capture tracepack.UUID, hour int64) (catalog.ReadSet, error)
}

// Reader reads records from the read sets Catalog names and the objects Store holds.
type Reader struct {
	Catalog ReadSets
	Store   store.ObjectStore
	// MaxReaders bounds the packs one merge opens; a cluster above it is not read.
	MaxReaders int
	// HeldBytes is tracepack's MaxHeldBytes for each merge; zero is tracepack's default.
	HeldBytes int64
	// Withholds tells whether a capture bound to a configuration is withheld;
	// it is asked of every read set, so no read, a resumed one included, reads a withheld capture's packs.
	// Nil withholds none.
	Withholds func(ctx context.Context, config *int64) (bool, error)
}

// Message selects records by their HSMS stream and function, any function when AnyFunction is set.
type Message struct {
	Stream, Function uint8
	AnyFunction      bool
}

// Position is a place in a capture's records: after every version of Seq in Hour.
type Position struct {
	Hour int64
	Seq  uint64
}

// Query selects one capture's records.
type Query struct {
	Capture tracepack.UUID
	// From and To bound the records' times, [From, To) in Unix nanoseconds.
	From, To int64
	Kinds    []tracepack.Kind
	Dirs     []tracepack.Dir
	Messages []Message
	// SeqFrom and SeqTo bound the producer seqs, inclusive; either set excludes the close record.
	SeqFrom, SeqTo *uint64
	// After resumes after a position; nil reads from the window's start.
	After *Position
}

// Record is one version of a record, owning its bytes.
type Record struct {
	Hour int64
	tracepack.Record
	Extra []byte
	// Header is the record's HSMS header; HasHeader tells whether it has one.
	Header    tracepack.HSMSHeader
	HasHeader bool
	// Conflict, Excluded and Unobserved flag the version (api.md §10.1 *Items*).
	Conflict, Excluded, Unobserved bool
	// Holders are, for a conflict's version, the read set's packs whose ranges hold the seq.
	Holders []catalog.ReadPack
}

// MaxNamedPacks bounds the packs an Incomplete names, so a marker stays far below a message's size; MorePacks counts the rest.
const MaxNamedPacks = 100

// Incomplete names what a read could not read.
type Incomplete struct {
	Hour   int64
	Reason string
	// Detail is tracepack's or the store's description, never a key.
	Detail string
	// Packs are at most MaxNamedPacks of the packs concerned; MorePacks counts those left out.
	Packs     []tracepack.UUID
	MorePacks int
	// SeqFrom and SeqTo bound the seqs the stream lacks, when known.
	SeqFrom, SeqTo *uint64
}

// Sink receives a read's output in order.
type Sink interface {
	// Versions receives every version of one seq.
	Versions(rs []Record) error
	Incomplete(in Incomplete) error
}

// Read reads q's records into sink, hour by hour; it returns early only on sink's error, the context's or a catalog read's.
func (r *Reader) Read(ctx context.Context, q *Query, sink Sink) error {
	first, last := floorHour(q.From), floorHour(q.To-1)
	if q.After != nil {
		first = max(first, q.After.Hour)
	}
	for h := first; h <= last; h++ {
		var after *uint64
		if q.After != nil && q.After.Hour == h {
			after = &q.After.Seq
		}
		if err := r.hour(ctx, q, h, after, sink); err != nil {
			return err
		}
	}

	return nil
}

// hour reads one hour, re-reading its read set once when an object is found gone.
func (r *Reader) hour(ctx context.Context, q *Query, h int64, after *uint64, sink Sink) error {
	rs, err := r.readSet(ctx, q.Capture, h)
	if err != nil {
		return err
	}
	hr := &hourRead{r: r, q: q, hour: h, last: after, sink: sink}
	if !rs.Scoped {
		if rs.Evicted {
			// Evicted after the capture's completeness was read, or before: either way the hour holds no record now.
			return hr.rest(ReasonEvicted, "retention evicted the hour", nil)
		}

		return nil // an hour without a scope holds no record; completeness reports why
	}
	gone, err := hr.read(ctx, &rs)
	if err != nil || gone == nil {
		return err
	}
	if rs, err = r.readSet(ctx, q.Capture, h); err != nil {
		return err
	}
	if !rs.Scoped {
		return hr.rest(ReasonEvicted, "retention evicted the hour during the read", nil)
	}
	gone2, err := hr.read(ctx, &rs)
	if err != nil || gone2 == nil {
		return err
	}

	return hr.rest(ReasonGone, "a pack's object is gone, again after the hour's read set was read again", nil)
}

// readSet reads an hour's read set, refusing a withheld capture's.
func (r *Reader) readSet(ctx context.Context, capture tracepack.UUID, h int64) (catalog.ReadSet, error) {
	rs, err := r.Catalog.ReadSet(ctx, capture, h)
	if err != nil || r.Withholds == nil {
		return rs, err
	}
	if withheld, err := r.Withholds(ctx, rs.ConfigID); err != nil {
		return catalog.ReadSet{}, err
	} else if withheld {
		return catalog.ReadSet{}, ErrWithheld
	}

	return rs, nil
}

// hourRead is one hour's read in progress: last is the hour's last seq yielded, every version included.
type hourRead struct {
	r    *Reader
	q    *Query
	hour int64
	last *uint64
	sink Sink
}

// rest reports the rest of the hour, above the last seq yielded, as incomplete.
func (hr *hourRead) rest(reason, detail string, packs []tracepack.UUID) error {
	in := Incomplete{Hour: hr.hour, Reason: reason, Detail: detail, Packs: packs}
	if hr.last != nil {
		from := *hr.last + 1
		in.SeqFrom = &from
	}

	return hr.sink.Incomplete(in)
}

// lacks reports a cluster, above the last seq yielded, as incomplete.
func (hr *hourRead) lacks(reason, detail string, c cluster, packs []catalog.ReadPack) error {
	from := c.lo
	if hr.last != nil && *hr.last >= from {
		from = *hr.last + 1
	}
	named, more := namedIDs(packs)

	return hr.sink.Incomplete(Incomplete{Hour: hr.hour, Reason: reason, Detail: detail, Packs: named, MorePacks: more, SeqFrom: &from, SeqTo: &c.hi})
}

// read reads rs's clusters in order; it returns the store's error of an object found gone, ending the read there.
func (hr *hourRead) read(ctx context.Context, rs *catalog.ReadSet) (gone, err error) {
	for _, c := range clusters(rs.Packs) {
		if hr.last != nil && c.hi <= *hr.last {
			continue
		}
		if len(c.packs) > hr.r.MaxReaders {
			named, more := namedIDs(c.packs)
			in := Incomplete{
				Hour: hr.hour, Reason: ReasonTooManyPacks, Detail: fmt.Sprintf("%d packs overlap, at most %d are read together", len(c.packs), hr.r.MaxReaders),
				Packs: named, MorePacks: more, SeqFrom: &c.lo, SeqTo: &c.hi,
			}
			if err := hr.sink.Incomplete(in); err != nil {
				return nil, err
			}

			continue
		}
		if gone, err := hr.merge(ctx, rs, c); gone != nil || err != nil {
			return gone, err
		}
	}

	return nil, nil
}

// merge reads one cluster.
func (hr *hourRead) merge(ctx context.Context, rs *catalog.ReadSet, c cluster) (gone, err error) {
	readers := make([]*tracepack.Reader, 0, len(c.packs))
	defer func() {
		for _, o := range c.objs {
			_ = o.Close()
		}
	}()
	for _, p := range c.packs {
		obj, err := Open(ctx, hr.r.Store, p.Key)
		if errors.Is(err, store.ErrNotFound) {
			return err, nil
		} else if err != nil {
			return nil, fmt.Errorf("records: open pack %s: %w", p.ID, err)
		}
		c.objs = append(c.objs, obj)
		rd, err := tracepack.Open(ctx, obj, obj.Size(), tracepack.ReaderOptions{})
		if err == nil && rd.Header().CaptureID != hr.q.Capture {
			err = errors.New("the pack is of another capture")
		}
		switch {
		case errors.Is(err, store.ErrNotFound):
			return err, nil
		case errors.Is(err, tracepack.ErrReadLimit):
			return nil, hr.lacks(ReasonReadLimit, err.Error(), c, []catalog.ReadPack{p})
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case errors.Is(err, errStore):
			// A store that fails a read is unavailable, not the pack corrupt: the client retries.
			return nil, fmt.Errorf("records: open pack %s: %w", p.ID, err)
		case err != nil:
			return nil, hr.lacks(ReasonCorrupt, err.Error(), c, []catalog.ReadPack{p})
		}
		readers = append(readers, rd)
	}
	g := &group{hr: hr, rs: rs, readers: readers}
	res, err := tracepack.MergeIterate(ctx, readers, hr.filter(), tracepack.MergeIterateOptions{Order: tracepack.OrderCapture, MaxHeldBytes: hr.r.HeldBytes}, g.add)
	if err == nil {
		err = g.flush()
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		return err, nil
	case errors.Is(err, tracepack.ErrReadLimit):
		return nil, hr.lacks(ReasonReadLimit, err.Error(), c, c.packs)
	case err != nil:
		return nil, err
	}

	return nil, hr.defects(&res, c)
}

// defects reports what tracepack found incomplete in a cluster's read.
// A coverage entry is a declared loss, which the capture's completeness already reports.
func (hr *hourRead) defects(res *tracepack.Result, c cluster) error {
	for _, d := range res.Incomplete {
		reason := ReasonCorrupt // a damaged block, an unknown codec, a truncated pack, an index mismatch
		if d.Reason == tracepack.ReasonCoverage {
			continue
		} else if d.Reason == tracepack.ReasonCrashGap {
			reason = ReasonCrashGap
		} else if d.Reason == tracepack.ReasonLimit {
			reason = ReasonReadLimit
		}
		in := Incomplete{Hour: hr.hour, Reason: reason, Detail: d.Reason.String(), Packs: []tracepack.UUID{c.packs[d.Pack].ID}}
		if err := hr.sink.Incomplete(in); err != nil {
			return err
		}
	}
	for _, f := range res.FooterErrs {
		if err := hr.sink.Incomplete(Incomplete{Hour: hr.hour, Reason: ReasonFooter, Detail: f.Err.Error(), Packs: []tracepack.UUID{c.packs[f.Pack].ID}}); err != nil {
			return err
		}
	}

	return nil
}

// filter is the part of the query tracepack's filter can express (api.md §10.1 *Filters*).
func (hr *hourRead) filter() tracepack.Query {
	from, to := max(hr.q.From, hourStart(hr.hour)), min(hr.q.To, hourStart(hr.hour+1))
	f := tracepack.Filter{TimeFrom: &from, TimeTo: &to, Kinds: hr.q.Kinds, Dirs: hr.q.Dirs}
	exact := len(hr.q.Messages) > 0
	for _, m := range hr.q.Messages {
		exact = exact && !m.AnyFunction
		f.SF = append(f.SF, tracepack.SF{Stream: m.Stream, Function: m.Function})
	}
	if !exact {
		f.SF = nil
	}

	return tracepack.Query{Filter: f, Payloads: true}
}

// group collects the versions of one seq and yields them together.
type group struct {
	hr      *hourRead
	rs      *catalog.ReadSet
	readers []*tracepack.Reader
	seq     uint64
	held    []Record
}

func (g *group) add(it *tracepack.Item) error {
	seq := it.Record.Seq
	if g.hr.last != nil && seq <= *g.hr.last {
		return nil
	}
	if len(g.held) > 0 && seq != g.seq {
		if err := g.flush(); err != nil {
			return err
		}
	}
	rec := Record{Hour: g.hr.hour, Record: it.Record, Extra: slices.Clone(it.HeaderExtra), Conflict: it.Conflict}
	rec.Payload = slices.Clone(it.Record.Payload)
	rec.Unobserved = g.readers[it.Pack].Unobserved(&it.Record)
	if rec.Kind == tracepack.KindData || rec.Kind == tracepack.KindControl {
		rec.Header = it.Record.HSMSHeader()
		rec.HasHeader = rec.Header.Available != 0
	}
	if !g.hr.q.admits(&rec) {
		return nil
	}
	rec.Excluded = Excluded(g.rs, seq)
	if rec.Conflict {
		rec.Holders = Holders(g.rs, seq)
	}
	g.seq = seq
	g.held = append(g.held, rec)

	return nil
}

func (g *group) flush() error {
	if len(g.held) == 0 {
		return nil
	}
	if err := g.hr.sink.Versions(g.held); err != nil {
		return err
	}
	seq := g.seq
	g.hr.last, g.held = &seq, nil

	return nil
}

// admits applies what tracepack's filter lacks: the seq range and the whole messages list.
func (q *Query) admits(rec *Record) bool {
	if (q.SeqFrom != nil || q.SeqTo != nil) && rec.Seq == Sentinel {
		return false
	}
	if q.SeqFrom != nil && rec.Seq < *q.SeqFrom || q.SeqTo != nil && rec.Seq > *q.SeqTo {
		return false
	}
	if len(q.Messages) == 0 {
		return true
	}
	if rec.Unobserved {
		return false
	}
	stream := rec.Header.Available&tracepack.FieldValidityStreamAndW != 0
	function := rec.Header.Available&tracepack.FieldValidityFunction != 0
	for _, m := range q.Messages {
		if stream && m.Stream == rec.Header.Stream && (m.AnyFunction || function && m.Function == rec.Header.Function) {
			return true
		}
	}

	return false
}

// Excluded tells whether no staged pack of rs holds seq: by its ranges for a producer seq,
// by a staged archive or close pack for the close record.
func Excluded(rs *catalog.ReadSet, seq uint64) bool {
	for _, p := range rs.Packs {
		if p.State != catalog.PackStaged {
			continue
		}
		if seq == Sentinel {
			if p.Role == catalog.RoleArchive || p.Role == catalog.RoleClose {
				return false
			}

			continue
		}
		for _, rg := range p.Ranges {
			if rg.SeqFrom <= seq && seq <= rg.SeqTo {
				return false
			}
		}
	}

	return true
}

// Holders are rs's packs whose ranges hold seq, without their ranges.
func Holders(rs *catalog.ReadSet, seq uint64) []catalog.ReadPack {
	var out []catalog.ReadPack
	for _, p := range rs.Packs {
		if slices.ContainsFunc(p.Ranges, func(rg catalog.Range) bool { return rg.SeqFrom <= seq && seq <= rg.SeqTo }) {
			p.Ranges = nil
			out = append(out, p)
		}
	}

	return out
}

// cluster is packs whose envelopes overlap, [lo, hi].
type cluster struct {
	lo, hi uint64
	packs  []catalog.ReadPack
	objs   []store.Object
}

// clusters splits packs into clusters of overlapping envelopes, in envelope order.
// A pack's envelope runs over its ranges, on to the sentinel for an archive or a close pack, and is the sentinel alone without ranges.
func clusters(packs []catalog.ReadPack) []cluster {
	type env struct {
		lo, hi uint64
		p      catalog.ReadPack
	}
	envs := make([]env, 0, len(packs))
	for _, p := range packs {
		e := env{lo: Sentinel, hi: 0, p: p}
		for _, rg := range p.Ranges {
			e.lo, e.hi = min(e.lo, rg.SeqFrom), max(e.hi, rg.SeqTo)
		}
		if len(p.Ranges) == 0 || p.Role == catalog.RoleArchive || p.Role == catalog.RoleClose {
			e.hi = Sentinel
		}
		envs = append(envs, e)
	}
	// Staged before excluded within a cluster: the read set's order, kept by a stable sort on lo.
	slices.SortStableFunc(envs, func(a, b env) int { return cmp.Compare(a.lo, b.lo) })
	var out []cluster
	for _, e := range envs {
		if n := len(out); n > 0 && e.lo <= out[n-1].hi {
			out[n-1].hi = max(out[n-1].hi, e.hi)
			out[n-1].packs = append(out[n-1].packs, e.p)

			continue
		}
		out = append(out, cluster{lo: e.lo, hi: e.hi, packs: []catalog.ReadPack{e.p}})
	}

	return out
}

// namedIDs are the packs' ids, at most MaxNamedPacks of them, and how many it left out.
func namedIDs(packs []catalog.ReadPack) ([]tracepack.UUID, int) {
	n := min(len(packs), MaxNamedPacks)
	out := make([]tracepack.UUID, 0, n)
	for _, p := range packs[:n] {
		out = append(out, p.ID)
	}

	return out, len(packs) - n
}

// hourStart is the start of hour h in nanoseconds, saturated at int64's bounds.
func hourStart(h int64) int64 {
	switch {
	case h > math.MaxInt64/nsPerHour:
		return math.MaxInt64
	case h < math.MinInt64/nsPerHour:
		return math.MinInt64
	default:
		return h * nsPerHour
	}
}

func floorHour(ns int64) int64 {
	q := ns / nsPerHour
	if ns%nsPerHour < 0 {
		q--
	}

	return q
}

// Open opens key's object, whose errors, its own and its reads', name no key: a store's errors quote it,
// and what a read reports reaches a person's answer and the logs.
func Open(ctx context.Context, st store.ObjectStore, key string) (store.Object, error) {
	obj, err := st.Open(ctx, key)
	if err != nil {
		return nil, Keyless(err)
	}

	return keyless{obj}, nil
}

type keyless struct{ store.Object }

func (o keyless) ReadAt(p []byte, off int64) (int, error) {
	n, err := o.Object.ReadAt(p, off)
	if err != nil && !errors.Is(err, io.EOF) {
		err = Keyless(err)
	}

	return n, err
}

// errStore is a store failure other than a missing object, without the store's text.
var errStore = errors.New("records: the store failed to read an object")

// Keyless is a store's err without its text, which names the object's key:
// store.ErrNotFound for a missing object, the context's error, or a store failure, a timeout said so.
func Keyless(err error) error {
	var nerr net.Error
	switch {
	case errors.Is(err, store.ErrNotFound):
		return store.ErrNotFound
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.As(err, &nerr) && nerr.Timeout():
		return fmt.Errorf("%w: timed out", errStore)
	default:
		return errStore
	}
}
