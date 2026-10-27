package catalog

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/arloliu/go-secs/tracepack"
)

// Selector names what a read covers (catalog.md §7.3 *Selection*):
// a tool, optionally its devices (none means every device) or one capture, and the window [From, To) in UTC nanoseconds.
type Selector struct {
	ToolID   string
	Devices  []string
	Capture  *tracepack.UUID
	From, To int64
}

// filter is the SQL condition on sel's devices and capture for the captures table under prefix, and its args after lead.
func (sel Selector) filter(prefix string, lead ...any) (string, []any) {
	var cond string
	args := lead
	if len(sel.Devices) > 0 {
		cond += ` AND ` + prefix + `device IN (` + strings.TrimSuffix(strings.Repeat("?, ", len(sel.Devices)), ", ") + `)`
		for _, d := range sel.Devices {
			args = append(args, d)
		}
	}
	if sel.Capture != nil {
		cond += ` AND ` + prefix + `capture_id = ?`
		args = append(args, sel.Capture[:])
	}

	return cond, args
}

// CaptureInfo is one capture as ListCaptures reports it (api.md §10.1).
type CaptureInfo struct {
	ID                 tracepack.UUID
	State              string
	Device             string
	RecorderInstanceID tracepack.UUID
	PreviousCaptureID  *tracepack.UUID
	// OriginNS is the capture's capture_origin_utc_ns, and LastRecordNS the time of its highest seq's record, nil before any.
	OriginNS     int64
	LastRecordNS *int64
	// ConfigID is the processing configuration bound at admission.
	ConfigID *int64
	// Thinning, EquipmentEndpoint and DeviceID are the start descriptor's.
	Thinning          []tracepack.Thinning
	EquipmentEndpoint *string
	DeviceID          *uint64
	// Overlap is set when the span of another capture of the same tool and recorder instance meets this one's.
	Overlap bool
	// Unregistered are the capture's unregistered intervals.
	Unregistered []UnregisteredInterval
}

// UnregisteredInterval is time before a capture's origin that no capture of its recorder instance accounts for:
// from the latest span end of the earlier captures, or from an unknown start when its predecessor was never admitted.
type UnregisteredInterval struct {
	Start *int64
	End   int64
}

// DeviceInfo is one device as ListDevices reports it (api.md §10.1).
type DeviceInfo struct {
	Name string
	// EquipmentEndpoint and DeviceID are the latest selected capture's.
	EquipmentEndpoint *string
	DeviceID          *uint64
	// OpenCaptures counts the selected captures still open.
	OpenCaptures int
	// Overlap is set when any selected capture of the device overlaps another.
	Overlap bool
	// LastRecordNS is the latest record time of the selected captures, nil before any.
	LastRecordNS *int64
	// Gap is set when a gap of a selected capture meets the window, an unknown bound counting as meeting it.
	Gap bool
}

// span is the part of a capture's row its span and its recorder's other captures are computed from.
type span struct {
	id       tracepack.UUID
	instance tracepack.UUID
	origin   int64
	last     sql.NullInt64
}

// end is the span's end: the last record's time, or the origin alone before any record.
func (s span) end() int64 {
	if s.last.Valid {
		return s.last.Int64
	}

	return s.origin
}

// ListCaptures lists the captures sel selects with their overlap flags and unregistered intervals against the quiet period q
// (catalog.md §7.3), from one snapshot.
func (c *Catalog) ListCaptures(ctx context.Context, sel Selector, q time.Duration) ([]CaptureInfo, error) {
	var out []CaptureInfo
	err := c.snapshot(ctx, func(tx *sqlx.Tx) error {
		var err error
		out, err = c.selectCaptures(ctx, tx, sel, q)

		return err
	})
	if err != nil {
		return nil, fmt.Errorf("catalog: list captures: %w", err)
	}

	return out, nil
}

// ListDevices lists per device what the captures sel selects report (catalog.md §7.3), from one snapshot.
func (c *Catalog) ListDevices(ctx context.Context, sel Selector, q time.Duration) ([]DeviceInfo, error) {
	var out []DeviceInfo
	err := c.snapshot(ctx, func(tx *sqlx.Tx) error {
		caps, err := c.selectCaptures(ctx, tx, sel, q)
		if err != nil {
			return err
		}
		byName := map[string]*DeviceInfo{}
		latest := map[string]CaptureInfo{}
		for _, ci := range caps {
			d := byName[ci.Device]
			if d == nil {
				d = &DeviceInfo{Name: ci.Device}
				byName[ci.Device] = d
			}
			if l, ok := latest[ci.Device]; !ok || ci.OriginNS > l.OriginNS {
				latest[ci.Device] = ci
				d.EquipmentEndpoint, d.DeviceID = ci.EquipmentEndpoint, ci.DeviceID
			}
			if ci.State == StateOpen {
				d.OpenCaptures++
			}
			d.Overlap = d.Overlap || ci.Overlap
			if ci.LastRecordNS != nil && (d.LastRecordNS == nil || *ci.LastRecordNS > *d.LastRecordNS) {
				d.LastRecordNS = ci.LastRecordNS
			}
			if d.Gap {
				continue
			}
			in, err := c.completenessInputs(ctx, tx, ci.ID)
			if err != nil {
				return err
			}
			d.Gap = slices.ContainsFunc(in.classify().Gaps, func(g Gap) bool { return g.meets(sel.From, sel.To) })
		}
		for _, d := range byName {
			out = append(out, *d)
		}
		slices.SortFunc(out, func(a, b DeviceInfo) int { return cmp.Compare(a.Name, b.Name) })

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("catalog: list devices: %w", err)
	}

	return out, nil
}

// meets reports whether the gap's time interval meets [from, to), a nil bound unbounded.
func (g Gap) meets(from, to int64) bool {
	return (g.Start == nil || *g.Start < to) && (g.End == nil || *g.End > from)
}

// admittedStates are the states of an admitted capture, as SQL.
const admittedStates = `('open', 'stopped', 'closed')`

// selectCaptures reads the captures sel selects, ordered by device, origin and id, with their recorders' other captures.
func (c *Catalog) selectCaptures(ctx context.Context, tx *sqlx.Tx, sel Selector, q time.Duration) ([]CaptureInfo, error) {
	query := `SELECT capture_id, state, device, recorder_instance_id, previous_capture_id, origin_ns, highest_seq_ts, config_id, descriptor
		FROM captures WHERE tool_id = ? AND state IN ` + admittedStates + `
		AND COALESCE(origin_ns, 0) < ? AND (state = 'open' OR COALESCE(highest_seq_ts, origin_ns, 0) >= ?)`
	filter, args := sel.filter("", sel.ToolID, sel.To, sel.From)
	query += filter
	var rows []struct {
		ID         []byte         `db:"capture_id"`
		State      string         `db:"state"`
		Device     sql.NullString `db:"device"`
		Instance   []byte         `db:"recorder_instance_id"`
		Previous   []byte         `db:"previous_capture_id"`
		Origin     sql.NullInt64  `db:"origin_ns"`
		Last       sql.NullInt64  `db:"highest_seq_ts"`
		Config     sql.NullInt64  `db:"config_id"`
		Descriptor []byte         `db:"descriptor"`
	}
	if err := tx.SelectContext(ctx, &rows, query+` ORDER BY device, origin_ns, capture_id`, args...); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]CaptureInfo, 0, len(rows))
	spans := make([]span, 0, len(rows))
	var instances, previous []tracepack.UUID
	for _, r := range rows {
		ci := CaptureInfo{
			ID: tracepack.UUID(r.ID), State: r.State, Device: r.Device.String, RecorderInstanceID: tracepack.UUID(r.Instance),
			OriginNS: r.Origin.Int64, LastRecordNS: nullable(r.Last), ConfigID: nullable(r.Config),
		}
		if r.Previous != nil {
			p := tracepack.UUID(r.Previous)
			ci.PreviousCaptureID = &p
			previous = append(previous, p)
		}
		if r.Descriptor != nil {
			m, err := tracepack.DecodeDescriptor(r.Descriptor)
			if err != nil {
				return nil, fmt.Errorf("the descriptor of admitted capture %s: %w", ci.ID, err)
			}
			ci.Thinning, ci.EquipmentEndpoint, ci.DeviceID = m.Thinning, m.EquipmentEndpoint, m.DeviceID
		}
		instances = append(instances, ci.RecorderInstanceID)
		out = append(out, ci)
		spans = append(spans, span{id: ci.ID, instance: ci.RecorderInstanceID, origin: ci.OriginNS, last: r.Last})
	}
	peers, err := recorderSpans(ctx, tx, sel.ToolID, instances)
	if err != nil {
		return nil, err
	}
	known, err := admittedIn(ctx, tx, previous)
	if err != nil {
		return nil, err
	}
	for i := range out {
		ci := &out[i]
		ci.Overlap, ci.Unregistered = relate(spans[i], peers[ci.RecorderInstanceID], int64(q))
		if p := ci.PreviousCaptureID; p != nil && !known[*p] {
			ci.Unregistered = append(ci.Unregistered, UnregisteredInterval{End: ci.OriginNS})
		}
	}

	return out, nil
}

// relate computes self's overlap with its recorder's other captures and the interval before it that none accounts for, past q.
func relate(self span, peers []span, q int64) (bool, []UnregisteredInterval) {
	overlap, earlier := false, false
	var latest int64
	for _, p := range peers {
		if p.id == self.id {
			continue
		}
		if p.origin <= self.end() && self.origin <= p.end() {
			overlap = true
		}
		if p.origin < self.origin && (!earlier || p.end() > latest) {
			earlier, latest = true, p.end()
		}
	}
	if earlier && self.origin-latest > q {
		start := latest

		return overlap, []UnregisteredInterval{{Start: &start, End: self.origin}}
	}

	return overlap, nil
}

// recorderSpans reads the spans of every admitted capture of tool and the recorder instances, keyed by instance.
func recorderSpans(ctx context.Context, tx *sqlx.Tx, tool string, instances []tracepack.UUID) (map[tracepack.UUID][]span, error) {
	slices.SortFunc(instances, compareUUIDs)
	instances = slices.Compact(instances)
	query, args := inList(`SELECT capture_id, recorder_instance_id, origin_ns, highest_seq_ts FROM captures
		WHERE tool_id = ? AND state IN `+admittedStates+` AND recorder_instance_id IN`, instances, tool)
	var rows []struct {
		ID       []byte        `db:"capture_id"`
		Instance []byte        `db:"recorder_instance_id"`
		Origin   sql.NullInt64 `db:"origin_ns"`
		Last     sql.NullInt64 `db:"highest_seq_ts"`
	}
	if err := tx.SelectContext(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	out := map[tracepack.UUID][]span{}
	for _, r := range rows {
		s := span{id: tracepack.UUID(r.ID), instance: tracepack.UUID(r.Instance), origin: r.Origin.Int64, last: r.Last}
		out[s.instance] = append(out[s.instance], s)
	}

	return out, nil
}

// admittedIn reports which of ids name an admitted capture.
func admittedIn(ctx context.Context, tx *sqlx.Tx, ids []tracepack.UUID) (map[tracepack.UUID]bool, error) {
	out := map[tracepack.UUID]bool{}
	if len(ids) == 0 {
		return out, nil
	}
	query, args := inList(`SELECT capture_id FROM captures WHERE state IN `+admittedStates+` AND capture_id IN`, ids)
	var rows [][]byte
	if err := tx.SelectContext(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	for _, r := range rows {
		out[tracepack.UUID(r)] = true
	}

	return out, nil
}

func compareUUIDs(a, b tracepack.UUID) int { return strings.Compare(string(a[:]), string(b[:])) }

// Cursor is a position in the change cursor (catalog.md §7.3): every event up to Seq of Epoch has been read.
type Cursor struct {
	Epoch, Seq int64
}

// Listing is what ListPacks returns (catalog.md §7.3).
type Listing struct {
	// Cursor is the watermark and epoch read before the listing, which a client continues ListScopeChanges from.
	Cursor Cursor
	// Hours are the requested hours in order.
	Hours []HourListing
	// Captures are the selected captures, each with its completeness.
	Captures []CaptureListing
}

// HourListing is one requested hour: its scopes, or, with none, what the hour holds instead.
type HourListing struct {
	Hour   int64
	Scopes []ScopeListing
	// Evicted names the captures whose ranges in the hour retention evicted, for an hour with no scope.
	Evicted []tracepack.UUID
	// Deferred and Losses are the parked seqs of an hour with no scope, as their captures' completeness classifies them.
	Deferred []Deferred
	Losses   []DeclaredLoss
	// NoData is set for an hour with no scope that holds nothing else; Spanned then tells whether a selected capture's span meets it.
	NoData, Spanned bool
}

// ScopeListing is one capture's scope of an hour with its packs.
type ScopeListing struct {
	Capture  tracepack.UUID
	State    string
	Revision int64
	Packs    []PackListing
}

// PackListing is one pack of a scope that is not deleted, with its ranges.
type PackListing struct {
	ID          tracepack.UUID
	Role        string
	State       string
	Size        int64
	FormatMinor int
	Ranges      []Range
}

// CaptureListing is a selected capture with its completeness.
type CaptureListing struct {
	CaptureInfo
	Completeness Completeness
}

// ListPacks lists the hours of sel's window (catalog.md §7.3) from one snapshot whose first statement reads the watermark and the epoch,
// so an event committed during the listing lies above the returned cursor.
func (c *Catalog) ListPacks(ctx context.Context, sel Selector, q time.Duration) (Listing, error) {
	var out Listing
	err := c.snapshot(ctx, func(tx *sqlx.Tx) error {
		err := tx.QueryRowxContext(ctx, `SELECT epoch, value FROM counters WHERE name = 'scope_events'`).Scan(&out.Cursor.Epoch, &out.Cursor.Seq)
		if err != nil {
			return err
		}
		c.hook(hookWatermarkRead)
		caps, err := c.selectCaptures(ctx, tx, sel, q)
		if err != nil {
			return err
		}
		ids := make([]tracepack.UUID, 0, len(caps))
		for _, ci := range caps {
			in, err := c.completenessInputs(ctx, tx, ci.ID)
			if err != nil {
				return err
			}
			out.Captures = append(out.Captures, CaptureListing{CaptureInfo: ci, Completeness: in.classify()})
			ids = append(ids, ci.ID)
		}
		first, last := floorHour(sel.From), floorHour(sel.To-1)
		out.Hours, err = listHours(ctx, tx, ids, first, last)
		if err != nil {
			return err
		}
		for i := range out.Hours {
			out.Hours[i].fill(out.Captures)
		}

		return nil
	})
	if err != nil {
		return Listing{}, fmt.Errorf("catalog: list packs: %w", err)
	}

	return out, nil
}

// fill completes an hour with no scope from the captures' completeness: its parked seqs, or no-data with whether a span meets it.
func (hl *HourListing) fill(caps []CaptureListing) {
	if len(hl.Scopes) > 0 || len(hl.Evicted) > 0 {
		return
	}
	start, end := hl.Hour*nsPerHour, (hl.Hour+1)*nsPerHour
	for _, cl := range caps {
		for _, d := range cl.Completeness.Deferred {
			if floorHour(d.TS) == hl.Hour {
				hl.Deferred = append(hl.Deferred, d)
			}
		}
		for _, l := range cl.Completeness.Losses {
			if l.ActionID != "" && l.TSFrom != nil && floorHour(*l.TSFrom) == hl.Hour {
				hl.Losses = append(hl.Losses, l)
			}
		}
		spanEnd := end
		if cl.State != StateOpen {
			spanEnd = cl.OriginNS
			if cl.LastRecordNS != nil {
				spanEnd = *cl.LastRecordNS
			}
		}
		hl.Spanned = hl.Spanned || cl.OriginNS < end && spanEnd >= start
	}
	hl.NoData = len(hl.Deferred) == 0 && len(hl.Losses) == 0
}

// listHours reads the scopes, packs, ranges and evicted hours of the captures ids for the hours first through last.
func listHours(ctx context.Context, tx *sqlx.Tx, ids []tracepack.UUID, first, last int64) ([]HourListing, error) {
	out := make([]HourListing, 0, last-first+1)
	for h := first; h <= last; h++ {
		out = append(out, HourListing{Hour: h})
	}
	if len(ids) == 0 {
		return out, nil
	}
	at := func(hour int64) *HourListing { return &out[hour-first] }
	var scopes []struct {
		Capture  []byte `db:"capture_id"`
		Hour     int64  `db:"hour"`
		State    string `db:"state"`
		Revision int64  `db:"revision"`
	}
	query, args := inList(`SELECT capture_id, hour, state, revision FROM scopes WHERE hour BETWEEN ? AND ? AND capture_id IN`, ids, first, last)
	if err := tx.SelectContext(ctx, &scopes, query+` ORDER BY hour, capture_id`, args...); err != nil {
		return nil, err
	}
	type key struct {
		capture tracepack.UUID
		hour    int64
	}
	index := map[key]int{}
	for _, s := range scopes {
		hl := at(s.Hour)
		index[key{tracepack.UUID(s.Capture), s.Hour}] = len(hl.Scopes)
		hl.Scopes = append(hl.Scopes, ScopeListing{Capture: tracepack.UUID(s.Capture), State: s.State, Revision: s.Revision})
	}
	var packs []struct {
		ID          []byte `db:"pack_id"`
		Capture     []byte `db:"capture_id"`
		Hour        int64  `db:"hour"`
		Role        string `db:"role"`
		State       string `db:"state"`
		Size        int64  `db:"size"`
		FormatMinor int    `db:"format_minor"`
	}
	query, args = inList(`SELECT pack_id, capture_id, hour, role, state, size, format_minor FROM packs
		WHERE state <> 'deleted' AND hour BETWEEN ? AND ? AND capture_id IN`, ids, first, last)
	if err := tx.SelectContext(ctx, &packs, query+` ORDER BY hour, capture_id, pack_id`, args...); err != nil {
		return nil, err
	}
	type slot struct {
		hour         int64
		scope, index int
	}
	where := map[tracepack.UUID]slot{}
	for _, p := range packs {
		i, ok := index[key{tracepack.UUID(p.Capture), p.Hour}]
		if !ok {
			continue // a pack's registration creates its scope, and eviction deletes both
		}
		s := &at(p.Hour).Scopes[i]
		where[tracepack.UUID(p.ID)] = slot{hour: p.Hour, scope: i, index: len(s.Packs)}
		s.Packs = append(s.Packs, PackListing{ID: tracepack.UUID(p.ID), Role: p.Role, State: p.State, Size: p.Size, FormatMinor: p.FormatMinor})
	}
	var ranges []struct {
		Pack   []byte `db:"pack_id"`
		From   int64  `db:"seq_from"`
		To     int64  `db:"seq_to"`
		TSFrom int64  `db:"ts_from"`
		TSTo   int64  `db:"ts_to"`
	}
	query, args = inList(`SELECT r.pack_id, r.seq_from, r.seq_to, r.ts_from, r.ts_to FROM pack_ranges r JOIN packs k ON k.pack_id = r.pack_id
		WHERE k.state <> 'deleted' AND k.hour BETWEEN ? AND ? AND k.capture_id IN`, ids, first, last)
	if err := tx.SelectContext(ctx, &ranges, query+` ORDER BY r.pack_id, r.seq_from`, args...); err != nil {
		return nil, err
	}
	for _, r := range ranges {
		if w, ok := where[tracepack.UUID(r.Pack)]; ok {
			p := &at(w.hour).Scopes[w.scope].Packs[w.index]
			p.Ranges = append(p.Ranges, Range{SeqFrom: uint64(r.From), SeqTo: uint64(r.To), TSFrom: r.TSFrom, TSTo: r.TSTo}) //nolint:gosec // stored seqs are below 2^63
		}
	}
	var evicted []struct {
		Capture []byte `db:"capture_id"`
		Hour    int64  `db:"hour"`
	}
	query, args = inList(`SELECT DISTINCT capture_id, hour FROM evicted_ranges WHERE hour BETWEEN ? AND ? AND capture_id IN`, ids, first, last)
	if err := tx.SelectContext(ctx, &evicted, query+` ORDER BY hour, capture_id`, args...); err != nil {
		return nil, err
	}
	for _, e := range evicted {
		if hl := at(e.Hour); len(hl.Scopes) == 0 {
			hl.Evicted = append(hl.Evicted, tracepack.UUID(e.Capture))
		}
	}

	return out, nil
}

// ScopeEvent is one event of the change cursor (catalog.md §7.3).
type ScopeEvent struct {
	Seq      int64
	Capture  tracepack.UUID
	Hour     int64
	Event    string
	Revision int64
	At       int64
}

// Changes is one page of ListScopeChanges.
type Changes struct {
	Events []ScopeEvent
	// Next is the cursor to continue from.
	Next Cursor
	// Stale is set when the cursor carries another epoch or a pruned seq lies above it:
	// the client re-lists the window from ListPacks and continues from the cursor that listing returns.
	Stale bool
}

// ListScopeChanges returns up to limit events above after whose capture sel's tool and devices select, in seq order (catalog.md §7.3),
// from one snapshot whose first statement reads the watermark;
// the next cursor is the last event's seq for a full page, else the watermark, so other tools' events never hold it back.
func (c *Catalog) ListScopeChanges(ctx context.Context, after Cursor, sel Selector, limit int) (Changes, error) {
	if limit <= 0 {
		return Changes{}, fmt.Errorf("catalog: list scope changes: limit %d is not positive", limit)
	}
	var out Changes
	err := c.snapshot(ctx, func(tx *sqlx.Tx) error {
		var w Cursor
		var low int64
		err := tx.QueryRowxContext(ctx, `SELECT epoch, value, low_water FROM counters WHERE name = 'scope_events'`).Scan(&w.Epoch, &w.Seq, &low)
		if err != nil {
			return err
		}
		c.hook(hookWatermarkRead)
		if after.Epoch != w.Epoch || after.Seq < low-1 {
			out.Stale = true

			return nil
		}
		query := `SELECT e.seq, e.capture_id, e.hour, e.event, e.revision, e.at FROM scope_events e JOIN captures k ON k.capture_id = e.capture_id
			WHERE e.seq > ? AND e.seq <= ? AND k.tool_id = ?`
		filter, args := sel.filter("k.", after.Seq, w.Seq, sel.ToolID)
		query += filter
		var rows []struct {
			Seq      int64  `db:"seq"`
			Capture  []byte `db:"capture_id"`
			Hour     int64  `db:"hour"`
			Event    string `db:"event"`
			Revision int64  `db:"revision"`
			At       int64  `db:"at"`
		}
		if err := tx.SelectContext(ctx, &rows, query+` ORDER BY e.seq LIMIT ?`, append(args, limit)...); err != nil {
			return err
		}
		for _, r := range rows {
			out.Events = append(out.Events, ScopeEvent{Seq: r.Seq, Capture: tracepack.UUID(r.Capture), Hour: r.Hour, Event: r.Event, Revision: r.Revision, At: r.At})
		}
		out.Next = Cursor{Epoch: w.Epoch, Seq: max(after.Seq, w.Seq)}
		if len(rows) == limit {
			out.Next.Seq = rows[len(rows)-1].Seq
		}

		return nil
	})
	if err != nil {
		return Changes{}, fmt.Errorf("catalog: list scope changes: %w", err)
	}

	return out, nil
}
