package catalog

import (
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

// admittedStates are the states of an admitted capture, as SQL.
const admittedStates = `('open', 'stopped', 'closed')`

// selectCaptures reads the captures sel selects, ordered by device, origin and id, with their recorders' other captures.
func (c *Catalog) selectCaptures(ctx context.Context, tx *sqlx.Tx, sel Selector, q time.Duration) ([]CaptureInfo, error) {
	query := `SELECT capture_id, state, device, recorder_instance_id, previous_capture_id, origin_ns, highest_seq_ts, config_id, descriptor
		FROM captures WHERE tool_id = ? AND state IN ` + admittedStates + `
		AND COALESCE(origin_ns, 0) < ? AND (state = 'open' OR COALESCE(highest_seq_ts, origin_ns, 0) >= ?)`
	args := []any{sel.ToolID, sel.To, sel.From}
	if len(sel.Devices) > 0 {
		query += ` AND device IN (` + strings.TrimSuffix(strings.Repeat("?, ", len(sel.Devices)), ", ") + `)`
		for _, d := range sel.Devices {
			args = append(args, d)
		}
	}
	if sel.Capture != nil {
		query += ` AND capture_id = ?`
		args = append(args, sel.Capture[:])
	}
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
