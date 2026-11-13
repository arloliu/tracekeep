package admin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// Pending is a pending capture of tracekeep status.
type Pending struct {
	Capture string    `json:"capture_id"`
	Since   time.Time `json:"since"`
}

// Equipment is one piece of equipment's line of tracekeep status (operations.md §10.4).
type Equipment struct {
	EquipmentID  string     `json:"equipment_id"`
	LastRecord   *time.Time `json:"last_record,omitempty"`
	OpenCaptures int        `json:"open_captures"`
	Deferred     int        `json:"deferred_messages"`
	// Backlog is the traffic stream's messages on the equipment's subjects.
	Backlog uint64 `json:"backlog_messages"`
}

// Feed is tracekeep status's host log feed (logfeed.md §15.11): its group streams' backlog, each and together,
// and the number of equipment whose log captures registered nothing for catalog.SilentAfter among those that did within catalog.SilentWithin.
type Feed struct {
	BacklogMessages uint64       `json:"backlog_messages"`
	BacklogBytes    uint64       `json:"backlog_bytes"`
	Streams         []FeedStream `json:"streams"`
	SilentEquipment int          `json:"silent_equipment"`
}

// FeedStream is one group stream's backlog, or why it could not be read, which leaves it out of the feed's sums.
type FeedStream struct {
	Group           int    `json:"group"`
	Stream          string `json:"stream"`
	BacklogMessages uint64 `json:"backlog_messages"`
	BacklogBytes    uint64 `json:"backlog_bytes"`
	Error           string `json:"error,omitempty"`
}

// Status is tracekeep status: the pending captures by id first, then each piece of equipment, and the traffic stream's whole backlog,
// whose bytes no subject divides.
type Status struct {
	Pending         []Pending   `json:"pending"`
	Equipment       []Equipment `json:"equipment"`
	BacklogMessages uint64      `json:"backlog_messages"`
	BacklogBytes    uint64      `json:"backlog_bytes"`
	// Feed is the host log feed's, absent with it disabled.
	Feed *Feed `json:"feed,omitempty"`
}

// Status is tracekeep status.
func (a *Admin) Status(ctx context.Context) (Status, error) {
	st, err := a.Catalog.Status(ctx)
	if err != nil {
		return Status{}, err
	}
	s, err := a.Bus.Stream(ctx, bus.StreamName(a.Config, catalog.RoleTraffic))
	if err != nil {
		return Status{}, fmt.Errorf("admin: status: the traffic stream: %w", err)
	}
	prefix := a.Config.SubjectPrefix + "."
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(prefix+">"))
	if err != nil {
		return Status{}, fmt.Errorf("admin: status: the traffic stream: %w", err)
	}
	out := Status{Pending: []Pending{}, Equipment: []Equipment{}, BacklogMessages: info.State.Msgs, BacklogBytes: info.State.Bytes}
	for _, p := range st.Pending {
		out.Pending = append(out.Pending, Pending{Capture: p.Capture.String(), Since: time.Unix(0, p.SinceNS).UTC()})
	}
	for _, t := range st.Equipment {
		line := Equipment{EquipmentID: t.EquipmentID, OpenCaptures: t.OpenCaptures, Deferred: t.Deferred}
		if t.LastRecordNS != nil {
			at := time.Unix(0, *t.LastRecordNS).UTC()
			line.LastRecord = &at
		}
		// A subject is <prefix>.<equipment token>.<capture> (tracepack's BusSubject).
		if token, err := tpbus.EquipmentToken(t.EquipmentID); err == nil {
			for subject, n := range info.State.Subjects {
				if strings.HasPrefix(subject, prefix+token+".") {
					line.Backlog += n
				}
			}
		}
		out.Equipment = append(out.Equipment, line)
	}
	if a.Feed != nil {
		// A stream whose servers are down is reported as such, the others still shown (T-68).
		out.Feed = &Feed{SilentEquipment: st.SilentLogEquipment}
		for _, fs := range bus.FeedStates(ctx, a.Bus, *a.Feed) {
			line := FeedStream{Group: fs.Group, Stream: fs.Stream, BacklogMessages: fs.Messages, BacklogBytes: fs.Bytes}
			if fs.Err != nil {
				line.Error = fs.Err.Error()
			}
			out.Feed.Streams = append(out.Feed.Streams, line)
			out.Feed.BacklogMessages, out.Feed.BacklogBytes = out.Feed.BacklogMessages+fs.Messages, out.Feed.BacklogBytes+fs.Bytes
		}
	}

	return out, nil
}

// Deferred is one group of deferred list.
type Deferred struct {
	Capture     string    `json:"capture_id"`
	EquipmentID string    `json:"equipment_id,omitempty"`
	Reason      string    `json:"reason"`
	Count       int       `json:"count"`
	Since       time.Time `json:"since"`
}

// DeferredList is deferred list: what waits, why and since when, by capture and reason; of capture alone when it is not nil.
func (a *Admin) DeferredList(ctx context.Context, capture *tracepack.UUID) ([]Deferred, error) {
	groups, err := a.Catalog.Deferred(ctx, capture)
	if err != nil {
		return nil, err
	}
	out := make([]Deferred, 0, len(groups))
	for _, g := range groups {
		out = append(out, Deferred{Capture: g.Capture.String(), EquipmentID: g.EquipmentID, Reason: g.Reason, Count: g.Count, Since: time.Unix(0, g.OldestNS).UTC()})
	}

	return out, nil
}
