package admin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// Pending is a pending capture of tracekeep status.
type Pending struct {
	Capture string    `json:"capture_id"`
	Since   time.Time `json:"since"`
}

// Tool is one tool's line of tracekeep status (operations.md §10.4).
type Tool struct {
	ToolID       string     `json:"tool_id"`
	LastRecord   *time.Time `json:"last_record,omitempty"`
	OpenCaptures int        `json:"open_captures"`
	Deferred     int        `json:"deferred_messages"`
	// Backlog is the traffic stream's messages on the tool's subjects.
	Backlog uint64 `json:"backlog_messages"`
}

// Status is tracekeep status: the pending captures by id first, then each tool, and the traffic stream's whole backlog,
// whose bytes no subject divides.
type Status struct {
	Pending         []Pending `json:"pending"`
	Tools           []Tool    `json:"tools"`
	BacklogMessages uint64    `json:"backlog_messages"`
	BacklogBytes    uint64    `json:"backlog_bytes"`
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
	out := Status{Pending: []Pending{}, Tools: []Tool{}, BacklogMessages: info.State.Msgs, BacklogBytes: info.State.Bytes}
	for _, p := range st.Pending {
		out.Pending = append(out.Pending, Pending{Capture: p.Capture.String(), Since: time.Unix(0, p.SinceNS).UTC()})
	}
	for _, t := range st.Tools {
		line := Tool{ToolID: t.ToolID, OpenCaptures: t.OpenCaptures, Deferred: t.Deferred}
		if t.LastRecordNS != nil {
			at := time.Unix(0, *t.LastRecordNS).UTC()
			line.LastRecord = &at
		}
		// A subject is <prefix>.<tool token>.<capture> (tracepack's BusSubject).
		if token, err := tracepack.BusToolToken(t.ToolID); err == nil {
			for subject, n := range info.State.Subjects {
				if strings.HasPrefix(subject, prefix+token+".") {
					line.Backlog += n
				}
			}
		}
		out.Tools = append(out.Tools, line)
	}

	return out, nil
}

// Deferred is one group of deferred list.
type Deferred struct {
	Capture string    `json:"capture_id"`
	ToolID  string    `json:"tool_id,omitempty"`
	Reason  string    `json:"reason"`
	Count   int       `json:"count"`
	Since   time.Time `json:"since"`
}

// DeferredList is deferred list: what waits, why and since when, by capture and reason; of capture alone when it is not nil.
func (a *Admin) DeferredList(ctx context.Context, capture *tracepack.UUID) ([]Deferred, error) {
	groups, err := a.Catalog.Deferred(ctx, capture)
	if err != nil {
		return nil, err
	}
	out := make([]Deferred, 0, len(groups))
	for _, g := range groups {
		out = append(out, Deferred{Capture: g.Capture.String(), ToolID: g.ToolID, Reason: g.Reason, Count: g.Count, Since: time.Unix(0, g.OldestNS).UTC()})
	}

	return out, nil
}
