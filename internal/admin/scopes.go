package admin

import (
	"context"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// Acknowledgement is what conflict acknowledge did: its action id and how many conflict rows it acknowledged.
type Acknowledgement struct {
	ActionID     string
	Acknowledged int
}

// ConflictAcknowledge is conflict acknowledge: every open conflict of capture covering seq acknowledged, which closes the incident;
// the scope stays unmerged for its lifetime.
func (a *Admin) ConflictAcknowledge(ctx context.Context, capture tracepack.UUID, seq uint64) (Acknowledgement, error) {
	action := actionID()
	n, err := a.Catalog.AcknowledgeConflict(ctx, capture, seq, a.Actor, action)

	return Acknowledgement{ActionID: action, Acknowledged: n}, err
}

// Exclusion is what scope exclude-input did: its action id and the scope whose input the pack was.
type Exclusion struct {
	ActionID string
	Capture  tracepack.UUID
	Hour     int64
}

// ScopeExcludeInput is scope exclude-input: the staged pack excluded from its scope's merges; it stays readable.
func (a *Admin) ScopeExcludeInput(ctx context.Context, pack tracepack.UUID) (Exclusion, error) {
	action := actionID()
	s, err := a.Catalog.ExcludeInput(ctx, pack, a.Actor, action)

	return Exclusion{ActionID: action, Capture: s.Capture, Hour: s.Hour}, err
}

// Requested is a request the compactor executes at its next cycle.
type Requested struct {
	RequestID tracepack.UUID
}

// ScopeMerge is scope merge: a request that the compactor merge the scope of capture's hour at its next cycle,
// its quiet period aside; the request's state shows how it ended.
func (a *Admin) ScopeMerge(ctx context.Context, capture tracepack.UUID, hour int64) (Requested, error) {
	id, err := a.Catalog.RequestCompaction(ctx, catalog.RequestMerge, capture, &hour, a.Actor)

	return Requested{RequestID: id}, err
}

// CaptureClose is capture close: a request that the compactor close the capture at its next cycle instead of waiting for I;
// the request's state shows how it ended.
func (a *Admin) CaptureClose(ctx context.Context, capture tracepack.UUID) (Requested, error) {
	id, err := a.Catalog.RequestCompaction(ctx, catalog.RequestClose, capture, nil, a.Actor)

	return Requested{RequestID: id}, err
}
