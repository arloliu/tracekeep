// Package admin is the operator's side of parking, termination and the compactor's scopes (docs/specs/tracekeep/operations.md §10.4, parking.md):
// the operations the admin commands run, each audited.
// They write the catalog and hold no message: every acknowledgement they decide happens at the message's next delivery,
// and every merge they ask for is the compactor's, under its lease.
package admin

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// Admin runs the operator's operations against the catalog and the bus.
type Admin struct {
	Catalog *catalog.Catalog
	Bus     jetstream.JetStream
	// Config names the bus's streams.
	Config bus.Config
	// Actor names the operator in every audit row.
	Actor string
}

// Termination is what a termination did: its action id, which its audit row, its versions and its order name,
// and how many unresolved versions it terminated.
type Termination struct {
	ActionID   string
	Terminated int
}

// ParkedList is parked list: the parked versions of capture, or of every capture when capture is nil, each with its messages.
func (a *Admin) ParkedList(ctx context.Context, capture *tracepack.UUID) ([]catalog.ParkedVersion, error) {
	return a.Catalog.ParkedVersions(ctx, capture)
}

// ParkedTerminate is parked terminate: the unresolved versions of capture, or of its seq when seq is not nil, terminated as declared losses.
func (a *Admin) ParkedTerminate(ctx context.Context, capture tracepack.UUID, seq *uint64) (Termination, error) {
	action := actionID()
	n, err := a.Catalog.TerminateParked(ctx, capture, seq, a.Actor, action)

	return Termination{ActionID: action, Terminated: n}, err
}

// CaptureTerminate is capture terminate: every unresolved version of capture terminated,
// and a termination order covering its messages on the traffic stream's incarnation up to that stream's last sequence,
// read by a fresh Info before the transaction, so no network call happens under the capture row
// and a publish accepted after the read carries a sequence above the cut-off.
func (a *Admin) CaptureTerminate(ctx context.Context, capture tracepack.UUID) (Termination, error) {
	s, err := a.Bus.Stream(ctx, bus.StreamName(a.Config, catalog.RoleTraffic))
	if err != nil {
		return Termination{}, fmt.Errorf("admin: traffic stream: %w", err)
	}
	inc, err := bus.Incarnate(ctx, a.Catalog, catalog.RoleTraffic, s)
	if err != nil {
		return Termination{}, err
	}
	info, err := s.Info(ctx)
	if err != nil {
		return Termination{}, fmt.Errorf("admin: traffic stream: %w", err)
	}
	action := actionID()
	n, err := a.Catalog.TerminateCapture(ctx, capture, inc.ID, int64(info.State.LastSeq), a.Actor, action) //nolint:gosec // a stream sequence fits

	return Termination{ActionID: action, Terminated: n}, err
}

// Show is capture show: the capture with its parked versions, their messages, and its termination orders.
type Show struct {
	Capture  catalog.Capture
	Versions []catalog.ParkedVersion
	Orders   []catalog.TerminationOrder
}

// CaptureShow reads capture's row, versions and orders.
func (a *Admin) CaptureShow(ctx context.Context, capture tracepack.UUID) (Show, error) {
	var sh Show
	var err error
	if sh.Capture, err = a.Catalog.Capture(ctx, capture); err != nil {
		return Show{}, err
	}
	if sh.Versions, err = a.Catalog.ParkedVersions(ctx, &capture); err != nil {
		return Show{}, err
	}
	if sh.Orders, err = a.Catalog.TerminationOrders(ctx, capture); err != nil {
		return Show{}, err
	}

	return sh, nil
}

// AdoptStream is bus adopt-stream role, the last step of a stop-the-world replacement of the stream (parking.md *incarnations*):
// refused while any lease is live, it records the stream as a new incarnation, the retired one's waiting copies lost and its orders spent,
// then deletes every consumer of the service the stream still has and verifies that none remains.
// A failure after the adoption is resolved by running the command again, which adopts the stream once more.
func (a *Admin) AdoptStream(ctx context.Context, role string) (catalog.Incarnation, error) {
	s, err := a.Bus.Stream(ctx, bus.StreamName(a.Config, role))
	if err != nil {
		return catalog.Incarnation{}, fmt.Errorf("admin: %s stream: %w", role, err)
	}
	info := s.CachedInfo()
	inc, err := a.Catalog.AdoptStream(ctx, role, info.Config.Name, info.Created.UnixNano(), a.Actor)
	if err != nil {
		return catalog.Incarnation{}, err
	}

	return inc, bus.RetireConsumers(ctx, s)
}

// actionID names one operator action.
func actionID() string { return rand.Text() }
