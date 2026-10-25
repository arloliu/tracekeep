package producer

import (
	"time"

	"github.com/arloliu/tracekeep/producer/internal/core"
)

// Pending is the producer's send path (docs/specs/tracekeep/producer.md §4.6 *Send-path token protocol*, producer-sdk.md §4.7 *Send path*):
// every forwarded W-bit primary is registered before it is sent, and its token reports what became of it.
// A forwarded request's T3 comes only from its token's TimedOut.
type Pending struct{ t *core.PendingTable }

// Token is one registration's handle: safe from any goroutine, never blocking on anything external.
// Sent, SendFailed and SubmissionFailed report the send's progress and exclude each other;
// Replied, TimedOut and Cancelled are the waiter's terminal outcomes, and the first seals the request.
// Every report the protocol does not take is a counted no-op.
type Token interface {
	// Sent reports that the forwarding call returned without error.
	Sent()
	// SendFailed reports that the forwarding call returned an error.
	SendFailed()
	// SubmissionFailed reports that the request never reached the forwarding call.
	SubmissionFailed()
	// Awaiting reports that the waiter opened the wait with timeout; the request is kept until the wait ends plus the reporting grace.
	Awaiting(timeout time.Duration)
	// Replied reports a reply.
	Replied()
	// TimedOut reports the waiter's timeout, from its timer branch only, with the timeout the wait was opened with.
	TimedOut(effective time.Duration)
	// Cancelled reports a wait that ended any other way, such as the device stopping.
	Cancelled()
}

var _ Token = (*core.PendingEntry)(nil)

// Pending returns the producer's send path.
func (p *Producer) Pending() *Pending { return &Pending{t: p.rec.Pending} }

// Register records a forwarded W-bit primary before it is sent and returns its token;
// call it for every such primary, on a poll channel or not.
//
// Parameters:
//   - sessionID: the primary's session id
//   - systemBytes: the primary's System Bytes
//   - channel: the channel that sends it
//
// Returns:
//   - Token: the handle the forwarding call and the waiter report on
func (p *Pending) Register(sessionID uint16, systemBytes uint32, channel string) Token {
	return p.t.Register(sessionID, systemBytes, channel)
}
