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
// a reply can arrive before the forwarding call returns, so Sent may follow the terminal outcome;
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

// TimeoutSource is the response waiter's side of the send path, which the gateway's waiter implements.
// The wait must be open before its reply can arrive: the gateway calls Await before it sends the request,
// or the waiter holds a reply that comes before its wait.
// A reply dropped for want of a wait ends in a timeout, a T3 the SDK cannot tell from a real one.
// A TimeoutSource never blocks its caller,
// reports exactly one terminal outcome per Await,
// and calls TimedOut only from its timer branch, with the timeout the wait was opened with,
// so a HotReload's new timeout applies to new waits only (producer.md §4.4).
// producertest.CheckTimeoutSource checks an implementation against these rules.
type TimeoutSource interface {
	// Await opens the wait for a registered request, reports Awaiting on t as it opens it,
	// and then reports exactly one terminal outcome on t.
	Await(t Token, timeout time.Duration)
}

var _ Token = (*core.PendingEntry)(nil)

// Pending returns the producer's send path.
func (p *Producer) Pending() *Pending { return &Pending{t: p.rec.Pending} }

// Origin is what the gateway knows of why it sends a request (producer.md §4.6, T-42).
type Origin uint8

const (
	// OriginUnmarked is the zero value: the request is never thinned.
	OriginUnmarked Origin = iota
	// OriginIntervalPoll marks a request every context of which is interval polling;
	// a marked S1F3 may be thinned, with its reply, under the capture's sampling rule.
	OriginIntervalPoll
)

// Request is a forwarded W-bit primary as the gateway is about to send it.
type Request struct {
	// SessionID and SystemBytes are the primary's key.
	SessionID   uint16
	SystemBytes uint32
	// Stream and Function are the primary's; the frame the wire observer sees go out under the key must name the same.
	Stream, Function uint8
	// Channel is the channel that sends it.
	Channel string
	// Origin is the request's origin; its zero value is unmarked.
	Origin Origin
}

// RegisterRequest records a forwarded W-bit primary before it is sent and returns its token;
// call it, or Register, for every such primary, marked or not.
// A marked S1F3 takes its thin decision here, from the sampling rule of the capture current now.
//
// Parameters:
//   - r: the request
//
// Returns:
//   - Token: the handle the forwarding call and the waiter report on
func (p *Pending) RegisterRequest(r Request) Token {
	return p.t.RegisterRequest(core.Request{
		SessionID: r.SessionID, SystemBytes: r.SystemBytes, Stream: r.Stream, Function: r.Function, Named: true,
		Channel: r.Channel, Marked: r.Origin == OriginIntervalPoll,
	})
}

// Register records an unmarked forwarded W-bit primary that names no stream and function; it never thins.
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
