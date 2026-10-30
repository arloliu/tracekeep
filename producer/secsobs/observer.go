package secsobs

import (
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/internal/core"
	"github.com/arloliu/tracekeep/producer/internal/link"
)

// Offsets in a frame, after its 4-byte length prefix, of the HSMS header's fields (SEMI E37).
const (
	sessionOff = 4
	// wBitOff holds the W-bit and the stream.
	wBitOff     = 6
	functionOff = 7
	// sTypeOff is the SType byte; nonzero marks a control frame.
	sTypeOff       = 9
	systemBytesOff = 10
	frameHeaderEnd = 14
)

// causeMap is a go-secs cause as tracepack-go.md §5.1 maps it, with its cause_raw.
type causeMap struct {
	cause tracepack.Cause
	timer tracepack.Timer
	raw   string
}

// causes maps every go-secs v2.8.0 transition cause (tracepack-go.md §5.1); cause_raw is go-secs:<ConstantName>.
var causes = map[hsms.TransitionCause]causeMap{
	hsms.CauseUnknown:        {tracepack.CauseUnknown, tracepack.TimerNone, "go-secs:CauseUnknown"},
	hsms.CauseLocalOpen:      {tracepack.CauseLocalOpen, tracepack.TimerNone, "go-secs:CauseLocalOpen"},
	hsms.CauseLocalClose:     {tracepack.CauseLocalClose, tracepack.TimerNone, "go-secs:CauseLocalClose"},
	hsms.CauseSelectAccepted: {tracepack.CauseSelectAccepted, tracepack.TimerNone, "go-secs:CauseSelectAccepted"},
	hsms.CauseSelectRejected: {tracepack.CauseSelectRejected, tracepack.TimerNone, "go-secs:CauseSelectRejected"},
	hsms.CausePeerSeparate:   {tracepack.CausePeerSeparate, tracepack.TimerNone, "go-secs:CausePeerSeparate"},
	hsms.CausePeerDeselect:   {tracepack.CausePeerDeselect, tracepack.TimerNone, "go-secs:CausePeerDeselect"},
	hsms.CauseT6Timeout:      {tracepack.CauseTimerExpiry, tracepack.TimerT6, "go-secs:CauseT6Timeout"},
	hsms.CauseT7Timeout:      {tracepack.CauseTimerExpiry, tracepack.TimerT7, "go-secs:CauseT7Timeout"},
	hsms.CauseLinktestFail:   {tracepack.CauseLinktestFailure, tracepack.TimerNone, "go-secs:CauseLinktestFail"},
	hsms.CauseIOError:        {tracepack.CauseTransportError, tracepack.TimerNone, "go-secs:CauseIOError"},
	hsms.CauseHandlerExit:    {tracepack.CauseImplementationFault, tracepack.TimerNone, "go-secs:CauseHandlerExit"},
}

// unknownRaw names every cause value by go-secs's own String, built once so a callback never formats one;
// a cause the go-secs this SDK was built with does not map still keeps its source name.
var unknownRaw = func() (t [256]string) {
	for i := range t {
		t[i] = "go-secs:" + hsms.TransitionCause(i).String() //nolint:gosec // i < 256
	}

	return t
}()

// Observer holds the observers that feed one producer from one HSMS-SS connection.
// Its callbacks are safe for concurrent use and never block on anything external.
type Observer struct {
	rec *core.Recorder
	// role is the connection's socket role, learned from its connects and accepts:
	// an HSMS-SS connection either dials or listens, so every socket it reports shares it, a refused one included.
	role atomic.Uint32
}

// New returns the observers that feed p.
func New(p *producer.Producer) *Observer {
	return &Observer{rec: link.Recorder(p)}
}

// ConnOptions returns the wire, socket and transaction observers,
// which an HSMS-SS integrator passes through hsmsss.WithConnectionOption when it builds the connection.
func (o *Observer) ConnOptions() []hsms.ConnOption {
	return []hsms.ConnOption{
		hsms.WithWireObserver(o.wire),
		hsms.WithSocketObserver(o.socket),
		hsms.WithTransactionObserver(o.tx),
	}
}

// AttachLifecycle subscribes the lifecycle observer to conn's state transitions;
// call it before the connection opens, and the returned cancel after End.
func (o *Observer) AttachLifecycle(conn hsms.Connection) (cancel func()) {
	return conn.SubscribeLifecycle(o.lifecycle)
}

// lifecycle records one state transition (tracepack-go.md §5.1, §5.2), timed when it fired, in its generation's epoch.
func (o *Observer) lifecycle(ev hsms.LifecycleEvent) {
	defer o.rec.Recover()
	c := mapCause(ev.Cause)
	o.event(ev.At.UnixNano(), core.Mono(ev.At), ev.Socket, &core.Event{
		Kind: tracepack.EventStateTransition, Prev: state(ev.Previous), Cur: state(ev.Current),
		Cause: c.cause, CauseRaw: c.raw, Timer: c.timer,
	})
}

func state(s hsms.ConnState) tracepack.State {
	switch s {
	case hsms.NotConnectedState:
		return tracepack.StateNotConnected
	case hsms.NotSelectedState:
		return tracepack.StateNotSelected
	case hsms.SelectedState:
		return tracepack.StateSelected
	default:
		return tracepack.StateUnknown
	}
}

// wire records one frame (tracepack-go.md §5.5).
// The gateway is the connection's host (vantage host), so an outbound frame travels host to equipment.
func (o *Observer) wire(ev hsms.WireEvent) {
	defer o.rec.Recover()
	obs := core.Observation{
		Wall: ev.At.UnixNano(), Mono: core.Mono(ev.At), Socket: ev.Socket,
		Kind: tracepack.KindData, Fidelity: tracepack.FidelityWireExact,
		Payload: ev.Frame,
	}
	if len(ev.Frame) > sTypeOff && ev.Frame[sTypeOff] != 0 {
		obs.Kind = tracepack.KindControl
	}
	switch ev.Direction {
	case hsms.WireInbound:
		obs.Dir = tracepack.DirEquipmentToHost
		o.adopted(&ev)
		obs.Pending = o.forwarded(ev.Frame, false)
	case hsms.WireOutbound:
		o.adopted(&ev)
		obs.Dir = tracepack.DirHostToEquipment
		o.bind(ev.Frame, ev.Socket)
		obs.Pending = o.forwarded(ev.Frame, true)
	default:
	}
	o.rec.Observe(&obs)
}

// forwarded finds the forwarded request a data frame's key names, for the publisher's thinning (producer.md §4.6):
// an outbound W-bit primary, or an inbound reply; a map read, no body inspection.
func (o *Observer) forwarded(f []byte, outbound bool) *core.PendingEntry {
	if len(f) < frameHeaderEnd || f[sTypeOff] != 0 {
		return nil
	}
	if outbound {
		if f[wBitOff]&0x80 == 0 || f[functionOff]&1 == 0 {
			return nil
		}
	} else if f[functionOff] == 0 || f[functionOff]&1 != 0 {
		return nil
	}

	return o.rec.Pending.Lookup(binary.BigEndian.Uint16(f[sessionOff:]), binary.BigEndian.Uint32(f[systemBytesOff:]))
}

// adopted records the socket of a frame a generation owns as the connection's newest;
// a refused socket's frames belong to no generation.
func (o *Observer) adopted(ev *hsms.WireEvent) {
	if ev.Generation != 0 {
		o.rec.Pending.SetSocket(ev.Socket)
	}
}

// bind binds a forwarded request's registration to the socket its W-bit primary went out on,
// which its T3 needs (producer.md §4.6 *Association protocol*); any other frame binds nothing.
func (o *Observer) bind(f []byte, socket uint64) {
	if len(f) < frameHeaderEnd || f[sTypeOff] != 0 || f[wBitOff]&0x80 == 0 || f[functionOff]&1 == 0 {
		return
	}
	o.rec.Pending.Bind(
		binary.BigEndian.Uint16(f[sessionOff:]), binary.BigEndian.Uint32(f[systemBytesOff:]),
		f[wBitOff]&0x7f, f[functionOff], socket,
	)
}

// socket records one socket event (tracepack-go.md §5.3).
func (o *Observer) socket(ev hsms.SocketEvent) {
	defer o.rec.Recover()
	var e core.Event
	switch ev.Kind {
	case hsms.SocketConnected:
		e = core.Event{Kind: tracepack.EventSocketConnect, Role: tracepack.SocketRoleActive}
		o.role.Store(uint32(e.Role))
		o.rec.Pending.SetSocket(ev.Socket)
	case hsms.SocketAccepted:
		// An accepted socket may still be refused; its first frame on a generation makes it the newest (adopted).
		e = core.Event{Kind: tracepack.EventSocketAccept, Role: tracepack.SocketRolePassive}
		o.role.Store(uint32(e.Role))
	case hsms.SocketRefused:
		e = core.Event{
			Kind: tracepack.EventStateTransition, Prev: tracepack.StateNotConnected, Cur: tracepack.StateNotConnected,
			Cause: tracepack.CauseSelectRejected, CauseRaw: "go-secs:SocketRefused",
		}
	case hsms.SocketClosed:
		e = core.Event{Kind: tracepack.EventSocketClose, Role: tracepack.SocketRole(o.role.Load())} //nolint:gosec // only SocketRole values are stored
		e.Cause, e.CauseRaw = closeCause(&ev)
	default:
		return
	}
	o.event(ev.At.UnixNano(), core.Mono(ev.At), ev.Socket, &e)
}

// tx records go-secs's own T3 (tracepack-go.md §5.4); every other outcome leaves no record.
// TxEvent carries no time, so the event is timed at its reception.
func (o *Observer) tx(ev hsms.TxEvent) {
	defer o.rec.Recover()
	if ev.Outcome != hsms.TxT3Timeout {
		return
	}
	wall, mono := core.Now()
	o.event(wall, mono, ev.Socket, &core.Event{
		Kind: tracepack.EventTimerExpiry, Timer: tracepack.TimerT3,
		SessionID: ev.SessionID, Stream: ev.Stream, Function: ev.Function, SystemBytes: ev.ID,
	})
}

func (o *Observer) event(wall, mono int64, socket uint64, e *core.Event) {
	o.rec.Observe(&core.Observation{
		Wall: wall, Mono: mono, Socket: socket,
		Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal, Fidelity: tracepack.FidelityNotApplicable,
		Event: *e,
	})
}

// mapCause maps a go-secs cause by tracepack-go.md §5.1; an unmapped one is unknown, its source name kept in cause_raw.
func mapCause(cause hsms.TransitionCause) causeMap {
	if c, ok := causes[cause]; ok {
		return c
	}

	return causeMap{cause: tracepack.CauseUnknown, raw: unknownRaw[cause]}
}

// closeCause maps a socket close's reason by the table of tracepack-go.md §5.3.
func closeCause(ev *hsms.SocketEvent) (tracepack.Cause, string) {
	c := mapCause(ev.Cause)
	if ev.Cause != hsms.CauseIOError {
		return c.cause, c.raw
	}
	// §5.3 takes io.ErrUnexpectedEOF as a peer close only on a refused socket.
	// Generation 0 marks a socket never adopted, and of those only a refused one closes with an I/O error:
	// a declined socket closes with no error and a dial completed after Close with CauseLocalClose
	// (go-secs v2.8.0 hsms/socket_observer.go, SocketEvent's Generation, Err and Cause).
	if errors.Is(ev.Err, io.EOF) || (ev.Generation == 0 && errors.Is(ev.Err, io.ErrUnexpectedEOF)) {
		return tracepack.CausePeerClose, c.raw
	}

	return tracepack.CauseTransportError, c.raw
}
