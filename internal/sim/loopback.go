// Package sim is the reference producer on the SDK, which cmd/tracekeep-sim and tracekeep dev run:
// it drives the SDK exactly as an equipment gateway does, from a go-secs HSMS-SS loopback pair with the SDK's observers.
// In loopback mode the host forwards requests at a steady rate and the equipment answers them;
// in replay mode the pair carries a synthetic or masked tracepack pack's frames, each from the side that sent it.
package sim

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/hsmsss"
	"github.com/arloliu/go-secs/v2/secs2"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/secsobs"
)

// channel names the channel the simulator forwards on, as the gateway names the source of a request.
const channel = "sim-eqp"

// openTimeout bounds the wait for the pair to select.
const openTimeout = 10 * time.Second

// Options shape the loopback pair's traffic.
type Options struct {
	// ToolID and DeviceName go into every capture's descriptor.
	ToolID, DeviceName string
	// SessionID is the HSMS-SS device id.
	SessionID uint16
	// T3 is how long the waiter waits for the reply to a forwarded request.
	T3 time.Duration
	// WithholdEvery makes the equipment leave every n-th forwarded primary unanswered, so its wait times out; 0 answers all.
	WithholdEvery uint64
	// Replay makes the equipment answer nothing itself: a replayed pack's own replies cross the pair instead.
	Replay bool
}

// Loopback is the simulator's device: a go-secs HSMS-SS pair on localhost,
// the host side the gateway's connection with the SDK's observers installed,
// the equipment side a peer that answers or withholds.
// It drives the SDK as the gateway does (producer-sdk.md §4.7, the six guarantees):
// Start, HotReload and Stop map onto Begin, Rotate and End,
// and every forwarded W-bit primary is registered before it is sent, its progress and outcome reported on its token.
type Loopback struct {
	p      *producer.Producer
	opts   Options
	host   hsmsss.Connection
	eqp    hsmsss.Connection
	detach func()
	waiter *waiter

	sb       atomic.Uint32 // the last System Bytes forwarded
	answered atomic.Uint64 // primaries the equipment has seen
	reload   atomic.Uint64 // HotReloads, which change the descriptor

	mu       sync.Mutex
	inflight map[uint32]*flight // forwarded primaries awaiting a reply, by System Bytes
	opened   bool
}

// NewLoopback builds the pair on a free localhost port for p; Start opens it.
func NewLoopback(p *producer.Producer, opts Options) (*Loopback, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	l := &Loopback{p: p, opts: opts, waiter: newWaiter(), inflight: make(map[uint32]*flight)}
	base := []hsmsss.Option{
		hsmsss.WithConnectionOption(hsms.WithSessionID(opts.SessionID)),
		hsmsss.WithConnectionOption(hsms.WithT5(100 * time.Millisecond)),
		hsmsss.WithConnectionOption(hsms.WithLinktestInterval(0)),
	}
	eqpCfg, err := hsmsss.NewConfig("127.0.0.1", port, slices.Concat(base, []hsmsss.Option{hsmsss.WithPassive(), hsmsss.WithEquipRole()})...)
	if err != nil {
		return nil, err
	}
	if l.eqp, err = hsmsss.New(eqpCfg); err != nil {
		return nil, err
	}
	l.eqp.AddDataMessageHandler(l.equipment)

	// The observers go on the host side before it opens (the third guarantee).
	obs := secsobs.New(p)
	hostOpts := slices.Concat(base, []hsmsss.Option{hsmsss.WithActive(), hsmsss.WithHostRole()})
	for _, o := range obs.ConnOptions() {
		hostOpts = append(hostOpts, hsmsss.WithConnectionOption(o))
	}
	hostCfg, err := hsmsss.NewConfig("127.0.0.1", port, hostOpts...)
	if err != nil {
		return nil, err
	}
	if l.host, err = hsmsss.New(hostCfg); err != nil {
		return nil, err
	}
	l.host.AddDataMessageHandler(l.hostReply)
	l.detach = obs.AttachLifecycle(l.host)

	return l, nil
}

// Start opens a capture, then the pair on its first call: the device's Start.
func (l *Loopback) Start(ctx context.Context) error {
	if err := l.p.Begin(ctx, l.descriptor()); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.opened {
		return nil
	}
	if err := l.eqp.Open(ctx, hsms.OpenBackground); err != nil {
		return fmt.Errorf("open equipment: %w", err)
	}
	octx, cancel := context.WithTimeout(context.Background(), openTimeout)
	defer cancel()
	if err := l.host.Open(octx, hsms.OpenWaitSelected); err != nil {
		return fmt.Errorf("open host: %w", err)
	}
	l.opened = true

	return nil
}

// HotReload changes a configured value, so the descriptor changes and the capture rotates.
func (l *Loopback) HotReload(ctx context.Context) error {
	l.reload.Add(1)
	return l.p.Rotate(ctx, l.descriptor())
}

// Stop ends every open wait, closes the pair and ends the capture: the device's Stop, which never fails.
func (l *Loopback) Stop(ctx context.Context) {
	l.waiter.cancelAll()
	_ = l.host.Close()
	_ = l.eqp.Close()
	l.p.End(ctx)
	l.detach()
}

// Traffic forwards n W-bit primaries without waiting for their replies, as the gateway relays a channel's requests.
func (l *Loopback) Traffic(ctx context.Context, n int) error {
	for range n {
		if err := l.Forward(ctx); err != nil {
			return err
		}
	}

	return nil
}

// Forward relays one S1F3 to the equipment the way the gateway does.
func (l *Loopback) Forward(ctx context.Context) error {
	sb := l.sb.Add(1)
	var sys [4]byte
	binary.BigEndian.PutUint32(sys[:], sb)
	msg, err := hsms.NewDataMessage(1, 3, true, l.opts.SessionID, sys, secs2.L(secs2.U4(sb)))
	if err != nil {
		return err
	}

	return l.relay(ctx, msg)
}

// errMalformed is a replayed frame go-secs cannot decode.
var errMalformed = errors.New("malformed frame")

// Replay sends one recorded HSMS frame through the pair from the side that sent it:
// a W-bit primary from the host goes through the send path as Forward's does, anything else is written as it is.
// A control frame is go-secs's own business and is skipped.
func (l *Loopback) Replay(ctx context.Context, dir tracepack.Dir, frame []byte) error {
	m, err := hsms.DecodeHSMSMessage(frame)
	if err != nil {
		return fmt.Errorf("%w: %w", errMalformed, err)
	}
	msg, ok := m.ToDataMessage()
	if !ok {
		return nil
	}
	msg = msg.WithSessionID(l.opts.SessionID)
	switch dir {
	case tracepack.DirHostToEquipment:
		if msg.WaitBit() && msg.Function()%2 == 1 {
			return l.relay(ctx, msg)
		}
		return l.host.ForwardDataMessage(ctx, msg)
	case tracepack.DirEquipmentToHost:
		return l.eqp.ForwardDataMessage(ctx, msg)
	case tracepack.DirUnknown, tracepack.DirLocal:
		return nil // no side sent it
	default:
		return fmt.Errorf("replayed frame: direction %v", dir)
	}
}

// relay relays a W-bit primary the way the gateway does: register, open the wait, send, report the send.
// The wait opens before the send, so a reply that comes back at once always finds it (TimeoutSource).
func (l *Loopback) relay(ctx context.Context, msg *hsms.DataMessage) error {
	sys := msg.SystemBytes()
	sb := binary.BigEndian.Uint32(sys[:])
	tok := &flight{Token: l.p.Pending().Register(l.opts.SessionID, sb, channel), l: l, sb: sb}
	l.mu.Lock()
	l.inflight[sb] = tok
	l.mu.Unlock()
	l.waiter.Await(tok, l.opts.T3)
	if err := l.host.ForwardDataMessage(ctx, msg); err != nil {
		tok.SendFailed()
		l.waiter.take(tok) // the request never left, so its wait ends with no outcome
		l.forget(sb)

		return fmt.Errorf("forward: %w", err)
	}
	tok.Sent()

	return nil
}

// flight is a forwarded request's token that leaves the in-flight table when its wait ends without a reply.
type flight struct {
	producer.Token
	l  *Loopback
	sb uint32
}

func (f *flight) TimedOut(d time.Duration) {
	f.l.forget(f.sb)
	f.Token.TimedOut(d)
}

func (f *flight) Cancelled() {
	f.l.forget(f.sb)
	f.Token.Cancelled()
}

func (l *Loopback) forget(sb uint32) {
	l.mu.Lock()
	delete(l.inflight, sb)
	l.mu.Unlock()
}

// Close releases the pair without ending the capture; Stop is the orderly end.
func (l *Loopback) Close() error {
	return errors.Join(l.host.Close(), l.eqp.Close())
}

func (l *Loopback) descriptor() producer.CaptureDescriptor {
	reload := l.reload.Load()
	return producer.CaptureDescriptor{ToolID: l.opts.ToolID, DeviceName: l.opts.DeviceName, DeviceID: &reload}
}

// equipment answers a primary with its own item, unless it withholds the answer.
func (l *Loopback) equipment(msg *hsms.DataMessage, ep hsms.SECS2Endpoint) {
	if l.opts.Replay || msg.Function()%2 == 0 || !msg.WaitBit() {
		return
	}
	if n := l.answered.Add(1); l.opts.WithholdEvery != 0 && n%l.opts.WithholdEvery == 0 {
		return
	}
	item, err := msg.Item()
	if err != nil {
		return
	}
	_ = ep.ReplyDataMessage(context.Background(), msg, item)
}

// hostReply hands a forwarded primary's reply to its waiter.
func (l *Loopback) hostReply(msg *hsms.DataMessage, _ hsms.SECS2Endpoint) {
	if msg.Function()%2 != 0 {
		return
	}
	sys := msg.SystemBytes()
	sb := binary.BigEndian.Uint32(sys[:])
	l.mu.Lock()
	tok := l.inflight[sb]
	delete(l.inflight, sb)
	l.mu.Unlock()
	if tok != nil {
		l.waiter.reply(tok)
	}
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("listener address %v is not TCP", ln.Addr())
	}

	return addr.Port, nil
}
