package secsobs

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/internal/core"
	"github.com/arloliu/tracekeep/producer/producertest"
)

type harness struct {
	t   *testing.T
	p   *producer.Producer
	obs *Observer
	bus *producertest.Bus
}

// newHarness opens a capture on a producer over the in-memory bus; its start is the first message.
func newHarness(t *testing.T) *harness {
	t.Helper()
	bus := producertest.NewBus()
	p, err := producer.New(producer.Config{SubjectPrefix: "tpk", ClaimBucket: "tpk-objects"}, producer.WithBus(bus))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}

	return &harness{t: t, p: p, obs: New(p), bus: bus}
}

// records waits for n records after the start and decodes them as a consumer does.
func (h *harness) records(n int) []tracepack.Record {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), 10*time.Second)
	defer cancel()
	msgs, err := h.bus.WaitMessages(ctx, n+1)
	if err != nil {
		h.t.Fatalf("%v; have %d", err, len(msgs))
	}
	recs := make([]tracepack.Record, 0, n)
	for i := range msgs[1:] {
		m := &msgs[i+1]
		br, err := tracepack.CheckBusMessage(tracepack.BusEnvelope{Layout: m.Header[tracepack.BusHeaderLayout], MsgID: m.MsgID}, m.Data)
		if err != nil {
			h.t.Fatalf("CheckBusMessage(%s): %v", m.MsgID, err)
		}
		recs = append(recs, br.Record)
	}

	return recs
}

func event(t *testing.T, r *tracepack.Record) tracepack.TransportEvent {
	t.Helper()
	if r.Kind != tracepack.KindTransportEvent || r.Dir != tracepack.DirLocal || r.Fidelity != tracepack.FidelityNotApplicable {
		t.Fatalf("record = %+v, want a transport event", r)
	}
	ev, err := tracepack.UnmarshalTransportEvent(r.Payload)
	if err != nil {
		t.Fatalf("UnmarshalTransportEvent: %v", err)
	}

	return *ev
}

// frame builds an HSMS frame: the length prefix, a header with the given SType, and a body.
func frame(sType byte, body ...byte) []byte {
	f := make([]byte, 0, 14+len(body))
	f = append(f, 0, 0, 0, byte(10+len(body)), 0, 1, 0x81, 1, 0, sType, 0, 0, 0, 7)

	return append(f, body...)
}

func TestWireFrames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		dir   hsms.WireDirection
		frame []byte
		kind  tracepack.Kind
		rdir  tracepack.Dir
	}{
		{"inbound data", hsms.WireInbound, frame(0, 0x01, 0x00), tracepack.KindData, tracepack.DirEquipmentToHost},
		{"outbound control", hsms.WireOutbound, frame(5), tracepack.KindControl, tracepack.DirHostToEquipment},
		{"too short for an SType", hsms.WireInbound, []byte{0, 0, 0, 10, 0, 1}, tracepack.KindData, tracepack.DirEquipmentToHost},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			at := time.Now()
			h.obs.wire(hsms.WireEvent{Direction: tt.dir, Socket: 3, Generation: 1, At: at, Frame: tt.frame})
			r := h.records(1)[0]
			if r.Kind != tt.kind || r.Dir != tt.rdir || r.Fidelity != tracepack.FidelityWireExact ||
				r.Epoch != 1 || r.TSUTCNs != at.UnixNano() || !reflect.DeepEqual(r.Payload, tt.frame) {
				t.Errorf("record = %+v", r)
			}
		})
	}
}

func TestWireCopiesTheBorrowedFrame(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	f := frame(0, 0x01, 0x00)
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, At: time.Now(), Frame: f})
	f[14] = 0xff // go-secs reuses the buffer once the observer returns

	if r := h.records(1)[0]; r.Payload[14] != 0x01 {
		t.Errorf("payload aliases the borrowed frame: % x", r.Payload)
	}
}

func TestSocketEvents(t *testing.T) {
	t.Parallel()
	closed := func(gen uint64, cause hsms.TransitionCause, err error) hsms.SocketEvent {
		return hsms.SocketEvent{Kind: hsms.SocketClosed, Socket: 3, Generation: gen, Cause: cause, Err: err}
	}
	closeEvent := func(c tracepack.Cause, raw string) tracepack.TransportEvent {
		return tracepack.TransportEvent{Event: tracepack.EventSocketClose, Cause: new(c), CauseRaw: new(raw)}
	}
	tests := []struct {
		name string
		ev   hsms.SocketEvent
		want tracepack.TransportEvent
	}{
		{
			"connected", hsms.SocketEvent{Kind: hsms.SocketConnected, Socket: 3},
			tracepack.TransportEvent{Event: tracepack.EventSocketConnect, SocketRole: new(tracepack.SocketRoleActive)},
		},
		{
			"accepted", hsms.SocketEvent{Kind: hsms.SocketAccepted, Socket: 3},
			tracepack.TransportEvent{Event: tracepack.EventSocketAccept, SocketRole: new(tracepack.SocketRolePassive)},
		},
		{
			"refused", hsms.SocketEvent{Kind: hsms.SocketRefused, Socket: 3},
			tracepack.TransportEvent{
				Event: tracepack.EventStateTransition, PrevState: new(tracepack.StateNotConnected), CurState: new(tracepack.StateNotConnected),
				Cause: new(tracepack.CauseSelectRejected), CauseRaw: new("go-secs:SocketRefused"),
			},
		},
		{"peer closed", closed(1, hsms.CauseIOError, io.EOF), closeEvent(tracepack.CausePeerClose, "go-secs:CauseIOError")},
		{
			"refused peer closed mid-select", closed(0, hsms.CauseIOError, io.ErrUnexpectedEOF),
			closeEvent(tracepack.CausePeerClose, "go-secs:CauseIOError"),
		},
		{
			"refusal cut short", closed(0, hsms.CauseIOError, errors.New("i/o timeout")),
			closeEvent(tracepack.CauseTransportError, "go-secs:CauseIOError"),
		},
		{
			"adopted socket unexpected EOF", closed(1, hsms.CauseIOError, io.ErrUnexpectedEOF),
			closeEvent(tracepack.CauseTransportError, "go-secs:CauseIOError"),
		},
		{
			"write error", closed(1, hsms.CauseIOError, errors.New("broken pipe")),
			closeEvent(tracepack.CauseTransportError, "go-secs:CauseIOError"),
		},
		{"local close", closed(1, hsms.CauseLocalClose, nil), closeEvent(tracepack.CauseLocalClose, "go-secs:CauseLocalClose")},
		{"declined", closed(0, hsms.CauseUnknown, nil), closeEvent(tracepack.CauseUnknown, "go-secs:CauseUnknown")},
		{"T7", closed(1, hsms.CauseT7Timeout, hsms.ErrT7Timeout), closeEvent(tracepack.CauseTimerExpiry, "go-secs:CauseT7Timeout")},
		{"peer separate", closed(1, hsms.CausePeerSeparate, nil), closeEvent(tracepack.CausePeerSeparate, "go-secs:CausePeerSeparate")},
		{"unknown cause", closed(1, 201, nil), closeEvent(tracepack.CauseUnknown, "go-secs:TransitionCause(201)")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			tt.ev.At = time.Now()
			h.obs.socket(tt.ev)
			r := h.records(1)[0]
			if r.Epoch != 1 || r.TSUTCNs != tt.ev.At.UnixNano() {
				t.Errorf("record = %+v", r)
			}
			if got := event(t, &r); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("event = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSocketCloseCarriesTheConnectionRole(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		up   hsms.SocketEventKind
		role tracepack.SocketRole
	}{
		{"dialed", hsms.SocketConnected, tracepack.SocketRoleActive},
		{"accepted", hsms.SocketAccepted, tracepack.SocketRolePassive},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.obs.socket(hsms.SocketEvent{Kind: tt.up, Socket: 3, At: time.Now()})
			h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketClosed, Socket: 3, Generation: 1, At: time.Now(), Cause: hsms.CauseLocalClose})

			r := h.records(2)[1]
			if ev := event(t, &r); ev.Event != tracepack.EventSocketClose || ev.SocketRole == nil || *ev.SocketRole != tt.role {
				t.Errorf("close = %+v, want socket_role %v", ev, tt.role)
			}
		})
	}
}

func TestRefusedSocket(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketAccepted, Socket: 4, At: time.Now()})
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketRefused, Socket: 4, At: time.Now()})
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketClosed, Socket: 4, At: time.Now(), Cause: hsms.CauseIOError, Err: io.ErrUnexpectedEOF})

	recs := h.records(3)
	want := []tracepack.TransportEvent{
		{Event: tracepack.EventSocketAccept, SocketRole: new(tracepack.SocketRolePassive)},
		{
			Event: tracepack.EventStateTransition, PrevState: new(tracepack.StateNotConnected), CurState: new(tracepack.StateNotConnected),
			Cause: new(tracepack.CauseSelectRejected), CauseRaw: new("go-secs:SocketRefused"),
		},
		{
			Event: tracepack.EventSocketClose, SocketRole: new(tracepack.SocketRolePassive),
			Cause: new(tracepack.CausePeerClose), CauseRaw: new("go-secs:CauseIOError"),
		},
	}
	for i := range recs {
		if recs[i].Epoch != 1 {
			t.Errorf("record %d epoch %d, want 1: the refused socket's own", i, recs[i].Epoch)
		}
		if got := event(t, &recs[i]); !reflect.DeepEqual(got, want[i]) {
			t.Errorf("event %d = %+v, want %+v", i, got, want[i])
		}
	}
}

func TestTransactionRecordsOnlyT3(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	for _, o := range []hsms.TxOutcome{hsms.TxReplied, hsms.TxSent, hsms.TxRejected, hsms.TxCanceled, hsms.TxSendError, hsms.TxAborted, hsms.TxPeerError} {
		h.obs.tx(hsms.TxEvent{Stream: 1, Function: 1, ID: 1, ReplyWaited: true, Outcome: o, Socket: 3, Generation: 1})
	}
	beforeNs := time.Now().UnixNano()
	h.obs.tx(hsms.TxEvent{
		Stream: 1, Function: 13, ID: 0x0a0b0c0d, ReplyWaited: true, Outcome: hsms.TxT3Timeout,
		SessionID: 0x0102, Socket: 3, Generation: 1,
	})

	r := h.records(1)[0]
	if r.Seq != 1 || r.Epoch != 1 || r.TSUTCNs < beforeNs {
		t.Errorf("record = %+v, want the T3 first, at its reception", r)
	}
	want := tracepack.TransportEvent{
		Event: tracepack.EventTimerExpiry, Timer: new(tracepack.TimerT3),
		PrimarySessionID: new(uint64(0x0102)), PrimaryStream: new(uint64(1)), PrimaryFunction: new(uint64(13)),
		PrimarySystemBytes: [4]byte{0x0a, 0x0b, 0x0c, 0x0d}, PrimarySystemBytesPresent: true,
	}
	if got := event(t, &r); !reflect.DeepEqual(got, want) {
		t.Errorf("event = %+v, want %+v", got, want)
	}
}

func TestNoSocketIsEpochZero(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.obs.tx(hsms.TxEvent{Stream: 1, Function: 1, Outcome: hsms.TxT3Timeout})

	if r := h.records(1)[0]; r.Epoch != 0 || !r.Quality.Has(tracepack.QualityCorrelationIncomplete) {
		t.Errorf("record = %+v, want epoch 0 and correlation-incomplete", r)
	}
}

// TestCallbacksAllocateOnlyTheFrameCopy runs the observers against a recorder no publisher drains,
// so the count holds the callbacks' allocations alone.
func TestCallbacksAllocateOnlyTheFrameCopy(t *testing.T) {
	r := core.NewRecorder(core.RecorderParams{ReservedCap: 8, OrdinaryRecords: 8, OrdinaryBytes: 1 << 20})
	var gen atomic.Uint64
	wall, mono := core.Now()
	c := core.NewCapture(core.CaptureParams{
		ID: tracepack.UUID{1}, Lifecycle: &gen, Tolerance: int64(time.Second), ReservedCap: 8, OriginWall: wall, OriginMono: mono,
	})
	if res := r.Rotate(c, time.Now().Add(time.Second), func() bool { return false }, core.Now); !res.Installed {
		t.Fatalf("Rotate: %+v", res)
	}
	<-r.Reserved()
	obs := &Observer{rec: r}
	f := frame(0, 0x01, 0x00)
	// frame's header is an S1F1 W-bit primary of session 1 and System Bytes 7: registered, its outbound frame finds its entry.
	r.Pending.Register(1, 7, "c")

	tests := []struct {
		name   string
		call   func()
		allocs float64
	}{
		{"wire", func() { obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, At: time.Now(), Frame: f}) }, 1},
		{"wire attaching a forwarded request", func() {
			obs.wire(hsms.WireEvent{Direction: hsms.WireOutbound, Socket: 3, At: time.Now(), Frame: f})
		}, 1},
		{"socket", func() {
			obs.socket(hsms.SocketEvent{Kind: hsms.SocketClosed, Socket: 3, At: time.Now(), Cause: hsms.CauseIOError, Err: io.EOF})
		}, 0},
		{"tx", func() { obs.tx(hsms.TxEvent{Outcome: hsms.TxT3Timeout, Socket: 3}) }, 0},
		{"lifecycle", func() {
			obs.lifecycle(hsms.LifecycleEvent{Previous: hsms.SelectedState, Current: hsms.NotConnectedState, Cause: 200, Socket: 3, At: time.Now()})
		}, 0},
	}
	for _, tt := range tests {
		got := testing.AllocsPerRun(1000, func() {
			tt.call()
			e := <-r.Ordinary()
			r.ReleaseOrdinary(&e)
		})
		if got != tt.allocs {
			t.Errorf("%s allocates %v times per call, want %v", tt.name, got, tt.allocs)
		}
	}
}

func TestLifecycleEvents(t *testing.T) {
	t.Parallel()
	transition := func(prev, cur tracepack.State, c tracepack.Cause, raw string) tracepack.TransportEvent {
		return tracepack.TransportEvent{
			Event: tracepack.EventStateTransition, PrevState: new(prev), CurState: new(cur), Cause: new(c), CauseRaw: new(raw),
		}
	}
	timed := func(ev tracepack.TransportEvent, tm tracepack.Timer) tracepack.TransportEvent {
		ev.Timer = new(tm)
		return ev
	}
	tests := []struct {
		name string
		ev   hsms.LifecycleEvent
		want tracepack.TransportEvent
	}{
		{
			"selected", hsms.LifecycleEvent{Previous: hsms.NotSelectedState, Current: hsms.SelectedState, Cause: hsms.CauseSelectAccepted},
			transition(tracepack.StateNotSelected, tracepack.StateSelected, tracepack.CauseSelectAccepted, "go-secs:CauseSelectAccepted"),
		},
		{
			"T6", hsms.LifecycleEvent{Previous: hsms.NotSelectedState, Current: hsms.NotConnectedState, Cause: hsms.CauseT6Timeout},
			timed(transition(tracepack.StateNotSelected, tracepack.StateNotConnected, tracepack.CauseTimerExpiry, "go-secs:CauseT6Timeout"), tracepack.TimerT6),
		},
		{
			"linktest", hsms.LifecycleEvent{Previous: hsms.SelectedState, Current: hsms.NotConnectedState, Cause: hsms.CauseLinktestFail},
			transition(tracepack.StateSelected, tracepack.StateNotConnected, tracepack.CauseLinktestFailure, "go-secs:CauseLinktestFail"),
		},
		{
			"handler exit", hsms.LifecycleEvent{Previous: hsms.SelectedState, Current: hsms.NotConnectedState, Cause: hsms.CauseHandlerExit},
			transition(tracepack.StateSelected, tracepack.StateNotConnected, tracepack.CauseImplementationFault, "go-secs:CauseHandlerExit"),
		},
		{
			"unknown cause", hsms.LifecycleEvent{Previous: hsms.SelectedState, Current: hsms.NotConnectedState, Cause: 200},
			tracepack.TransportEvent{
				Event: tracepack.EventStateTransition, PrevState: new(tracepack.StateSelected), CurState: new(tracepack.StateNotConnected),
				Cause: new(tracepack.CauseUnknown), CauseRaw: new("go-secs:TransitionCause(200)"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			tt.ev.Socket, tt.ev.Generation, tt.ev.At = 3, 1, time.Now()
			h.obs.lifecycle(tt.ev)
			r := h.records(1)[0]
			if r.Epoch != 1 || r.TSUTCNs != tt.ev.At.UnixNano() {
				t.Errorf("record = %+v", r)
			}
			if got := event(t, &r); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("event = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// lifecycleConn is the one method of hsms.Connection that AttachLifecycle calls.
type lifecycleConn struct {
	hsms.Connection
	fn        func(hsms.LifecycleEvent)
	cancelled bool
}

func (c *lifecycleConn) SubscribeLifecycle(fn func(hsms.LifecycleEvent)) func() {
	c.fn = fn
	return func() { c.cancelled = true }
}

func TestAttachLifecycleSubscribes(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	conn := &lifecycleConn{}
	cancel := h.obs.AttachLifecycle(conn)
	conn.fn(hsms.LifecycleEvent{Previous: hsms.NotConnectedState, Current: hsms.NotSelectedState, Cause: hsms.CauseLocalOpen, Socket: 3, At: time.Now()})

	want := tracepack.TransportEvent{
		Event: tracepack.EventStateTransition, PrevState: new(tracepack.StateNotConnected), CurState: new(tracepack.StateNotSelected),
		Cause: new(tracepack.CauseLocalOpen), CauseRaw: new("go-secs:CauseLocalOpen"),
	}
	r := h.records(1)[0]
	if got := event(t, &r); !reflect.DeepEqual(got, want) {
		t.Errorf("event = %+v, want %+v", got, want)
	}
	cancel()
	if !conn.cancelled {
		t.Errorf("cancel did not cancel the subscription")
	}
}

// panicError panics when errors.Is inspects it, as a broken custom transport's error might.
type panicError struct{}

func (panicError) Error() string { return "broken" }
func (panicError) Is(error) bool { panic("broken Is") }

func TestPanicBeforeObserveFaultsTheCapture(t *testing.T) {
	t.Parallel()
	bus := producertest.NewBus()
	p, err := producer.New(producer.Config{SubjectPrefix: "tpk", ClaimBucket: "tpk-objects"}, producer.WithBus(bus))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Begin(t.Context(), producer.CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	obs := New(p)
	c := obs.rec.Current()

	obs.socket(hsms.SocketEvent{Kind: hsms.SocketClosed, Socket: 3, At: time.Now(), Cause: hsms.CauseIOError, Err: panicError{}})

	if !c.Faulted() {
		t.Errorf("capture not faulted by a panic before Observe")
	}
	if got := obs.rec.Counters().Panics; got != 1 {
		t.Errorf("panics = %d, want 1", got)
	}
}

// TestOneObserverAcrossSockets feeds one observer two sockets in turn, with events of the first delivered after the second is up.
func TestOneObserverAcrossSockets(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	now := time.Now()
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketConnected, Socket: 5, At: now})
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketClosed, Socket: 5, Generation: 1, At: now, Cause: hsms.CauseIOError, Err: io.EOF})
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketConnected, Socket: 6, At: now})
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 6, Generation: 2, At: now, Frame: frame(1)})
	// Late reports of generation 1: its drop and a send bound to it that timed out.
	h.obs.lifecycle(hsms.LifecycleEvent{
		Previous: hsms.SelectedState, Current: hsms.NotConnectedState, Cause: hsms.CauseIOError, Socket: 5, Generation: 1, At: now,
	})
	h.obs.tx(hsms.TxEvent{Stream: 1, Function: 3, ID: 9, Outcome: hsms.TxT3Timeout, Socket: 5, Generation: 1})

	recs := h.records(6)
	for i, want := range []uint32{1, 1, 2, 2, 1, 1} {
		if recs[i].Epoch != want {
			t.Errorf("record %d epoch %d, want %d", i, recs[i].Epoch, want)
		}
	}
	if ev := event(t, &recs[4]); ev.Event != tracepack.EventStateTransition || recs[4].TSUTCNs != now.UnixNano() {
		t.Errorf("late transition = %+v at %d, want its own time %d", ev, recs[4].TSUTCNs, now.UnixNano())
	}
}

// TestConcurrentCallbacks runs the wire observer for both directions and the socket observer at once on one observer,
// as go-secs's receive and write goroutines do; every record keeps its seq.
func TestConcurrentCallbacks(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	const per = 50
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, call := range []func(){
		func() {
			h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, At: time.Now(), Frame: frame(0, 1)})
		},
		func() {
			h.obs.wire(hsms.WireEvent{Direction: hsms.WireOutbound, Socket: 3, At: time.Now(), Frame: frame(0, 2)})
		},
		func() { h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketConnected, Socket: 3, At: time.Now()}) },
	} {
		wg.Go(func() {
			<-start
			for range per {
				call()
			}
		})
	}
	close(start)
	wg.Wait()

	recs := h.records(3 * per)
	seen := make(map[uint64]bool, len(recs))
	for i := range recs {
		seen[recs[i].Seq] = true
		if recs[i].Epoch != 1 {
			t.Errorf("seq %d epoch %d, want 1", recs[i].Seq, recs[i].Epoch)
		}
	}
	for seq := uint64(1); seq <= 3*per; seq++ {
		if !seen[seq] {
			t.Errorf("seq %d missing", seq)
		}
	}
}
