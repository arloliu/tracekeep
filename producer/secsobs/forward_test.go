package secsobs

import (
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
)

// A forwarded primary's outbound frame binds its registration to the socket, so the waiter's timeout records T3.
func TestForwardedTimeoutRecordsT3(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketConnected, Socket: 3, At: time.Now()})
	tok := h.p.Pending().Register(1, 7, "c")
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireOutbound, Socket: 3, Generation: 1, At: time.Now(), Frame: frame(0)})
	tok.Sent()
	tok.Awaiting(45 * time.Second)
	tok.TimedOut(45 * time.Second)

	recs := h.records(3)
	if recs[1].Kind != tracepack.KindData || recs[1].Epoch != 1 {
		t.Fatalf("frame record = %+v", recs[1])
	}
	ev := event(t, &recs[2])
	if ev.Event != tracepack.EventTimerExpiry || *ev.Timer != tracepack.TimerT3 || recs[2].Epoch != 1 ||
		*ev.PrimarySessionID != 1 || *ev.PrimaryStream != 1 || *ev.PrimaryFunction != 1 ||
		ev.PrimarySystemBytes != [4]byte{0, 0, 0, 7} || ev.Detail == nil || *ev.Detail != "effective_timeout_ms=45000" {
		t.Errorf("T3 event = %+v in epoch %d", ev, recs[2].Epoch)
	}
}

// A reconnect between the registration and the send leaves the request ambiguous: its timeout records nothing.
func TestReconnectBeforeTheSendRecordsNoT3(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketConnected, Socket: 3, At: time.Now()})
	tok := h.p.Pending().Register(1, 7, "c")
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketConnected, Socket: 4, At: time.Now()})
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireOutbound, Socket: 4, Generation: 2, At: time.Now(), Frame: frame(0)})
	tok.TimedOut(45 * time.Second)
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 4, Generation: 2, At: time.Now(), Frame: frame(5)})

	// Two connects, the frame, then the inbound marker: no T3 between them.
	recs := h.records(4)
	if recs[3].Kind != tracepack.KindControl {
		t.Errorf("record after the frame = %+v, want the inbound control frame", recs[3])
	}
}

// Only a W-bit primary sent outbound binds a registration.
func TestOnlyAnOutboundPrimaryBinds(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.p.Pending().Register(1, 7, "c")
	inbound := hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, At: time.Now(), Frame: frame(0)}
	noWait := frame(0)
	noWait[6] = 0x01 // S1F1 without the W-bit
	reply := frame(0)
	reply[7] = 2 // S1F2
	for _, ev := range []hsms.WireEvent{
		inbound,
		{Direction: hsms.WireOutbound, Socket: 3, At: time.Now(), Frame: noWait},
		{Direction: hsms.WireOutbound, Socket: 3, At: time.Now(), Frame: reply},
		{Direction: hsms.WireOutbound, Socket: 3, At: time.Now(), Frame: frame(5)},
	} {
		h.obs.wire(ev)
	}
	if s := h.obs.rec.Pending.Lookup(1, 7).Socket(); s != 0 {
		t.Errorf("bound to socket %d, want unbound", s)
	}
}

// A socket the passive side accepts and then refuses is not the connection's newest:
// a request registered after it and sent on the live socket keeps its T3.
func TestRefusedSocketKeepsTheLiveSocket(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketAccepted, Socket: 3, At: time.Now()})
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, Generation: 1, At: time.Now(), Frame: frame(1)})
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketAccepted, Socket: 4, At: time.Now()})
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 4, At: time.Now(), Frame: frame(1)})
	h.obs.socket(hsms.SocketEvent{Kind: hsms.SocketRefused, Socket: 4, At: time.Now()})
	tok := h.p.Pending().Register(1, 7, "c")
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireOutbound, Socket: 3, Generation: 1, At: time.Now(), Frame: frame(0)})

	if e, ok := tok.(interface{ Ambiguous() bool }); !ok || e.Ambiguous() {
		t.Fatalf("a request sent on the live socket is ambiguous after a refused socket")
	}
}

// go-secs v2.8.0 times an outbound frame just before its write and reports it once the write returns
// (hsms/connection_send.go, writeFrame), while the receive goroutine reports an inbound frame as soon as it is read
// (hsmsss/transport_control.go, observeInbound), so a reply read before its primary's write returns is reported first.
// The reply then takes the lower seq, and the primary, timed before it, is ordering-uncertain (producer.md §4.2):
// a lookup that seeks a primary's reply above its seq does not find it.
func TestReplyReportedBeforeItsPrimary(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	primary, reply := frame(0), frame(0) // an S1F1 W-bit primary of System Bytes 7, and its S1F2
	reply[6], reply[7] = 0x01, 2
	issued := time.Now()
	read := issued.Add(time.Millisecond)
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireInbound, Socket: 3, Generation: 1, At: read, Frame: reply})
	h.obs.wire(hsms.WireEvent{Direction: hsms.WireOutbound, Socket: 3, Generation: 1, At: issued, Frame: primary})

	recs := h.records(2)
	in, out := recs[0], recs[1]
	if in.Dir != tracepack.DirEquipmentToHost || out.Dir != tracepack.DirHostToEquipment {
		t.Fatalf("records in the order %v, %v; want the reply, then the primary", in.Dir, out.Dir)
	}
	if in.Seq != 1 || in.Quality.HasAny(tracepack.QualityOrderingUncertain) {
		t.Errorf("reply: seq %d, quality %v; want seq 1, certain", in.Seq, in.Quality)
	}
	if out.Seq != 2 || !out.Quality.HasAny(tracepack.QualityOrderingUncertain) || out.TSUTCNs >= in.TSUTCNs {
		t.Errorf("primary: seq %d, quality %v, at %d after the reply's %d; want seq 2, ordering-uncertain, timed first",
			out.Seq, out.Quality, out.TSUTCNs, in.TSUTCNs)
	}
}
