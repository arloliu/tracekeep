package producer_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

func TestObservedEventsEncode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ev   core.Event
		want tracepack.TransportEvent
	}{
		{
			name: "socket connect",
			ev:   core.Event{Kind: tracepack.EventSocketConnect, Role: tracepack.SocketRoleActive},
			want: tracepack.TransportEvent{Event: tracepack.EventSocketConnect, SocketRole: new(tracepack.SocketRoleActive)},
		},
		{
			name: "socket accept",
			ev:   core.Event{Kind: tracepack.EventSocketAccept, Role: tracepack.SocketRolePassive},
			want: tracepack.TransportEvent{Event: tracepack.EventSocketAccept, SocketRole: new(tracepack.SocketRolePassive)},
		},
		{
			name: "socket close",
			ev: core.Event{
				Kind: tracepack.EventSocketClose, Role: tracepack.SocketRoleActive, Cause: tracepack.CausePeerClose, CauseRaw: "go-secs:CauseIOError",
			},
			want: tracepack.TransportEvent{
				Event: tracepack.EventSocketClose, SocketRole: new(tracepack.SocketRoleActive),
				Cause: new(tracepack.CausePeerClose), CauseRaw: new("go-secs:CauseIOError"),
			},
		},
		{
			name: "socket close of no known role",
			ev:   core.Event{Kind: tracepack.EventSocketClose, Cause: tracepack.CauseLocalClose, CauseRaw: "go-secs:CauseLocalClose"},
			want: tracepack.TransportEvent{
				Event: tracepack.EventSocketClose, Cause: new(tracepack.CauseLocalClose), CauseRaw: new("go-secs:CauseLocalClose"),
			},
		},
		{
			name: "state transition by a timer",
			ev: core.Event{
				Kind: tracepack.EventStateTransition, Prev: tracepack.StateNotSelected, Cur: tracepack.StateNotConnected,
				Cause: tracepack.CauseTimerExpiry, CauseRaw: "go-secs:CauseT7Timeout", Timer: tracepack.TimerT7,
			},
			want: tracepack.TransportEvent{
				Event: tracepack.EventStateTransition, PrevState: new(tracepack.StateNotSelected), CurState: new(tracepack.StateNotConnected),
				Cause: new(tracepack.CauseTimerExpiry), CauseRaw: new("go-secs:CauseT7Timeout"), Timer: new(tracepack.TimerT7),
			},
		},
		{
			name: "state transition",
			ev: core.Event{
				Kind: tracepack.EventStateTransition, Prev: tracepack.StateNotSelected, Cur: tracepack.StateSelected,
				Cause: tracepack.CauseSelectAccepted, CauseRaw: "go-secs:CauseSelectAccepted",
			},
			want: tracepack.TransportEvent{
				Event: tracepack.EventStateTransition, PrevState: new(tracepack.StateNotSelected), CurState: new(tracepack.StateSelected),
				Cause: new(tracepack.CauseSelectAccepted), CauseRaw: new("go-secs:CauseSelectAccepted"),
			},
		},
		{
			name: "another timer names no primary",
			ev:   core.Event{Kind: tracepack.EventTimerExpiry, Timer: tracepack.TimerT6, SessionID: 1, Stream: 1, Function: 1, SystemBytes: 1},
			want: tracepack.TransportEvent{Event: tracepack.EventTimerExpiry, Timer: new(tracepack.TimerT6)},
		},
		{
			name: "T3",
			ev: core.Event{
				Kind: tracepack.EventTimerExpiry, Timer: tracepack.TimerT3,
				SessionID: 0x0102, Stream: 1, Function: 13, SystemBytes: 0x0a0b0c0d,
			},
			want: tracepack.TransportEvent{
				Event: tracepack.EventTimerExpiry, Timer: new(tracepack.TimerT3),
				PrimarySessionID: new(uint64(0x0102)), PrimaryStream: new(uint64(1)), PrimaryFunction: new(uint64(13)),
				PrimarySystemBytes: [4]byte{0x0a, 0x0b, 0x0c, 0x0d}, PrimarySystemBytesPresent: true,
			},
		},
		{
			name: "T3 of a forwarded request names its effective timeout",
			ev: core.Event{
				Kind: tracepack.EventTimerExpiry, Timer: tracepack.TimerT3,
				SessionID: 1, Stream: 1, Function: 3, SystemBytes: 9, Timeout: 45 * time.Second,
			},
			want: tracepack.TransportEvent{
				Event: tracepack.EventTimerExpiry, Timer: new(tracepack.TimerT3),
				PrimarySessionID: new(uint64(1)), PrimaryStream: new(uint64(1)), PrimaryFunction: new(uint64(3)),
				PrimarySystemBytes: [4]byte{0, 0, 0, 9}, PrimarySystemBytesPresent: true,
				Detail: new("effective_timeout_ms=45000"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, producer.Tunables{})
			h.begin()
			wall, mono := core.Now()
			h.rec.Observe(&core.Observation{
				Wall: wall, Mono: mono, Socket: 7,
				Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal, Fidelity: tracepack.FidelityNotApplicable,
				Event: tt.ev,
			})
			msgs := h.wait(2)
			br := check(t, &msgs[1])
			r := br.Record
			if r.Kind != tracepack.KindTransportEvent || r.Dir != tracepack.DirLocal ||
				r.Fidelity != tracepack.FidelityNotApplicable || r.Epoch != 1 || r.TSUTCNs != wall {
				t.Errorf("record = %+v", r)
			}
			got, err := tracepack.UnmarshalTransportEvent(r.Payload)
			if err != nil {
				t.Fatalf("UnmarshalTransportEvent: %v", err)
			}
			if !reflect.DeepEqual(*got, tt.want) {
				t.Errorf("event = %+v, want %+v", *got, tt.want)
			}
		})
	}
}

func TestObservedBoundaryEventIsNotPublished(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Tunables{})
	h.begin()
	for _, k := range []tracepack.Event{tracepack.EventCaptureBoundary, tracepack.EventClockStep, tracepack.EventSocketConnect} {
		wall, mono := core.Now()
		h.rec.Observe(&core.Observation{
			Wall: wall, Mono: mono, Socket: 7,
			Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal, Fidelity: tracepack.FidelityNotApplicable,
			Event: core.Event{Kind: k, Role: tracepack.SocketRoleActive},
		})
	}
	msgs := h.wait(2)
	if r := check(t, &msgs[1]).Record; r.Seq != 3 {
		t.Errorf("first record after the start has seq %d, want 3: an observed boundary or clock step was published", r.Seq)
	}
}
