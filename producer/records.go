package producer

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

// message is one entry made ready to publish.
type message struct {
	msg Msg
	// claim is set for a record that travels by claim-check: its payload, put before the message is published.
	claim *claimObject
}

type claimObject struct {
	meta    ObjectMeta
	payload []byte
	digest  string
}

// materialize turns a lane entry into its record ([SEM §4]: mono_ns from the capture's origin, mono present).
// The quality bits the SDK evaluates are set here: ordering-uncertain from allocation,
// correlation-incomplete for a record of no known epoch; direction is never inferred.
func materialize(e *core.Entry) (tracepack.Record, error) {
	r := tracepack.Record{
		Seq:         e.Seq,
		TSUTCNs:     e.Wall,
		MonoNs:      e.Mono - e.Cap.OriginMono,
		MonoPresent: true,
		Epoch:       e.Epoch,
		Quality:     e.Quality,
	}
	if r.Epoch == 0 {
		r.Quality |= tracepack.QualityCorrelationIncomplete
	}
	var ev tracepack.TransportEvent
	switch e.Kind {
	case core.EntryObserved:
		if e.RecKind != tracepack.KindTransportEvent {
			r.Kind, r.Dir, r.Fidelity, r.Payload = e.RecKind, e.Dir, e.Fidelity, e.Payload
			return r, nil
		}
		var err error
		if ev, err = observedEvent(&e.Event); err != nil {
			return r, err
		}
	case core.EntryStart:
		ev = tracepack.TransportEvent{Event: tracepack.EventCaptureBoundary, BoundaryKind: new(tracepack.BoundaryKindStart)}
	case core.EntryStop:
		ev = tracepack.TransportEvent{Event: tracepack.EventCaptureBoundary, BoundaryKind: new(tracepack.BoundaryKindStop)}
	case core.EntryClockStep:
		ev = tracepack.TransportEvent{Event: tracepack.EventClockStep, ClockStepNs: new(e.Drift)}
	default:
		return r, fmt.Errorf("producer: unknown entry kind %d", e.Kind)
	}
	payload, err := ev.MarshalBinary()
	if err != nil {
		return r, fmt.Errorf("producer: encode %v event: %w", ev.Event, err)
	}
	r.Kind, r.Dir, r.Fidelity, r.Payload = tracepack.KindTransportEvent, tracepack.DirLocal, tracepack.FidelityNotApplicable, payload

	return r, nil
}

// observedEvent builds the transport event an observer reported, with the tags its kind carries (tracepack-go.md §5);
// an observer never reports a capture boundary or a clock step, which the SDK writes itself.
func observedEvent(o *core.Event) (tracepack.TransportEvent, error) {
	ev := tracepack.TransportEvent{Event: o.Kind}
	switch o.Kind {
	case tracepack.EventSocketConnect, tracepack.EventSocketAccept:
		ev.SocketRole = new(o.Role)
	case tracepack.EventSocketClose:
		ev.Cause = new(o.Cause)
		if o.Role != tracepack.SocketRoleUnknown {
			ev.SocketRole = new(o.Role)
		}
	case tracepack.EventStateTransition:
		ev.PrevState, ev.CurState, ev.Cause = new(o.Prev), new(o.Cur), new(o.Cause)
		if o.Timer != tracepack.TimerNone {
			ev.Timer = new(o.Timer)
		}
	case tracepack.EventTimerExpiry:
		ev.Timer = new(o.Timer)
		if o.Timer != tracepack.TimerT3 {
			break
		}
		ev.PrimarySessionID = new(uint64(o.SessionID))
		ev.PrimaryStream, ev.PrimaryFunction = new(uint64(o.Stream)), new(uint64(o.Function))
		binary.BigEndian.PutUint32(ev.PrimarySystemBytes[:], o.SystemBytes)
		ev.PrimarySystemBytesPresent = true
	case tracepack.EventUnknown, tracepack.EventCaptureBoundary, tracepack.EventClockStep:
		return ev, fmt.Errorf("producer: an observer reported a %v event", o.Kind)
	default:
	}
	if o.CauseRaw != "" {
		ev.CauseRaw = new(o.CauseRaw)
	}

	return ev, nil
}

// buildMessage encodes e as its bus message: inline when its body fits max_payload − 4096, else by claim-check.
func (p *Producer) buildMessage(e *core.Entry, subject string, maxPayload int) (*message, error) {
	r, err := materialize(e)
	if err != nil {
		return nil, err
	}
	msgID, err := tracepack.BusMsgID(e.Cap.ID, e.Seq)
	if err != nil {
		return nil, err
	}
	m := &message{msg: Msg{
		Subject:      subject,
		Header:       map[string]string{tracepack.BusHeaderLayout: strconv.Itoa(tracepack.BusLayout1)},
		MsgID:        msgID,
		ExpectStream: p.stream,
	}}
	if e.Kind == core.EntryStart {
		m.msg.Header[tracepack.BusHeaderDescriptor] = e.Cap.Descriptor
	}

	br := tracepack.NewBusRecord(&r)
	if !tracepack.BusInline(44+len(r.Payload), maxPayload) {
		br.ClaimCheck, br.Bucket, br.Digest = true, p.bucket, sha256.Sum256(r.Payload)
	}
	body, err := tracepack.EncodeBusBody(&br)
	if err != nil {
		return nil, err
	}
	m.msg.Data = body
	if br.ClaimCheck {
		digest := tracepack.BusPayloadDigest(r.Payload)
		m.msg.Header[tracepack.BusHeaderPayloadRef] = p.bucket + "/" + msgID
		m.msg.Header[tracepack.BusHeaderPayloadDigest] = digest
		m.claim = &claimObject{
			meta: ObjectMeta{
				Name: msgID,
				Metadata: map[string]string{
					ObjectMetaRecordHeader: hex.EncodeToString(body),
					ObjectMetaTSUTCNs:      strconv.FormatInt(r.TSUTCNs, 16),
				},
				ChunkSize: chunkSize(maxPayload),
			},
			payload: r.Payload,
			digest:  digest,
		}
	}
	if err := tracepack.CheckBusStart(headerBlockLen(&m.msg), len(body), maxPayload); err != nil {
		return nil, err
	}

	return m, nil
}

// chunkSize keeps every object chunk, with its headers, within max_payload.
func chunkSize(maxPayload int) uint32 {
	return uint32(max(maxPayload-tracepack.BusHeaderBlockLimit, 1)) //nolint:gosec // max_payload fits in 32 bits
}
