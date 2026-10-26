package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// wrongLastSequence starts the description of nats-server's error 10071, which carries the subject's stored last sequence as text
// (nats-server v2.15.1 JSStreamWrongLastSequenceErrF: "wrong last sequence: {seq}"); nats.go has no structured field for it.
const wrongLastSequence = "wrong last sequence: "

// Publisher publishes a copy to the parking stream; a jetstream.JetStream is one.
type Publisher interface {
	PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

// Confirmation is how the parking protocol reads the answer to a copy's publish (parking.md *Confirmation*).
type Confirmation int

const (
	// Confirmed: the copy is stored, by this publish or an earlier one of the same version.
	Confirmed Confirmation = iota + 1
	// Suppressed: the duplicate window answered, and nothing was stored; the original waits longer than the window and is published again.
	Suppressed
	// Unconfirmed: any other answer; the original stays unacknowledged and is published again.
	Unconfirmed
)

// Coordinator owns the parking protocol's steps, so no caller holds half of them (parking.md *The parking coordinator*):
// begin under the capture row, publish only an unresolved version, confirm by two outcomes only, advance by compare-and-set,
// and acknowledge, a DoubleAck and then the delivery's copy row acked.
// No catalog transaction is open across the publish or the acknowledgement.
type Coordinator struct {
	Catalog *catalog.Catalog
	Bus     Publisher
	// ParkPrefix is the parking stream's subject prefix.
	ParkPrefix string
}

// Parking is what parking an original did.
type Parking struct {
	// Begun is what begin found: a resolved version, or a new one past the horizon, publishes nothing.
	Begun catalog.Begun
	// Confirmation is the copy's publish, zero when nothing was published.
	Confirmation Confirmation
}

// Park runs the protocol for the original e, deferred indefinitely for reason:
// begin; for an unresolved version, publish and confirm the copy, advance, and acknowledge the original.
// A resolved version, a version past the horizon or an unconfirmed copy is returned for the caller to act on, nothing acknowledged;
// an error leaves the original unacknowledged.
func (k *Coordinator) Park(ctx context.Context, e Entry, reason string, horizonHour int64) (Parking, error) {
	b, err := k.Begin(ctx, e.D, reason, horizonHour)
	if err != nil || b.BeyondRetention || b.State.Resolved() {
		return Parking{Begun: b}, err
	}
	c, err := k.Publish(ctx, e.D, b.State)
	if err != nil || c != Confirmed {
		return Parking{Begun: b, Confirmation: c}, err
	}
	if _, err := k.Advance(ctx, e.D); err != nil {
		return Parking{Begun: b, Confirmation: c}, err
	}

	return Parking{Begun: b, Confirmation: c}, k.Acknowledge(ctx, e.D, e.Msg, false)
}

// Begin is step (1): the version's row and the delivery's copy row, under the capture row; it returns the version's state.
func (k *Coordinator) Begin(ctx context.Context, d *Delivery, reason string, horizonHour int64) (catalog.Begun, error) {
	var objectKey string
	if d.Raw.ClaimCheck {
		objectKey = d.Msg.Env.PayloadRef // the claim object a claim-check record keeps alive
	}

	return k.Catalog.BeginParking(ctx, d.identity(), reason, objectKey, horizonHour)
}

// Publish is step (2) for a version in state, which must be unresolved: the copy on the version's own subject,
// expecting no message stored under it.
func (k *Coordinator) Publish(ctx context.Context, d *Delivery, state catalog.VersionState) (Confirmation, error) {
	if state != catalog.VersionParking && state != catalog.VersionParked && state != catalog.VersionEvictedHour {
		return 0, fmt.Errorf("ingest: publish of a version %s", state)
	}
	subject, err := parkSubject(k.ParkPrefix, d.Msg.Subject, d.Raw.Version)
	if err != nil {
		return 0, err
	}
	m := &nats.Msg{Subject: subject, Header: nats.Header{}, Data: d.Msg.Body}
	for key, vs := range d.Msg.Header {
		m.Header[key] = append([]string(nil), vs...)
	}
	// The SDK's publish stamps it, and the parking stream would refuse it; every other header travels unchanged.
	m.Header.Del(jetstream.ExpectedStreamHeader)
	ack, err := k.Bus.PublishMsg(ctx, m, jetstream.WithExpectLastSequencePerSubject(0))

	return confirm(ack, err), nil
}

// Advance is step (3), parking → parked; a version past parking is left as it is.
func (k *Coordinator) Advance(ctx context.Context, d *Delivery) (catalog.VersionState, error) {
	v := d.versionKey()

	return k.Catalog.AdvanceParked(ctx, v)
}

// Acknowledge is step (4) for the delivery the caller holds, original or copy:
// a DoubleAck, then its copy row acked once the server confirmed it.
// A message of a terminated version is acknowledged under the termination: its audit row, naming the action, is written first,
// so no such acknowledgement goes unrecorded, and a redelivery acknowledged again writes its own (parking.md *Resolution*).
func (k *Coordinator) Acknowledge(ctx context.Context, d *Delivery, m Msg, terminated bool) error {
	if terminated {
		action, err := k.Catalog.TerminatedBy(ctx, d.versionKey())
		if err != nil {
			return err
		}
		capture := d.Raw.Version.Capture
		err = k.Catalog.Audit(ctx, catalog.AuditEntry{
			Actor: "ingest", Action: ReasonTerminated, ActionID: action, Capture: &capture,
			Detail: fmt.Sprintf("seq %d incarnation %d stream seq %d", d.Raw.Version.Seq, d.Msg.Incarnation, d.Msg.StreamSeq),
		})
		if err != nil {
			return err
		}
	}
	if err := m.DoubleAck(ctx); err != nil {
		return err
	}

	return k.Catalog.AckCopy(ctx, d.identity())
}

// confirm reads a copy's publish: a PubAck with Duplicate false, or error 10071 whose description parses to a strictly positive sequence
// (the version's subject holds a message, which can only be a copy of this version), confirms it;
// a PubAck with Duplicate true stored nothing; anything else, 10164, a timeout, a zero or malformed description, confirms nothing.
func confirm(ack *jetstream.PubAck, err error) Confirmation {
	if err == nil {
		if ack.Duplicate {
			return Suppressed
		}

		return Confirmed
	}
	var api *jetstream.APIError
	if !errors.As(err, &api) || api.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
		return Unconfirmed
	}
	text, ok := strings.CutPrefix(api.Description, wrongLastSequence)
	if seq, perr := strconv.ParseUint(text, 10, 64); ok && perr == nil && seq > 0 {
		return Confirmed
	}

	return Unconfirmed
}

// parkSubject is the version's own subject on the parking stream, <park prefix>.<tool token>.<capture_id>.<seq>.<version token>,
// the tool token taken from the original's traffic subject, whose last two tokens are the tool token and the capture id.
// The version token is the full lowercase hex of SHA-256 over the 44 header bytes and the 32 digest bytes,
// so two versions of one seq never share a subject.
func parkSubject(parkPrefix, trafficSubject string, v Version) (string, error) {
	tokens := strings.Split(trafficSubject, ".")
	if len(tokens) < 3 || tokens[len(tokens)-1] != v.Capture.String() {
		return "", fmt.Errorf("ingest: traffic subject %q does not end with a tool token and capture %s", trafficSubject, v.Capture)
	}
	h := sha256.New()
	h.Write(v.Header[:])
	h.Write(v.Digest[:])

	return fmt.Sprintf("%s.%s.%s.%d.%s", parkPrefix, tokens[len(tokens)-2], v.Capture, v.Seq, hex.EncodeToString(h.Sum(nil))), nil
}
