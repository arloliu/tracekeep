package ingest

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/redact"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/policy"
)

// maxSteps bounds one delivery's steps; Decide finishes far sooner.
const maxSteps = 32

// recordKinds are the four kinds a record can be; any other is a producer fault.
var recordKinds = []tracepack.Kind{tracepack.KindData, tracepack.KindControl, tracepack.KindTransportEvent, tracepack.KindAnnotation}

// ErrClaimNotFound reports a claim object the bucket does not hold.
var ErrClaimNotFound = errors.New("ingest: claim object not found")

// ClaimStore reads claim objects: the claim bucket, a NATS object store on the traffic cluster (storage.md §5.2).
type ClaimStore interface {
	// Get returns the object named name, or an error wrapping ErrClaimNotFound.
	Get(ctx context.Context, name string) ([]byte, error)
}

// Message is one delivery as ingest reads it.
type Message struct {
	Env  tracepack.BusEnvelope
	Body []byte
	// Incarnation is the incarnation of the stream the message came from, and StreamSeq its sequence there.
	Incarnation int64
	StreamSeq   int64
	// Parked reports a copy delivered from the parking stream.
	Parked bool
	// Delivered counts the message's deliveries, this one included.
	Delivered uint64
	// Subject and Header are the message's as the bus delivered them, which its parked copy carries on.
	Subject string
	Header  nats.Header
}

// Delivery is one delivery being decided: the message, the facts gathered and what the steps found.
type Delivery struct {
	Msg   Message
	Raw   Raw
	Facts Facts
	// Record is the bus record once framed, with its payload once a claim object was fetched and its derived values once classified.
	Record tracepack.BusRecord
	// ConfigID is the capture's processing configuration once it is admitted.
	ConfigID int64
	// Claim is the claim object of a claim-check record once fetched, until the record is masked; ClaimSize is its size.
	Claim     []byte
	ClaimSize int64
	// Redaction is the masked record's redaction entries, nil when nothing was masked.
	Redaction []tracepack.RedactionEntry
	// MaskErr is why the masking failed, and Incident the incident it raises (ingest.md §6.1):
	// cannot-screen, key-binding-mismatch, key-refused or key-unavailable. The error names keys and seqs, never material or content.
	MaskErr  error
	Incident string
	// ToolID is the capture's tool once the capture is looked up and admitted, or its start admitted, for the metrics labelled by tool.
	ToolID string
}

// Config is a processing configuration this replica executes (rollouts.md §6.4):
// the classifier's name, which every pack of a capture bound to it records, its frame bound, and the classifier itself;
// and, for a masking configuration, its policy, compiled under its key ids.
type Config struct {
	Classifier tracepack.Classifier
	// Name names the classifier's implementation and its version: the configuration's identity is its content,
	// so two classifier releases sharing a name would share a configuration and pass the activation gate together.
	Name        string
	MaxFrameLen uint64
	// Policy is the masking policy, nil for a policy-less configuration; Masking is it compiled under DigestKey and EncryptionKey.
	Policy                   *policy.Policy
	DigestKey, EncryptionKey KeyRef
	Masking                  *redact.Compiled
	// FormatMinor is the format_minor the configuration's captures need, 0 for none; Masker records the masker it was staged with.
	FormatMinor int
	Masker      string
}

// Executor answers Decide's steps for a delivery: the reads, the claim fetch and the admission transactions.
type Executor struct {
	Catalog *catalog.Catalog
	Claims  ClaimStore
	// Horizon returns the retention horizon, in UTC nanoseconds: an older record is expired.
	Horizon func() int64
	// Configs are the processing configurations this replica executes, by id.
	Configs map[int64]Config
	// Keys gives each delivery's masking the keys its configuration names (masking-keys.md §9).
	Keys MaskKeys
	// Bindable admits the configurations an admission may bind, by id; nil admits any, as tracekeep dev does.
	// Outside dev it admits the masking configurations this replica executes (rollouts.md §6.4).
	Bindable func(configID int64) bool
	// Metrics records the masking's duration; nil records nothing.
	Metrics *metrics.Ingest

	// accepts is the descriptor check, nil for acceptedDescriptor; a test widens it to stand in for a fixed release.
	accepts func(descriptor []byte) bool
}

// Dispose decides msg's disposition, running the steps Decide asks for (ingest.md §6.1).
// An error is a step that could not run, the catalog or the claim bucket unreachable:
// nothing durable was decided, and the caller naks the message.
func (x *Executor) Dispose(ctx context.Context, msg Message) (*Delivery, Decision, error) {
	d := &Delivery{Msg: msg}
	d.Raw, d.Facts = ReadRaw(&d.Msg.Env, msg.Body, x.Horizon())
	d.Facts.LastDelay = nakDelay(msg.Delivered) >= lastNakDelay
	for range maxSteps {
		dec := Decide(&d.Facts)
		if dec.Step == StepNone {
			return d, dec, nil
		}
		if err := x.run(ctx, d, dec.Step); err != nil {
			return d, Decision{}, err
		}
	}

	return d, Decision{}, errors.New("ingest: no disposition after the step bound")
}

func (x *Executor) run(ctx context.Context, d *Delivery, s Step) error {
	f := &d.Facts
	switch s {
	case StepResolve:
		return x.resolve(ctx, d)
	case StepApplyOrder:
		return x.applyOrder(ctx, d)
	case StepFrame:
		br, err := tracepack.CheckBusMessage(d.Msg.Env, d.Msg.Body)
		switch {
		case errors.Is(err, tracepack.ErrBusMalformed):
			f.Framing = FramedMalformed
		case errors.Is(err, tracepack.ErrBusNotAccepted):
			f.Framing = FramedNotAccepted
		case err != nil:
			return err
		default:
			f.Framing, d.Record, f.Start = Framed, br, br.Descriptor != nil
		}
	case StepLookupCapture:
		return x.lookupCapture(ctx, d)
	case StepFetchClaim:
		return x.fetchClaim(ctx, d)
	case StepValidateDescriptor:
		f.Descriptor = No
		accepts := acceptedDescriptor
		if x.accepts != nil {
			accepts = x.accepts
		}
		if accepts(d.Record.Descriptor) {
			f.Descriptor = Yes
		}
	case StepAdmit:
		return x.admit(ctx, d)
	case StepReject:
		return x.reject(ctx, d)
	case StepLookupReceipt:
		m, err := x.Catalog.Receipt(ctx, d.versionKey())
		if err != nil {
			return err
		}
		f.Receipt = map[catalog.ReceiptMatch]Receipt{catalog.ReceiptNone: ReceiptNone, catalog.ReceiptEqual: ReceiptEqual, catalog.ReceiptOther: ReceiptOther}[m]
	case StepCheckConfig, StepDerive, StepMask:
		return x.record(ctx, d, s)
	case StepNone:
	default:
		return fmt.Errorf("ingest: unknown step %d", s)
	}

	return nil
}

func (x *Executor) resolve(ctx context.Context, d *Delivery) error {
	r, err := x.Catalog.Resolve(ctx, d.versionKey(), d.Msg.Incarnation, d.Msg.StreamSeq)
	if err != nil {
		return err
	}
	switch r {
	case catalog.ResolvedStaged:
		d.Facts.Resolution = ResolvedStaged
	case catalog.ResolvedTerminated:
		d.Facts.Resolution = ResolvedTerminated
	case catalog.CoveredByOrder:
		d.Facts.Resolution = CoveredByOrder
	case catalog.Unresolved:
		d.Facts.Resolution = Unresolved
	default:
		return fmt.Errorf("ingest: unknown resolution %d", r)
	}

	return nil
}

// applyOrder terminates the delivery's version under the order that covers it, and records the resolution it finds under the capture row:
// unresolved again when the order was spent meanwhile.
func (x *Executor) applyOrder(ctx context.Context, d *Delivery) error {
	r, err := x.Catalog.ApplyOrder(ctx, d.identity())
	if err != nil {
		return err
	}
	switch r {
	case catalog.ResolvedTerminated:
		d.Facts.Resolution = ResolvedTerminated
	case catalog.ResolvedStaged:
		d.Facts.Resolution = ResolvedStaged
	case catalog.Unresolved:
		d.Facts.Resolution = Unresolved
	case catalog.CoveredByOrder:
		return errors.New("ingest: an applied order returned covered")
	default:
		return fmt.Errorf("ingest: unknown resolution %d", r)
	}

	return nil
}

func (x *Executor) lookupCapture(ctx context.Context, d *Delivery) error {
	c, err := x.Catalog.Capture(ctx, d.Raw.Version.Capture)
	switch {
	case errors.Is(err, catalog.ErrUnknownCapture):
		d.Facts.Capture = CaptureUnknown
	case err != nil:
		return err
	case c.State == catalog.StatePending:
		d.Facts.Capture = CapturePending
	case c.State == catalog.StateRejected:
		d.Facts.Capture = CaptureRejected
	default:
		d.Facts.Capture, d.ConfigID, d.ToolID = CaptureAdmitted, c.ConfigID, c.ToolID
	}

	return nil
}

// fetchClaim fetches the claim object the message names and checks it against the length and digest the message declares;
// for a start, CheckBusPayload also decodes its descriptor.
func (x *Executor) fetchClaim(ctx context.Context, d *Delivery) error {
	payload, err := x.Claims.Get(ctx, d.Msg.Env.MsgID)
	if errors.Is(err, ErrClaimNotFound) {
		d.Facts.Claim = ClaimMissing
		return nil
	}
	if err != nil {
		return err
	}
	err = tracepack.CheckBusPayload(&d.Record, payload)
	switch {
	case errors.Is(err, tracepack.ErrBusPayloadMismatch):
		d.Facts.Claim = ClaimMismatch
	case errors.Is(err, tracepack.ErrBusNotAccepted):
		// A start whose descriptor does not decode: the payload is the start's, and the descriptor is not accepted.
		d.Facts.Claim, d.Facts.Start, d.Facts.Descriptor = ClaimOK, true, No
	case err != nil:
		return err // CheckBusPayload of a record that is not a claim-check one: an invariant failure, nak'd
	default:
		d.Facts.Claim, d.Facts.Start, d.Claim, d.ClaimSize = ClaimOK, d.Record.Descriptor != nil, payload, int64(len(payload))
	}

	return nil
}

func (x *Executor) admit(ctx context.Context, d *Delivery) error {
	meta, err := tracepack.DecodeDescriptor(d.Record.Descriptor)
	if err != nil {
		return err // validated by framing; a failure now is an invariant failure
	}
	desc := &catalog.Descriptor{
		Bytes: d.Record.Descriptor, ToolID: meta.ToolID, RecorderInstanceID: meta.RecorderInstanceID,
		PreviousCaptureID: meta.PreviousCaptureID,
	}
	if meta.RecorderInstanceName != nil {
		desc.Device = *meta.RecorderInstanceName
	}
	if meta.CaptureOriginUTCNs != nil {
		desc.OriginNS = *meta.CaptureOriginUTCNs
	}
	d.ToolID = meta.ToolID
	res, err := x.Catalog.AdmitWhere(ctx, d.identity(), desc, x.Bindable)
	if errors.Is(err, catalog.ErrNoBindableConfig) {
		d.Facts.Admitted, d.Facts.Unbindable = true, true
		return nil
	}
	if err != nil {
		return err
	}
	d.Facts.Admitted = true
	switch res.Outcome {
	case catalog.Terminated:
		d.Facts.Resolution = ResolvedTerminated
	case catalog.Admitted, catalog.Unchanged:
		d.Facts.Capture, d.ConfigID = CaptureAdmitted, res.ConfigID
	case catalog.DescriptorConflict:
		d.Facts.Conflict = true
	case catalog.Rejected, catalog.AlreadyAdmitted:
		return fmt.Errorf("ingest: admission returned the rejection outcome %d", res.Outcome)
	default:
		return fmt.Errorf("ingest: unknown admission outcome %d", res.Outcome)
	}

	return nil
}

func (x *Executor) reject(ctx context.Context, d *Delivery) error {
	res, err := x.Catalog.Reject(ctx, d.identity(), d.descriptorBytes())
	if err != nil {
		return err
	}
	switch res.Outcome {
	case catalog.Terminated:
		d.Facts.Resolution = ResolvedTerminated
	case catalog.Rejected:
		d.Facts.Rejected, d.Facts.Capture = true, CaptureRejected
	case catalog.AlreadyAdmitted:
		d.Facts.AlreadyAdmitted, d.Facts.Capture, d.ConfigID = true, CaptureAdmitted, res.ConfigID
	case catalog.DescriptorConflict:
		d.Facts.Conflict = true
	case catalog.Admitted, catalog.Unchanged:
		return fmt.Errorf("ingest: rejection returned the admission outcome %d", res.Outcome)
	default:
		return fmt.Errorf("ingest: unknown rejection outcome %d", res.Outcome)
	}

	return nil
}

// record runs the steps on the record itself, once its capture is admitted: the configuration's check, the classification and the masking.
func (x *Executor) record(ctx context.Context, d *Delivery, s Step) error {
	if s == StepCheckConfig {
		d.Facts.Config = No
		if _, ok := x.Configs[d.ConfigID]; ok {
			d.Facts.Config = Yes
		}

		return nil
	}
	if s == StepDerive {
		x.derive(d)
		return nil
	}

	return x.mask(ctx, d)
}

// MaskKeys gives one use's masking its keys; *keys.Keys is one.
type MaskKeys interface {
	ForUse(ctx context.Context) redact.Keys
}

// mask masks the record under its capture's configuration (ingest.md §6.2), unobserved as the bus record's quality bit 7 says;
// the delivery then holds the masked record and its entries, and drops the claim object's clear bytes.
// A policy-less configuration leaves the record as received. A failure is a verdict, not an error, unless the delivery's context ended.
func (x *Executor) mask(ctx context.Context, d *Delivery) error {
	cfg := x.Configs[d.ConfigID]
	if cfg.Masking == nil {
		d.Facts.Masked = MaskedOK
		return nil
	}
	started := time.Now()
	r := d.Record.Record
	masked, entries, err := cfg.Masking.Mask(d.Raw.Version.Capture, &r, r.Quality.Has(tracepack.QualityUnobserved), x.Keys.ForUse(ctx))
	x.Metrics.Masked(time.Since(started))
	switch {
	case err == nil:
		d.Record.Record, d.Redaction, d.Facts.Masked = masked, entries, MaskedOK
		d.Claim = nil
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(err, redact.ErrCannotScreen), errors.Is(err, redact.ErrNotUnobserved), errors.Is(err, redact.ErrRedacted):
		d.Facts.Masked, d.Incident = MaskedCannotScreen, ReasonCannotScreen
		x.Metrics.CannotScreen()
	case errors.Is(err, catalog.ErrKeyBindingMismatch):
		d.Facts.Masked, d.Incident = MaskedKeyRefused, IncidentKeyBindingMismatch
		x.Metrics.KeyBindingMismatch()
	case keys.Refused(err), errors.Is(err, redact.ErrKeySize):
		d.Facts.Masked, d.Incident = MaskedKeyRefused, ReasonKeyRefused
	default:
		d.Facts.Masked, d.Incident = MaskedKeyUnavailable, ReasonKeyUnavailable
	}
	if err != nil {
		d.MaskErr = err
	}

	return nil
}

// derive classifies the record with its capture's configuration.
func (x *Executor) derive(d *Delivery) {
	if !slices.Contains(recordKinds, d.Record.Record.Kind) {
		d.Facts.Derived = DerivedKindInvalid
		return
	}
	if err := tracepack.DeriveBusRecord(&d.Record.Record, x.Configs[d.ConfigID].Classifier); err != nil {
		d.Facts.Derived = DerivedClassifierInvalid
		return
	}
	d.Facts.Derived = DerivedOK
}

// identity is the delivery's identity, which every transaction re-checking its termination takes (catalog.md §7.2).
func (d *Delivery) identity() *catalog.Identity {
	return &catalog.Identity{Version: *d.versionKey(), Incarnation: d.Msg.Incarnation, StreamSeq: d.Msg.StreamSeq, Hour: HourOf(d.Raw.TS)}
}

func (d *Delivery) versionKey() *catalog.VersionKey {
	v := d.Raw.Version

	return &catalog.VersionKey{Capture: v.Capture, Seq: v.Seq, Header: v.Header, Digest: v.Digest}
}

// descriptorBytes are the start's descriptor as decoded, or, when it does not decode, the header as the message carries it,
// which a rejection keeps for the operator.
func (d *Delivery) descriptorBytes() []byte {
	if d.Record.Descriptor != nil {
		return d.Record.Descriptor
	}
	if b, err := base64.StdEncoding.DecodeString(d.Msg.Env.Descriptor); err == nil {
		return b
	}

	return []byte(d.Msg.Env.Descriptor)
}

// acceptedDescriptor reports whether this service accepts a start's descriptor:
// a raw-stream capture of an HSMS-SS connection that names its device (architecture.md §3.4).
func acceptedDescriptor(descriptor []byte) bool {
	m, err := tracepack.DecodeDescriptor(descriptor)

	return err == nil && m.Transport == tracepack.TransportHSMSSS && m.CaptureMethod == tracepack.CaptureMethodRawStream &&
		m.RecorderInstanceName != nil && strings.TrimSpace(*m.RecorderInstanceName) != ""
}

// AuditTermination writes the audit row of a beyond-retention termination: intentional, and visible (ingest.md §6.1).
func (x *Executor) AuditTermination(ctx context.Context, d *Delivery, reason string) error {
	capture := d.Raw.Version.Capture

	return x.Catalog.Audit(ctx, catalog.AuditEntry{
		Actor: "ingest", Action: reason, Capture: &capture, Detail: fmt.Sprintf("seq %d ts %d", d.Raw.Version.Seq, d.Raw.TS),
	})
}
