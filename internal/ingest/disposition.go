// Package ingest is the ingest role (docs/specs/tracekeep/ingest.md): it decides each delivery's disposition,
// admits captures, batches records per (capture, hour), writes and registers segments, and acknowledges each message.
package ingest

// Disposition is the decision procedure of ingest.md §6.1, kept apart from its effects:
// Decide takes the facts gathered so far for one delivery and returns a final disposition or the next step it needs.
// The step runs, its result joins the facts, and Decide is called again.

// Step is the next fact a decision needs: evidence gathered without a durable effect,
// or a transactional operation that re-reads its facts under the capture row and returns the actual outcome.
type Step int

const (
	// StepNone: the decision is final.
	StepNone Step = iota
	// StepResolve looks the delivery's version up in parked and its capture's active termination orders.
	StepResolve
	// StepApplyOrder terminates an unresolved version an active termination order covers (transactional).
	StepApplyOrder
	// StepFrame runs CheckBusMessage.
	StepFrame
	// StepLookupCapture reads the capture's state.
	StepLookupCapture
	// StepFetchClaim fetches the claim object and checks it with CheckBusPayload.
	StepFetchClaim
	// StepValidateDescriptor validates a start's descriptor: whether a consumer of this service accepts it.
	StepValidateDescriptor
	// StepAdmit runs the admission transaction.
	StepAdmit
	// StepReject durably writes the capture rejected.
	StepReject
	// StepLookupReceipt looks for a receipt of (capture, seq).
	StepLookupReceipt
	// StepCheckConfig checks that this replica executes the capture's processing configuration.
	StepCheckConfig
	// StepDerive classifies the record with the bound configuration (DeriveBusRecord).
	StepDerive
)

// Action is what a final disposition does with the message.
type Action int

const (
	// ActBatch adds the record to its (capture, hour) batch; it is acknowledged once a segment holding it is registered.
	ActBatch Action = iota + 1
	// ActAck acknowledges a resolved message without writing it: already staged, or under a termination.
	ActAck
	// ActTerminate terminates the message on the bus; its seq becomes a gap or a declared loss.
	ActTerminate
	// ActNak returns the message for redelivery after a delay; nothing durable changes.
	ActNak
	// ActPark moves a deferral that cannot clear by itself within the nak backoff to the parking stream (parking.md *Parking*).
	ActPark
)

// String names the action for the messages_total metric's disposition.
func (a Action) String() string {
	switch a {
	case ActBatch:
		return "batch"
	case ActAck:
		return "ack"
	case ActTerminate:
		return "terminate"
	case ActNak:
		return "nak"
	case ActPark:
		return "park"
	default:
		return "unknown"
	}
}

// Reasons name a disposition for its metric, its alert and the nak delay.
const (
	ReasonMalformed       = "malformed"
	ReasonAlreadyStaged   = "already-staged"
	ReasonTerminated      = "terminated"
	ReasonCaptureRejected = "capture-rejected"
	ReasonConflict        = "descriptor-conflict"
	ReasonClaimMissing    = "claim-missing"
	ReasonClaimConflict   = "claim-conflict"
	ReasonUnknownCapture  = "unknown-capture"
	ReasonDeferredAged    = "deferred-expired"
	ReasonBeyondRetention = "beyond-retention"
	ReasonIncompatible    = "incompatible-configuration"
	ReasonInvalidRecord   = "invalid-record"
	ReasonInvariant       = "invariant"
	ReasonWriteFailure    = "write-failure"
)

// Decision is Decide's answer: a step to run, or a final action with its reason.
type Decision struct {
	Step   Step
	Action Action
	Reason string
	// Alert reports that the disposition raises an alert.
	Alert bool
	// Audit reports that the disposition writes an audit row (a beyond-retention termination).
	Audit bool
}

// Resolution is the delivery's version as parked and the termination orders know it.
type Resolution int

const (
	ResolutionUnknown Resolution = iota // not looked up yet
	Unresolved
	ResolvedStaged
	ResolvedTerminated
	// CoveredByOrder: unresolved, and an active termination order of the capture covers the message's stream sequence.
	CoveredByOrder
)

// Framing is CheckBusMessage's verdict.
type Framing int

const (
	FramingUnknown Framing = iota
	Framed
	// FramedMalformed is ErrBusMalformed.
	FramedMalformed
	// FramedNotAccepted is ErrBusNotAccepted: a start whose descriptor does not decode.
	FramedNotAccepted
)

// CaptureState is the capture as the catalog knows it.
type CaptureState int

const (
	CaptureUnknownState CaptureState = iota // not looked up yet
	// CaptureUnknown: no row.
	CaptureUnknown
	// CapturePending: a pending row, which an earlier parking of the capture's messages inserted; the capture is still unknown.
	CapturePending
	CaptureRejected
	// CaptureAdmitted: open, stopped or closed.
	CaptureAdmitted
)

// Claim is the claim object's fetch and check.
type Claim int

const (
	ClaimUnknown Claim = iota // not fetched yet
	ClaimOK
	ClaimMissing
	// ClaimMismatch is ErrBusPayloadMismatch.
	ClaimMismatch
)

// Receipt is the lookup of a receipt for (capture, seq).
type Receipt int

const (
	ReceiptUnknown Receipt = iota // not looked up yet
	// ReceiptEqual: a receipt whose header and payload digest equal the delivery's.
	ReceiptEqual
	// ReceiptOther: a receipt of another version.
	ReceiptOther
	ReceiptNone
)

// Verdict is the answer of a yes-or-no step.
type Verdict int

const (
	VerdictUnknown Verdict = iota
	Yes
	No
)

// Derived is DeriveBusRecord's verdict.
type Derived int

const (
	DerivedUnknown Derived = iota
	DerivedOK
	// DerivedKindInvalid: a record kind outside the four; a producer fault.
	DerivedKindInvalid
	// DerivedClassifierInvalid: a classifier result out of range, or CheckBusPayload called for a non-claim record; a service fault.
	DerivedClassifierInvalid
)

// Facts are what is known about one delivery.
// The raw facts come from the message alone; every other field starts unknown and is set by the step that finds it.
type Facts struct {
	// Raw: Tpk-Layout 1 and a body of at least 44 bytes; Nats-Msg-Id parses to (capture, seq).
	LayoutOK, MsgIDOK bool
	// Expired: ts_utc_ns, read at body offset 8, is older than the retention horizon.
	Expired bool
	// ClaimCheck: the message carries a claim reference.
	ClaimCheck bool
	// LastDelay: the message's nak delay reached its last step, so a wait that has not cleared by now is taken as indefinite.
	LastDelay bool

	Resolution Resolution
	Framing    Framing
	// Start: the record is a start; known after framing for an inline message, after the fetch for a claim-check one.
	Start   bool
	Capture CaptureState
	Claim   Claim
	// Descriptor: whether this service accepts the start's descriptor.
	Descriptor Verdict
	// Admitted: the admission transaction ran for this delivery.
	Admitted bool
	// Rejected: the durable write of rejected ran for this delivery and the capture is rejected.
	Rejected bool
	// AlreadyAdmitted: the durable write of rejected found the capture admitted with these descriptor bytes,
	// so the start is ingested as a redelivery of the admitted one (catalog.md §7.2 `Reject`, T-32).
	AlreadyAdmitted bool
	// Conflict: the capture holds other descriptor bytes.
	Conflict bool
	Receipt  Receipt
	// Config: this replica executes the capture's processing configuration.
	Config  Verdict
	Derived Derived
}

// Decide returns the disposition of the delivery f describes, or the next step it needs (ingest.md §6.1).
// It performs no I/O and holds no lock; the order of its checks is §6.1's:
// layout and length, resolution before framing, framing, admission before any age decision, age, then the waits.
func Decide(f *Facts) Decision {
	if !f.LayoutOK || !f.MsgIDOK {
		return terminate(ReasonMalformed, true)
	}
	switch f.Resolution {
	case ResolutionUnknown:
		return step(StepResolve)
	case ResolvedStaged:
		return final(ActAck, ReasonAlreadyStaged)
	case ResolvedTerminated:
		return final(ActAck, ReasonTerminated)
	case CoveredByOrder:
		return step(StepApplyOrder)
	case Unresolved:
	default:
		panic("ingest: unknown resolution")
	}
	switch f.Framing {
	case FramingUnknown:
		return step(StepFrame)
	case FramedMalformed:
		return terminate(ReasonMalformed, true)
	case FramedNotAccepted, Framed:
	default:
		panic("ingest: unknown framing")
	}
	if f.Capture == CaptureUnknownState {
		return step(StepLookupCapture)
	}
	if f.Conflict {
		return nak(ReasonConflict, true)
	}
	if d, ok := admission(f); ok {
		return d
	}
	if f.Expired {
		if f.Capture == CaptureAdmitted {
			return Decision{Action: ActTerminate, Reason: ReasonBeyondRetention, Alert: true, Audit: true}
		}
		// A rejected or unknown capture's record is never terminated by age (G5-251): it is deferred indefinitely.
		return park(ReasonDeferredAged, false)
	}

	return waits(f)
}

// waits is the wait on the capture's state, once admission and age are decided: a record of a capture that is not admitted waits,
// on the bus while its start may still come, on the parking stream otherwise; an admitted capture's record goes on.
func waits(f *Facts) Decision {
	switch f.Capture {
	case CaptureUnknown:
		// Its start may still come: the wait can clear by itself.
		return nak(ReasonUnknownCapture, false)
	case CapturePending:
		// An earlier message of the capture was parked, so this one would wait behind it.
		return park(ReasonUnknownCapture, false)
	case CaptureRejected:
		return park(ReasonCaptureRejected, false)
	case CaptureUnknownState, CaptureAdmitted:
	default:
		panic("ingest: unknown capture state")
	}

	return admitted(f)
}

// admission is the start path, before any age decision: a delivery that may be a start completes recognition,
// validation and the admission or rejection transaction; ok is false once there is nothing left to do here.
func admission(f *Facts) (Decision, bool) {
	// A claim-check message of a capture that is not admitted may be a start, which only its payload shows.
	if f.ClaimCheck && f.Capture != CaptureAdmitted {
		switch f.Claim {
		case ClaimUnknown:
			return step(StepFetchClaim), true
		case ClaimMissing:
			// A producer may still be putting the object, so only a wait past the nak backoff is taken as indefinite.
			if f.LastDelay {
				return park(ReasonClaimMissing, true), true
			}

			return nak(ReasonClaimMissing, true), true
		case ClaimMismatch:
			return nak(ReasonClaimMissing, true), true
		case ClaimOK:
		default:
			panic("ingest: unknown claim")
		}
	}
	// A start whose descriptor does not decode (ErrBusNotAccepted) is a start this service does not accept.
	start, descriptor := f.Start, f.Descriptor
	if f.Framing == FramedNotAccepted {
		start, descriptor = true, No
	}
	if !start || f.AlreadyAdmitted {
		return Decision{}, false
	}
	switch {
	case descriptor == VerdictUnknown:
		return step(StepValidateDescriptor), true
	case descriptor == No && !f.Rejected:
		return step(StepReject), true
	case descriptor == No:
		return park(ReasonCaptureRejected, true), true
	case !f.Admitted:
		return step(StepAdmit), true
	}

	return Decision{}, false
}

// admitted finishes the delivery of an admitted capture's record: its claim payload, its configuration, its classification.
func admitted(f *Facts) Decision {
	if f.ClaimCheck {
		switch f.Claim {
		case ClaimUnknown:
			return step(StepFetchClaim)
		case ClaimMissing, ClaimMismatch:
			switch f.Receipt {
			case ReceiptUnknown:
				return step(StepLookupReceipt)
			case ReceiptEqual:
				return final(ActAck, ReasonAlreadyStaged)
			case ReceiptOther:
				return nak(ReasonClaimConflict, true)
			case ReceiptNone:
				return nak(ReasonClaimMissing, true)
			default:
				panic("ingest: unknown receipt")
			}
		case ClaimOK:
		default:
			panic("ingest: unknown claim")
		}
	}
	switch f.Config {
	case VerdictUnknown:
		return step(StepCheckConfig)
	case No:
		return nak(ReasonIncompatible, true)
	case Yes:
	default:
		panic("ingest: unknown verdict")
	}
	switch f.Derived {
	case DerivedUnknown:
		return step(StepDerive)
	case DerivedKindInvalid:
		return terminate(ReasonInvalidRecord, true)
	case DerivedClassifierInvalid:
		return nak(ReasonInvariant, true)
	case DerivedOK:
	default:
		panic("ingest: unknown derivation")
	}

	return final(ActBatch, "")
}

func step(s Step) Decision { return Decision{Step: s} }

func final(a Action, reason string) Decision { return Decision{Action: a, Reason: reason} }

func terminate(reason string, alert bool) Decision {
	return Decision{Action: ActTerminate, Reason: reason, Alert: alert}
}

func nak(reason string, alert bool) Decision {
	return Decision{Action: ActNak, Reason: reason, Alert: alert}
}

func park(reason string, alert bool) Decision {
	return Decision{Action: ActPark, Reason: reason, Alert: alert}
}
