package ingest

import (
	"slices"
	"testing"
)

// world answers the steps of one delivery as the executor would, by setting the facts each step finds;
// a later answer to a step overrides an earlier one.
type world []answer

type answer struct {
	step Step
	set  func(*Facts)
}

func (w world) answer(s Step) (func(*Facts), bool) {
	for i := len(w) - 1; i >= 0; i-- {
		if w[i].step == s {
			return w[i].set, true
		}
	}

	return nil, false
}

// drive runs Decide to a final disposition, answering each step from w, and returns it with the steps taken.
func drive(t *testing.T, f Facts, w world) (Decision, []Step) {
	t.Helper()
	var steps []Step
	for range 20 {
		d := Decide(&f)
		if d.Step == StepNone {
			return d, steps
		}
		steps = append(steps, d.Step)
		set, ok := w.answer(d.Step)
		if !ok {
			t.Fatalf("step %v asked, which this case does not answer (after %v)", d.Step, steps)
		}
		set(&f)
	}
	t.Fatalf("no final disposition after %v", steps)

	return Decision{}, nil
}

// Answers shared by the cases.
var (
	unresolved    = func(f *Facts) { f.Resolution = Unresolved }
	masks         = func(f *Facts) { f.Masked = MaskedOK }
	framed        = func(f *Facts) { f.Framing = Framed }
	framedStart   = func(f *Facts) { f.Framing, f.Start = Framed, true }
	unknown       = func(f *Facts) { f.Capture = CaptureUnknown }
	pendingCap    = func(f *Facts) { f.Capture = CapturePending }
	rejectedCap   = func(f *Facts) { f.Capture = CaptureRejected }
	admittedCap   = func(f *Facts) { f.Capture = CaptureAdmitted }
	accepted      = func(f *Facts) { f.Descriptor = Yes }
	notAccepted   = func(f *Facts) { f.Descriptor = No }
	admits        = func(f *Facts) { f.Admitted, f.Capture = true, CaptureAdmitted }
	conflicts     = func(f *Facts) { f.Admitted, f.Conflict = true, true }
	rejects       = func(f *Facts) { f.Rejected, f.Capture = true, CaptureRejected }
	executes      = func(f *Facts) { f.Config = Yes }
	derives       = func(f *Facts) { f.Derived = DerivedOK }
	claimOK       = func(f *Facts) { f.Claim = ClaimOK }
	claimOKStart  = func(f *Facts) { f.Claim, f.Start = ClaimOK, true }
	claimMissing  = func(f *Facts) { f.Claim = ClaimMissing }
	claimMismatch = func(f *Facts) { f.Claim = ClaimMismatch }
)

var raw = Facts{LayoutOK: true, MsgIDOK: true}

// One case per row of the ingest.md §6.1 table, plus the resolution rules above it.
// The cases prove the decision; the barrier schedules prove the re-reads under the capture row.
func TestDecide(t *testing.T) {
	t.Parallel()
	normal := world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, admittedCap}, {StepCheckConfig, executes}, {StepDerive, derives}, {StepMask, masks}}
	with := func(base, changes world) world { return append(append(world{}, base...), changes...) }
	claim := raw
	claim.ClaimCheck = true
	expired := raw
	expired.Expired = true
	expiredClaim := claim
	expiredClaim.Expired = true
	lastClaim := claim
	lastClaim.LastDelay = true

	cases := []struct {
		name  string
		facts Facts
		world world
		want  Decision
		steps []Step
	}{
		{"unknown layout or short body", Facts{MsgIDOK: true}, nil, Decision{Action: ActTerminate, Reason: ReasonMalformed, Alert: true}, nil},
		{"bad Nats-Msg-Id", Facts{LayoutOK: true}, nil, Decision{Action: ActTerminate, Reason: ReasonMalformed, Alert: true}, nil},
		{"version already staged: no framing, no admission", raw, world{{StepResolve, func(f *Facts) { f.Resolution = ResolvedStaged }}},
			Decision{Action: ActAck, Reason: ReasonAlreadyStaged}, []Step{StepResolve}},
		{"version terminated, even an undecodable start", raw, world{{StepResolve, func(f *Facts) { f.Resolution = ResolvedTerminated }}},
			Decision{Action: ActAck, Reason: ReasonTerminated}, []Step{StepResolve}},
		{"covered by a termination order", raw, world{{StepResolve, func(f *Facts) { f.Resolution = CoveredByOrder }}, {StepApplyOrder, func(f *Facts) { f.Resolution = ResolvedTerminated }}},
			Decision{Action: ActAck, Reason: ReasonTerminated}, []Step{StepResolve, StepApplyOrder}},
		{"ErrBusMalformed", raw, world{{StepResolve, unresolved}, {StepFrame, func(f *Facts) { f.Framing = FramedMalformed }}},
			Decision{Action: ActTerminate, Reason: ReasonMalformed, Alert: true}, []Step{StepResolve, StepFrame}},
		{"ErrBusNotAccepted: rejected durably, parked", raw, world{{StepResolve, unresolved}, {StepFrame, func(f *Facts) { f.Framing = FramedNotAccepted }}, {StepLookupCapture, unknown}, {StepReject, rejects}},
			Decision{Action: ActPark, Reason: ReasonCaptureRejected, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepReject}},
		{"start of an unknown capture: admitted, then ingested", raw, with(normal, world{{StepFrame, framedStart}, {StepLookupCapture, unknown}, {StepValidateDescriptor, accepted}, {StepAdmit, admits}}),
			Decision{Action: ActBatch}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepAdmit, StepCheckConfig, StepDerive, StepMask}},
		{"start of a rejected capture a fixed consumer accepts", raw, with(normal, world{{StepFrame, framedStart}, {StepLookupCapture, rejectedCap}, {StepValidateDescriptor, accepted}, {StepAdmit, admits}}),
			Decision{Action: ActBatch}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepAdmit, StepCheckConfig, StepDerive, StepMask}},
		{"descriptor not accepted: rejected durably, parked", raw, world{{StepResolve, unresolved}, {StepFrame, framedStart}, {StepLookupCapture, unknown}, {StepValidateDescriptor, notAccepted}, {StepReject, rejects}},
			Decision{Action: ActPark, Reason: ReasonCaptureRejected, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepReject}},
		{"rejection finds the capture admitted with these bytes: ingested", raw,
			with(normal, world{{StepFrame, framedStart}, {StepLookupCapture, unknown}, {StepValidateDescriptor, notAccepted}, {StepReject, func(f *Facts) { f.AlreadyAdmitted, f.Capture = true, CaptureAdmitted }}}),
			Decision{Action: ActBatch}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepReject, StepCheckConfig, StepDerive, StepMask}},
		{"claim-check start of an unknown capture: fetched first", claim,
			with(normal, world{{StepLookupCapture, unknown}, {StepFetchClaim, claimOKStart}, {StepValidateDescriptor, accepted}, {StepAdmit, admits}}),
			Decision{Action: ActBatch}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim, StepValidateDescriptor, StepAdmit, StepCheckConfig, StepDerive, StepMask}},
		{"claim-check message of an unknown capture whose object is missing", claim, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, unknown}, {StepFetchClaim, claimMissing}},
			Decision{Action: ActNak, Reason: ReasonClaimMissing, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim}},
		{"object still missing once the nak delay reached its last step: parked", lastClaim, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, unknown}, {StepFetchClaim, claimMissing}},
			Decision{Action: ActPark, Reason: ReasonClaimMissing, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim}},
		{"object of a rejected capture missing at the last delay: parked", lastClaim, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, rejectedCap}, {StepFetchClaim, claimMissing}},
			Decision{Action: ActPark, Reason: ReasonClaimMissing, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim}},
		{"mismatched object at the last delay: still a nak", lastClaim, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, unknown}, {StepFetchClaim, claimMismatch}},
			Decision{Action: ActNak, Reason: ReasonClaimMissing, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim}},
		{"other record of an unknown capture: never terminated", raw, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, unknown}},
			Decision{Action: ActNak, Reason: ReasonUnknownCapture}, []Step{StepResolve, StepFrame, StepLookupCapture}},
		{"record of a capture an earlier parking left pending: parked", raw, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, pendingCap}},
			Decision{Action: ActPark, Reason: ReasonUnknownCapture}, []Step{StepResolve, StepFrame, StepLookupCapture}},
		{"start of a pending capture: admitted in place", raw, with(normal, world{{StepFrame, framedStart}, {StepLookupCapture, pendingCap}, {StepValidateDescriptor, accepted}, {StepAdmit, admits}}),
			Decision{Action: ActBatch}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepAdmit, StepCheckConfig, StepDerive, StepMask}},
		{"start of a registered capture, equal bytes: normal ingestion", raw, with(normal, world{{StepFrame, framedStart}, {StepValidateDescriptor, accepted}, {StepAdmit, admits}}),
			Decision{Action: ActBatch}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepAdmit, StepCheckConfig, StepDerive, StepMask}},
		{"start of a registered capture, other bytes: conflict", raw, with(normal, world{{StepFrame, framedStart}, {StepValidateDescriptor, accepted}, {StepAdmit, conflicts}}),
			Decision{Action: ActNak, Reason: ReasonConflict, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepAdmit}},
		{"claim object missing, equal receipt: already staged", claim, with(normal, world{{StepFetchClaim, claimMissing}, {StepLookupReceipt, func(f *Facts) { f.Receipt = ReceiptEqual }}}),
			Decision{Action: ActAck, Reason: ReasonAlreadyStaged}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim, StepLookupReceipt}},
		{"claim mismatch, receipt of another version: claim conflict", claim, with(normal, world{{StepFetchClaim, claimMismatch}, {StepLookupReceipt, func(f *Facts) { f.Receipt = ReceiptOther }}}),
			Decision{Action: ActNak, Reason: ReasonClaimConflict, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim, StepLookupReceipt}},
		{"claim object missing, no receipt", claim, with(normal, world{{StepFetchClaim, claimMissing}, {StepLookupReceipt, func(f *Facts) { f.Receipt = ReceiptNone }}}),
			Decision{Action: ActNak, Reason: ReasonClaimMissing, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim, StepLookupReceipt}},
		{"claim-check record of an admitted capture", claim, with(normal, world{{StepFetchClaim, claimOK}}),
			Decision{Action: ActBatch}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim, StepCheckConfig, StepDerive, StepMask}},
		{"expired record of an admitted capture: terminated with an audit row", expired, normal,
			Decision{Action: ActTerminate, Reason: ReasonBeyondRetention, Alert: true, Audit: true}, []Step{StepResolve, StepFrame, StepLookupCapture}},
		{"expired start of an unknown capture: admitted first, then terminated", expired, with(normal, world{{StepFrame, framedStart}, {StepLookupCapture, unknown}, {StepValidateDescriptor, accepted}, {StepAdmit, admits}}),
			Decision{Action: ActTerminate, Reason: ReasonBeyondRetention, Alert: true, Audit: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepAdmit}},
		{"expired start rejected: parked whatever its age", expired, world{{StepResolve, unresolved}, {StepFrame, framedStart}, {StepLookupCapture, unknown}, {StepValidateDescriptor, notAccepted}, {StepReject, rejects}},
			Decision{Action: ActPark, Reason: ReasonCaptureRejected, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepReject}},
		{"expired record of a rejected capture: deferred", expired, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, rejectedCap}},
			Decision{Action: ActPark, Reason: ReasonDeferredAged}, []Step{StepResolve, StepFrame, StepLookupCapture}},
		{"expired record of an unknown capture: never terminated by age, parked", expired, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, unknown}},
			Decision{Action: ActPark, Reason: ReasonDeferredAged}, []Step{StepResolve, StepFrame, StepLookupCapture}},
		{"expired claim-check message of an unknown capture: fetched before age", expiredClaim, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, unknown}, {StepFetchClaim, claimOK}},
			Decision{Action: ActPark, Reason: ReasonDeferredAged}, []Step{StepResolve, StepFrame, StepLookupCapture, StepFetchClaim}},
		{"record of a rejected capture: parked", raw, world{{StepResolve, unresolved}, {StepFrame, framed}, {StepLookupCapture, rejectedCap}},
			Decision{Action: ActPark, Reason: ReasonCaptureRejected}, []Step{StepResolve, StepFrame, StepLookupCapture}},
		{"kind outside the four: terminated", raw, with(normal, world{{StepDerive, func(f *Facts) { f.Derived = DerivedKindInvalid }}}),
			Decision{Action: ActTerminate, Reason: ReasonInvalidRecord, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepCheckConfig, StepDerive}},
		{"classifier result out of range: never terminated", raw, with(normal, world{{StepDerive, func(f *Facts) { f.Derived = DerivedClassifierInvalid }}}),
			Decision{Action: ActNak, Reason: ReasonInvariant, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepCheckConfig, StepDerive}},
		{"configuration this replica cannot execute", raw, with(normal, world{{StepCheckConfig, func(f *Facts) { f.Config = No }}}),
			Decision{Action: ActNak, Reason: ReasonIncompatible, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepCheckConfig}},
		{"start with no configuration it may bind: waits as incompatible", raw, with(normal, world{{StepFrame, framedStart}, {StepLookupCapture, unknown}, {StepValidateDescriptor, accepted}, {StepAdmit, func(f *Facts) { f.Admitted, f.Unbindable = true, true }}}),
			Decision{Action: ActNak, Reason: ReasonIncompatible, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepValidateDescriptor, StepAdmit}},
		{"record masking cannot screen", raw, with(normal, world{{StepMask, func(f *Facts) { f.Masked = MaskedCannotScreen }}}),
			Decision{Action: ActNak, Reason: ReasonCannotScreen, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepCheckConfig, StepDerive, StepMask}},
		{"record whose key is refused: parked", raw, with(normal, world{{StepMask, func(f *Facts) { f.Masked = MaskedKeyRefused }}}),
			Decision{Action: ActPark, Reason: ReasonKeyRefused, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepCheckConfig, StepDerive, StepMask}},
		{"record whose key is unavailable: nak'd", raw, with(normal, world{{StepMask, func(f *Facts) { f.Masked = MaskedKeyUnavailable }}}),
			Decision{Action: ActNak, Reason: ReasonKeyUnavailable, Alert: true}, []Step{StepResolve, StepFrame, StepLookupCapture, StepCheckConfig, StepDerive, StepMask}},
		{"ordinary record", raw, normal, Decision{Action: ActBatch}, []Step{StepResolve, StepFrame, StepLookupCapture, StepCheckConfig, StepDerive, StepMask}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, steps := drive(t, c.facts, c.world)
			if got != c.want {
				t.Errorf("disposition %+v, want %+v", got, c.want)
			}
			if !slices.Equal(steps, c.steps) {
				t.Errorf("steps %v, want %v", steps, c.steps)
			}
		})
	}
}
