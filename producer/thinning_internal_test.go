package producer

import (
	"testing"
	"time"

	"github.com/arloliu/tracekeep/producer/internal/core"
)

// RegisterRequest takes its decision under the current descriptor's rule;
// an unmarked request, Register and a capture without a rule keep their bodies (producer.md §4.6).
func TestRegisterRequestTakesTheDescriptorsRule(t *testing.T) {
	t.Parallel()
	p := newTestProducer(t, Tunables{})
	d := testDescriptor()
	d.Thinning = &SamplingRule{KeepEveryN: 2, KeepInterval: time.Hour}
	if err := p.Begin(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	thin := func(tok Token) bool {
		e, ok := tok.(*core.PendingEntry)
		if !ok {
			t.Fatalf("token %T is no pending entry", tok)
		}

		return e.ThinSelected()
	}
	poll := Request{SessionID: 1, SystemBytes: 1, Stream: 1, Function: 3, Channel: "fdc", Origin: OriginIntervalPoll}
	if thin(p.Pending().RegisterRequest(poll)) {
		t.Error("the capture's first poll is thin-selected")
	}
	poll.SystemBytes = 2
	if !thin(p.Pending().RegisterRequest(poll)) {
		t.Error("the second poll under keep_every_n=2 kept its bodies")
	}
	unmarked := poll
	unmarked.SystemBytes, unmarked.Origin = 3, OriginUnmarked
	if thin(p.Pending().RegisterRequest(unmarked)) || thin(p.Pending().Register(1, 4, "fdc")) {
		t.Error("an unmarked registration is thin-selected")
	}

	if err := p.Rotate(t.Context(), testDescriptor()); err != nil {
		t.Fatal(err)
	}
	poll.SystemBytes = 5
	if thin(p.Pending().RegisterRequest(poll)) {
		t.Error("a poll under a capture without a rule is thin-selected")
	}
}
