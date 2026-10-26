package ingest

import (
	"crypto/sha256"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
)

// message builds an inline bus message of capture's record seq at ts, as a producer publishes it.
func message(t *testing.T, capture tracepack.UUID, seq uint64, ts int64, payload []byte) (tracepack.BusEnvelope, []byte) {
	t.Helper()
	br := tracepack.NewBusRecord(&tracepack.Record{
		Seq: seq, TSUTCNs: ts, Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, Fidelity: tracepack.FidelityWireExact, Payload: payload,
	})
	br.CaptureID = capture
	body, err := tracepack.EncodeBusBody(&br)
	if err != nil {
		t.Fatal(err)
	}
	id, err := tracepack.BusMsgID(capture, seq)
	if err != nil {
		t.Fatal(err)
	}

	return tracepack.BusEnvelope{Layout: "1", MsgID: id}, body
}

func TestReadRawInline(t *testing.T) {
	t.Parallel()
	capture := tracepack.UUID{1}
	payload := []byte("frame bytes")
	env, body := message(t, capture, 7, 5000, payload)
	r, f := ReadRaw(&env, body, 1000)
	if !f.LayoutOK || !f.MsgIDOK || f.ClaimCheck || f.Expired {
		t.Fatalf("facts %+v", f)
	}
	if r.Version.Capture != capture || r.Version.Seq != 7 || r.TS != 5000 || r.Version.Digest != sha256.Sum256(payload) {
		t.Errorf("raw %+v", r)
	}
	if [headerLen]byte(body[:headerLen]) != r.Version.Header {
		t.Error("the version's header is not the body's first 44 bytes")
	}
	if _, f := ReadRaw(&env, body, 6000); !f.Expired {
		t.Error("a record older than the horizon is not expired")
	}
}

func TestReadRawClaimCheck(t *testing.T) {
	t.Parallel()
	payload := []byte("a large frame")
	env, body := message(t, tracepack.UUID{2}, 3, 5000, payload)
	env.PayloadRef = "claims/" + env.MsgID
	env.PayloadDigest = tracepack.BusPayloadDigest(payload)
	r, f := ReadRaw(&env, body[:headerLen], 0)
	if !f.MsgIDOK || !f.ClaimCheck || r.Version.Digest != sha256.Sum256(payload) {
		t.Errorf("facts %+v, digest %x", f, r.Version.Digest)
	}
}

// What cannot be read before framing is malformed: Decide terminates it without looking further.
func TestReadRawMalformed(t *testing.T) {
	t.Parallel()
	env, body := message(t, tracepack.UUID{3}, 1, 5000, []byte("x"))
	cases := map[string]func(e *tracepack.BusEnvelope, b []byte) []byte{
		"layout":     func(e *tracepack.BusEnvelope, b []byte) []byte { e.Layout = "2"; return b },
		"short body": func(_ *tracepack.BusEnvelope, b []byte) []byte { return b[:headerLen-1] },
		"message id": func(e *tracepack.BusEnvelope, b []byte) []byte { e.MsgID = "nonsense"; return b },
		"claim digest": func(e *tracepack.BusEnvelope, b []byte) []byte {
			e.PayloadRef, e.PayloadDigest = "claims/"+e.MsgID, "MD5=abc"
			return b
		},
	}
	for name, mutate := range cases {
		e := env
		b := mutate(&e, append([]byte(nil), body...))
		if _, f := ReadRaw(&e, b, 0); f.LayoutOK && f.MsgIDOK {
			t.Errorf("%s: facts %+v, want malformed", name, f)
		} else if d := Decide(&f); d.Action != ActTerminate || d.Reason != ReasonMalformed {
			t.Errorf("%s: disposition %+v, want a malformed termination", name, d)
		}
	}
}
