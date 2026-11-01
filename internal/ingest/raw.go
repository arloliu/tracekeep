package ingest

import (
	"crypto/sha256"

	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"
)

// Version identifies one version of a record (parking.md *Parking*, G5-269):
// the capture and seq, the 44 header bytes as the body carries them, and one payload digest.
type Version struct {
	Capture tracepack.UUID
	Seq     uint64
	Header  [tpbus.RecordHeaderLen]byte
	Digest  [32]byte
}

// Raw is what is read from a message before CheckBusMessage sees it (ingest.md §6.1):
// the version, the record's ts_utc_ns, and whether it travelled by claim-check.
type Raw struct {
	Version    Version
	TS         int64
	ClaimCheck bool
}

// ReadRaw reads env and body as far as resolution needs, and returns the raw facts:
// the layout and length checked first (a layout other than 1 is LayoutUnknown, deferred, not malformed), then the version, whose capture and seq come from Nats-Msg-Id
// and whose digest is the claim's Tpk-Payload-Digest or the SHA-256 of the inline payload.
// A record older than horizonNS (UTC nanoseconds) is expired.
func ReadRaw(env *tpbus.Envelope, body []byte, horizonNS int64) (Raw, Facts) {
	var r Raw
	var f Facts
	if env.Layout != tpbus.Layout1 {
		f.LayoutUnknown = true
		return r, f
	}
	header, ok := tpbus.PeekHeader(body)
	if !ok {
		return r, f
	}
	f.LayoutOK = true
	capture, seq, err := tpbus.ParseMsgID(env.MsgID)
	if err != nil {
		return r, f
	}
	r.Version.Capture, r.Version.Seq = capture, seq
	copy(r.Version.Header[:], body[:tpbus.RecordHeaderLen])
	r.TS = header.TSUTCNs
	r.ClaimCheck = env.PayloadRef != ""
	if r.ClaimCheck {
		digest, err := tpbus.ParsePayloadDigest(env.PayloadDigest)
		if err != nil {
			return r, f // a bad claim header is malformed; without its digest there is no version to resolve
		}
		r.Version.Digest = digest
	} else {
		r.Version.Digest = sha256.Sum256(body[tpbus.RecordHeaderLen:])
	}
	f.MsgIDOK = true
	f.ClaimCheck = r.ClaimCheck
	f.Expired = r.TS < horizonNS

	return r, f
}
