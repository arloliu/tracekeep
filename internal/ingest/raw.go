package ingest

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"strings"

	"github.com/arloliu/go-secs/tracepack"
)

// headerLen is the record header every bus body starts with.
const headerLen = 44

// payloadDigestPrefix starts a Tpk-Payload-Digest value, which carries the padded base64url SHA-256 of the payload.
const payloadDigestPrefix = "SHA-256="

// Version identifies one version of a record (parking.md *Parking*, G5-269):
// the capture and seq, the 44 header bytes as the body carries them, and one payload digest.
type Version struct {
	Capture tracepack.UUID
	Seq     uint64
	Header  [headerLen]byte
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
// the layout and length checked first, then the version, whose capture and seq come from Nats-Msg-Id
// and whose digest is the claim's Tpk-Payload-Digest or the SHA-256 of the inline payload.
// A record older than horizonNS (UTC nanoseconds) is expired.
func ReadRaw(env *tracepack.BusEnvelope, body []byte, horizonNS int64) (Raw, Facts) {
	var r Raw
	var f Facts
	if env.Layout != "1" || len(body) < headerLen {
		return r, f
	}
	f.LayoutOK = true
	capture, seq, err := tracepack.ParseBusMsgID(env.MsgID)
	if err != nil {
		return r, f
	}
	r.Version.Capture, r.Version.Seq = capture, seq
	copy(r.Version.Header[:], body[:headerLen])
	r.TS = int64(binary.LittleEndian.Uint64(body[8:16])) //nolint:gosec // ts_utc_ns is signed little-endian, body bytes 8–15
	r.ClaimCheck = env.PayloadRef != ""
	if r.ClaimCheck {
		digest, ok := parseDigest(env.PayloadDigest)
		if !ok {
			return r, f // a bad claim header is malformed; without its digest there is no version to resolve
		}
		r.Version.Digest = digest
	} else {
		r.Version.Digest = sha256.Sum256(body[headerLen:])
	}
	f.MsgIDOK = true
	f.ClaimCheck = r.ClaimCheck
	f.Expired = r.TS < horizonNS

	return r, f
}

// parseDigest decodes a Tpk-Payload-Digest value.
func parseDigest(v string) ([32]byte, bool) {
	var d [32]byte
	enc, ok := strings.CutPrefix(v, payloadDigestPrefix)
	if !ok {
		return d, false
	}
	b, err := base64.URLEncoding.DecodeString(enc)
	if err != nil || len(b) != len(d) {
		return d, false
	}
	copy(d[:], b)

	return d, true
}
