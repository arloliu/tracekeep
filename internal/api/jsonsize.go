package api

import (
	"encoding/base64"
	"sync"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// The range of a Timestamp protojson encodes: from 0001-01-01 to 9999-12-31.
const (
	minTimestamp = -62135596800
	maxTimestamp = 253402300799
)

// jsonComma is the width of the separator protojson writes between two fields on one line:
// a comma, and in some binaries a space after it, which protojson chooses once a binary to keep its output unstable.
var jsonComma = sync.OnceValue(func() int {
	b, err := protojson.Marshal(&tkv1.Holder{PackId: "a", State: "b"})
	if err != nil {
		panic(err) // two ASCII strings always encode
	}

	return len(b) - len(`{"packId":"a""state":"b"}`)
})

// jsonObject counts the bytes of a JSON object protojson writes, field by field, the zero values it omits left out.
type jsonObject struct {
	n, fields int
	// refused is set by a value protojson refuses to encode.
	refused bool
}

// jsonRecordSize is the length of protojson's encoding of a record item, counted without encoding it.
// It answers false for any other item, and for a record protojson refuses (a string not UTF-8, a time outside its range),
// which the caller encodes instead.
func jsonRecordSize(it *tkv1.RecordItem) (int, bool) {
	r := it.GetRecord()
	if r == nil {
		return 0, false
	}
	var o jsonObject
	o.time("hour", r.GetHour())
	o.quoted("seq", r.GetSeq())
	o.time("time", r.GetTime())
	o.uint("epoch", uint64(r.GetEpoch()))
	o.str("kind", r.GetKind())
	o.str("direction", r.GetDirection())
	if h := r.GetHeader(); h != nil {
		var ho jsonObject
		ho.uint("sessionId", uint64(h.GetSessionId()))
		ho.uint("stream", uint64(h.GetStream()))
		ho.uint("function", uint64(h.GetFunction()))
		ho.bool("w", h.GetW())
		ho.bytes("systemBytes", h.GetSystemBytes())
		o.field("header", ho.size())
		o.refused = o.refused || ho.refused
	}
	o.bool("conflict", r.GetConflict())
	o.bool("excluded", r.GetExcluded())
	o.bool("unobserved", r.GetUnobserved())
	o.bytes("payload", r.GetPayload())
	o.str("sml", r.GetSml())
	o.bool("smlTruncated", r.GetSmlTruncated())
	o.str("smlError", r.GetSmlError())
	if hs := r.GetHolders(); len(hs) > 0 {
		n := 2 + (len(hs)-1)*jsonComma()
		for _, h := range hs {
			var ho jsonObject
			ho.str("packId", h.GetPackId())
			ho.str("state", h.GetState())
			n += ho.size()
			o.refused = o.refused || ho.refused
		}
		o.field("holders", n)
	}
	var item jsonObject
	item.field("record", o.size())

	return item.size(), !o.refused
}

// jsonString is the length of s as protojson writes it, quoted and escaped, or false for a string not UTF-8.
func jsonString(s string) (int, bool) {
	n := 2
	for i := 0; i < len(s); {
		c := s[i]
		if c >= utf8.RuneSelf {
			r, w := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && w == 1 {
				return 0, false
			}
			n, i = n+w, i+w

			continue
		}
		switch c {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			n += 2
		default:
			if c < ' ' {
				n += 6 // \u00XX
			} else {
				n++
			}
		}
		i++
	}

	return n, true
}

// digits is the decimal digits of v.
func digits(v uint64) int {
	n := 1
	for ; v >= 10; v /= 10 {
		n++
	}

	return n
}

// field counts a field whose value takes n bytes: its quoted name, a colon, the value, and a separator before it but the first.
func (o *jsonObject) field(name string, n int) {
	if o.fields > 0 {
		o.n += jsonComma()
	}
	o.fields++
	o.n += len(name) + 3 + n
}

// size is the object's length with its braces.
func (o *jsonObject) size() int { return o.n + 2 }

func (o *jsonObject) str(name, s string) {
	if s == "" {
		return
	}
	n, ok := jsonString(s)
	o.refused = o.refused || !ok
	o.field(name, n)
}

func (o *jsonObject) bytes(name string, b []byte) {
	if len(b) > 0 {
		o.field(name, 2+base64.StdEncoding.EncodedLen(len(b)))
	}
}

func (o *jsonObject) bool(name string, v bool) {
	if v {
		o.field(name, len("true"))
	}
}

// uint counts a 32-bit unsigned field, which protojson writes as a number.
func (o *jsonObject) uint(name string, v uint64) {
	if v != 0 {
		o.field(name, digits(v))
	}
}

// quoted counts a 64-bit unsigned field, which protojson writes as a string.
func (o *jsonObject) quoted(name string, v uint64) {
	if v != 0 {
		o.field(name, 2+digits(v))
	}
}

// time counts a Timestamp, which protojson writes in RFC 3339, in UTC, with 0, 3, 6 or 9 fractional digits.
func (o *jsonObject) time(name string, t *timestamppb.Timestamp) {
	if t == nil {
		return
	}
	secs, nanos := t.GetSeconds(), t.GetNanos()
	if secs < minTimestamp || secs > maxTimestamp || nanos < 0 || nanos >= 1e9 {
		o.refused = true // protojson refuses it, or (nanos of a whole second) writes the next second: encoding decides

		return
	}
	n := len(`"2006-01-02T15:04:05Z"`)
	switch {
	case nanos == 0:
	case nanos%1e6 == 0:
		n += 4
	case nanos%1e3 == 0:
		n += 7
	default:
		n += 10
	}
	o.field(name, n)
}
