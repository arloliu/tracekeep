package logfeed

import (
	"fmt"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/sml"
)

// Built is one record an item becomes, without its seq, which the converter assigns in conversion order, dense per capture (§15.5 *Seq*).
type Built struct {
	Record tracepack.Record
	// Primary reports a data record of a primary message, whose system bytes and TID its reply's device follows (§15.5 *Devices*).
	Primary bool
	// SystemBytes and Session are the record's trusted system bytes and session ID, valid when Trusted; Session is -1 otherwise.
	SystemBytes [4]byte
	Session     int32
	Trusted     bool
}

// Build is the record item becomes (logfeed.md §15.5 *What becomes what*), at its first entry's time:
// a header and a body that parse as exactly one message a reconstructed data record, everything else an annotation.
func Build(it *Item) (Built, error) {
	switch it.Kind {
	case Pair:
		if b, ok := buildData(it.Header, it.Body); ok {
			return b, nil
		}

		return annotation(it.Header, tracepack.AnnotationKindUnparsedEntry, tracepack.DecodeStatusParseFailed, *it.Body.Text)
	case LoneHeader:
		return annotation(it.Header, tracepack.AnnotationKindUnparsedEntry, tracepack.DecodeStatusParseFailed, string(it.Header.Raw))
	case LoneBody, Encrypted:
		at := it.Body
		if it.Header != nil {
			at = it.Header
		}

		return annotation(at, tracepack.AnnotationKindUnparsedEntry, tracepack.DecodeStatusNotApplicable, *it.Body.Text)
	case Unclassified:
		return annotation(it.Body, tracepack.AnnotationKindUnrecognisedLine, tracepack.DecodeStatusNotApplicable, *it.Body.Text)
	case Unrecognised:
		return annotation(it.Body, tracepack.AnnotationKindUnrecognisedLine, tracepack.DecodeStatusNotApplicable, string(it.Body.Raw))
	default:
		return Built{}, fmt.Errorf("logfeed: item kind %d", it.Kind)
	}
}

// buildData parses body's SML as exactly one message and builds its frame (§15.5 *Trust*): a logged data-message header naming the body's
// stream and function gives the frame its W bit, session ID and system bytes, all valid; otherwise only the function is claimed,
// since the stream shares its validity bit with the W the log did not establish.
func buildData(header, body *Entry) (Built, bool) {
	msgs, err := sml.Parse(*body.Text)
	if err != nil || len(msgs) != 1 {
		return Built{}, false
	}
	m := msgs[0]
	b := Built{Primary: m.IsPrimary(), Session: -1}
	validity := tracepack.FieldValidityFunction
	if h, ok := header.header(); ok && h[2]&0x7F == m.Stream() && h[3] == m.Function() {
		item, err := m.Item()
		if err != nil {
			return Built{}, false
		}
		if trusted, err := hsms.NewDataMessageFromHeader(h, item); err == nil {
			m = trusted
			validity = tracepack.FieldValiditySessionID | tracepack.FieldValidityStreamAndW | tracepack.FieldValidityFunction |
				tracepack.FieldValidityPType | tracepack.FieldValiditySType | tracepack.FieldValiditySystemBytes
			b.SystemBytes, b.Trusted, b.Session = m.SystemBytes(), true, int32(m.SessionID())
		}
	}
	dir := tracepack.DirEquipmentToHost
	if header.Dir == DirSent {
		dir = tracepack.DirHostToEquipment
	}
	b.Record = tracepack.Record{
		TSUTCNs: header.UTCNs, Kind: tracepack.KindData, Dir: dir, Fidelity: tracepack.FidelityReconstructed,
		DecodeStatus: tracepack.DecodeStatusReconstructedOK, Quality: quality(header), FieldValidity: validity, Payload: frame(m),
	}

	return b, true
}

// frame is m's HSMS frame, its length prefix included.
func frame(m *hsms.DataMessage) []byte { return m.ToBytes() }

// annotation is an annotation of kind holding text, at e's time, with status: parse-failed for an entry that did not parse.
func annotation(e *Entry, kind tracepack.AnnotationKind, status tracepack.DecodeStatus, text string) (Built, error) {
	a := tracepack.Annotation{AnnotationKind: kind, Text: &text}
	payload, err := a.MarshalBinary()
	if err != nil {
		return Built{}, fmt.Errorf("logfeed: annotation: %w", err)
	}

	return Built{Record: tracepack.Record{
		TSUTCNs: e.UTCNs, Kind: tracepack.KindAnnotation, Fidelity: tracepack.FidelityNotApplicable,
		DecodeStatus: status, Quality: quality(e), Payload: payload,
	}}, nil
}

func quality(e *Entry) tracepack.Quality {
	if e.Uncertain {
		return tracepack.QualityOrderingUncertain
	}

	return 0
}
