package api

import (
	"strings"
	"unicode/utf8"

	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/sml"
)

// smlLimit is the most bytes of SML a record carries (api.md §10.1 *Items*).
const smlLimit = 64 << 10

// renderSML renders a data record's frame as SML: valid UTF-8, each invalid byte written as U+FFFD,
// cut at the last character boundary at or below smlLimit; a frame that does not render answers the error's text instead.
func renderSML(frame []byte) (text string, truncated bool, failure string) {
	m, err := hsms.DecodeHSMSMessage(frame)
	if err != nil {
		return "", false, err.Error()
	}
	dm, ok := m.ToDataMessage()
	if !ok {
		return "", false, "not a data message"
	}
	s, err := sml.EncodeMessage(dm)
	if err != nil {
		return "", false, err.Error()
	}

	return validUTF8(s, smlLimit)
}

// validUTF8 is s with each invalid byte written as U+FFFD, cut at the last character boundary at or below limit bytes.
func validUTF8(s string, limit int) (string, bool, string) {
	var b strings.Builder
	for i := 0; i < len(s); {
		r, n := utf8.DecodeRuneInString(s[i:]) // an invalid byte decodes as U+FFFD of width 1
		if b.Len()+utf8.RuneLen(r) > limit {
			return b.String(), true, ""
		}
		b.WriteRune(r)
		i += n
	}

	return b.String(), false, ""
}
