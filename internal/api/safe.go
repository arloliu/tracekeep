package api

import (
	"errors"
	"net/url"
	"regexp"
	"strings"

	"connectrpc.com/connect"
)

// urlToken finds a candidate URL in a text: a scheme, ://, and everything up to the next space.
var urlToken = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://\S+`)

// safeText is s as an answer or a log line may carry it (operations.md §10.4 *Logs*):
// every URL cut to its scheme and host, without user information, path, query or fragment,
// one that does not parse to its scheme and [redacted], and the rest of the text kept.
func safeText(s string) string {
	return urlToken.ReplaceAllStringFunc(s, func(tok string) string {
		// Punctuation the text put after the URL stays after it.
		trimmed := strings.TrimRight(tok, `.,;:)]}>"'`)
		if strings.Count(trimmed, "[") > strings.Count(trimmed, "]") && len(trimmed) < len(tok) {
			trimmed = tok[:len(trimmed)+1] // the closing bracket of an IPv6 host
		}
		tail := tok[len(trimmed):]
		scheme, _, _ := strings.Cut(trimmed, "://")
		u, err := url.Parse(trimmed)
		if err != nil {
			return scheme + "://[redacted]" + tail
		}

		return u.Scheme + "://" + u.Host + tail
	})
}

// safeAnswer is err, a decided answer, with its message and its cause made safe by safeText; its code, details and marks are kept.
func safeAnswer(err error) error {
	var a *answer
	if err == nil || !errors.As(err, &a) {
		return err
	}
	ce := connect.NewError(a.err.Code(), errors.New(safeText(a.err.Message())))
	for _, d := range a.err.Details() {
		ce.AddDetail(d)
	}
	out := &answer{err: ce, decided: a.decided, gone: a.gone}
	if a.cause != nil {
		out.cause = errors.New(safeText(a.cause.Error()))
	}

	return out
}
