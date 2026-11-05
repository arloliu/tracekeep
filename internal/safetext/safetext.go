// Package safetext makes a dependency's error text fit for an answer or a log line (operations.md §10.4 *Logs*):
// every URL it holds cut to its scheme and host.
package safetext

import (
	"net/url"
	"regexp"
	"strings"
)

// urlToken finds a candidate URL in a text: a scheme, ://, and everything up to the next space.
var urlToken = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://\S+`)

// Text is s as an answer or a log line may carry it:
// every URL cut to its scheme and host, without user information, path, query or fragment,
// one that does not parse to its scheme and [redacted], and the rest of the text kept.
// It finds a URL only up to its first space, so an error that quotes a URL whole, a *url.Error, is shown by its structure instead.
func Text(s string) string {
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
