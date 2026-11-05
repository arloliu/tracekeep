package safetext

import "testing"

// Text cuts each URL to its scheme and host, keeps the text around it and a URL's trailing punctuation,
// keeps a bracketed IPv6 host whole, redacts a URL that does not parse, quoted or not, and ends a URL at its first space.
func TestText(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, want string }{
		{"dial mysql://u:pw-sentinel@db.invalid:3306/tk?tls=q-sentinel: refused", "dial mysql://db.invalid:3306: refused"},
		{"retry (http://u:pw-sentinel@[::1]:9000/p?q-sentinel).", "retry (http://[::1]:9000)."},
		{"bad nats://u:pw-sentinel@host:bad%zz/x", "bad nats://[redacted]"},
		{"no url here", "no url here"},
		{"a nats://a.invalid and https://b.invalid/x?y=q-sentinel", "a nats://a.invalid and https://b.invalid"},
		// A quoted URL that does not parse: redacted inside its quotes, the cause after it kept.
		{`parse "nats://u:pw-sentinel@a.invalid:bad?token=q-sentinel": invalid port ":bad" after host`, `parse "nats://[redacted]": invalid port ":bad" after host`},
		// A URL ends at its first space for Text, which keeps the rest: an error quoting a URL whole is shown by its structure instead.
		{`dial "nats://u:pw-sentinel@a.invalid/p b?q": refused`, `dial "nats://a.invalid b?q": refused`},
		{`from "https://u:p-sentinel@a.invalid/x?k=q-sentinel", to tls://b.invalid:4443/?t=q-sentinel; ws://[::1]:80/y then`, `from "https://a.invalid", to tls://b.invalid:4443; ws://[::1]:80 then`},
	} {
		if got := Text(tc.in); got != tc.want {
			t.Errorf("Text(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
