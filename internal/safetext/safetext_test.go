package safetext

import "testing"

// Text cuts each URL to its scheme and host, keeps the text around it and a URL's trailing punctuation,
// keeps a bracketed IPv6 host whole, and redacts a URL that does not parse.
func TestText(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, want string }{
		{"dial mysql://u:pw-sentinel@db.invalid:3306/tk?tls=q-sentinel: refused", "dial mysql://db.invalid:3306: refused"},
		{"retry (http://u:pw-sentinel@[::1]:9000/p?q-sentinel).", "retry (http://[::1]:9000)."},
		{"bad nats://u:pw-sentinel@host:bad%zz/x", "bad nats://[redacted]"},
		{"no url here", "no url here"},
		{"a nats://a.invalid and https://b.invalid/x?y=q-sentinel", "a nats://a.invalid and https://b.invalid"},
	} {
		if got := Text(tc.in); got != tc.want {
			t.Errorf("Text(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
