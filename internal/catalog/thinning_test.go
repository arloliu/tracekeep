package catalog

import (
	"fmt"
	"testing"
)

// A thinning entry's source gives its sampling rule only when it parses as the SDK's convention;
// any other text leaves the parameters absent.
func TestSamplingRuleOfASource(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, source string
		n, ms        uint64
		ok           bool
	}{
		{"the defaults", "fdc interval polling by request origin; keep_every_n=60; keep_interval_ms=60000; abnormal=kept", 60, 60000, true},
		{"other values", "fdc interval polling by request origin; keep_every_n=3; keep_interval_ms=1500; abnormal=kept", 3, 1500, true},
		{"spaces and an unknown key", "x;keep_interval_ms = 7 ;  keep_every_n=2;later=1", 2, 7, true},
		{"free text", "fdc", 0, 0, false},
		{"empty", "", 0, 0, false},
		{"no interval", "keep_every_n=60", 0, 0, false},
		{"no count", "keep_interval_ms=60000", 0, 0, false},
		{"a count of zero", "keep_every_n=0; keep_interval_ms=60000", 0, 0, false},
		{"a negative count", "keep_every_n=-1; keep_interval_ms=60000", 0, 0, false},
		{"a value not a number", "keep_every_n=sixty; keep_interval_ms=60000", 0, 0, false},
		{"a key twice", "keep_every_n=60; keep_every_n=30; keep_interval_ms=60000", 0, 0, false},
		{"an overflow", "keep_every_n=18446744073709551616; keep_interval_ms=1", 0, 0, false},
	}
	for _, c := range cases {
		n, ms := samplingRule(c.source)
		if c.ok != (n != nil) || c.ok != (ms != nil) {
			t.Errorf("%s: parameters %v, %v, want present %t", c.name, n, ms, c.ok)
			continue
		}
		if c.ok && (*n != c.n || *ms != c.ms) {
			t.Errorf("%s: parameters %d, %d, want %d, %d", c.name, *n, *ms, c.n, c.ms)
		}
	}
}

// A source is a producer's text: parsing never panics, gives both parameters or neither, never a count of zero,
// and what it accepts reads back from the SDK's own spelling of the same rule.
func FuzzSamplingRule(f *testing.F) {
	f.Add("fdc interval polling by request origin; keep_every_n=60; keep_interval_ms=60000; abnormal=kept")
	f.Add("keep_every_n=1;keep_interval_ms=0")
	f.Add("keep_every_n=60; keep_every_n=60; keep_interval_ms=1")
	f.Add(";;=;")
	f.Fuzz(func(t *testing.T, source string) {
		n, ms := samplingRule(source)
		if (n == nil) != (ms == nil) {
			t.Fatalf("samplingRule(%q) = %v, %v: one parameter without the other", source, n, ms)
		}
		if n == nil {
			return
		}
		if *n == 0 {
			t.Fatalf("samplingRule(%q) accepted a count of zero", source)
		}
		spelled := fmt.Sprintf("fdc interval polling by request origin; keep_every_n=%d; keep_interval_ms=%d; abnormal=kept", *n, *ms)
		if n2, ms2 := samplingRule(spelled); n2 == nil || *n2 != *n || *ms2 != *ms {
			t.Fatalf("samplingRule(%q) does not read back from %q", source, spelled)
		}
	})
}
