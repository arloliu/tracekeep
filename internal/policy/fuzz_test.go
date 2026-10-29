package policy

import (
	"errors"
	"testing"
)

// A policy file is operator input: parsing never panics, refuses with ErrInvalid, and what it accepts reads back from its canonical bytes.
func FuzzParse(f *testing.F) {
	f.Add(baselineYAML)
	f.Add([]byte("policy_id: p\nversion: 1\n"))
	f.Add([]byte("policy_id: p\nversion: 1\nannotation_rules: [{kind: note, domain: d}]\n"))
	f.Add([]byte("{"))
	f.Fuzz(func(t *testing.T, doc []byte) {
		p, err := Parse(doc)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("Parse failed as %v", err)
			}

			return
		}
		b, sum := p.Canonical()
		back, err := FromCanonical(b)
		if err != nil {
			t.Fatalf("FromCanonical of an accepted policy = %v", err)
		}
		if _, again := back.Canonical(); again != sum {
			t.Fatal("the canonical form does not read back to the same hash")
		}
	})
}
