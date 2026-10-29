package maskingtest

import (
	"bytes"
	"errors"
	"testing"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/redact"

	"github.com/arloliu/tracekeep/internal/policy"
)

type keys struct{}

func (keys) DigestKey(string) ([]byte, error) { return bytes.Repeat([]byte{1}, 32), nil }
func (keys) EncryptionKey(tracepack.UUID, string) ([]byte, error) {
	return bytes.Repeat([]byte{2}, 64), nil
}

func record(seq uint64, c *Carrier) *tracepack.Record {
	r := &tracepack.Record{Seq: seq, Kind: c.Kind, Payload: c.Payload, Epoch: 1}
	if c.Kind == tracepack.KindData {
		r.Dir, r.Fidelity, r.DecodeStatus = tracepack.DirHostToEquipment, tracepack.FidelityWireExact, tracepack.DecodeStatusOK
		r.SetCapturedFieldValidity()
	} else {
		r.Dir, r.Fidelity, r.DecodeStatus = tracepack.DirLocal, tracepack.FidelityNotApplicable, tracepack.DecodeStatusNotApplicable
	}

	return r
}

// Every baseline carrier masks rule by rule, one entry with an item path per rule, the annotations by kind, and no secret survives,
// under encryption and without.
func TestBaselineCarriersMask(t *testing.T) {
	t.Parallel()
	b := policy.Baseline()
	cs, err := Carriers(&b)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(cs); n != 24 {
		t.Errorf("%d carriers, want 21 data records (one per stream and function) and 3 annotations", n)
	}
	for _, enc := range []string{"", "m-1"} {
		compiled, err := b.Compile("d-1", enc)
		if err != nil {
			t.Fatal(err)
		}
		for i := range cs {
			c := &cs[i]
			masked, entries, err := compiled.Mask(tracepack.UUID{1}, record(uint64(i), c), false, keys{})
			if err != nil {
				t.Fatalf("%s: %v", c.Name, err)
			}
			want := max(len(c.Rules), 1)
			if len(entries) < want {
				t.Errorf("%s: %d entries, want at least %d", c.Name, len(entries), want)
			}
			for _, e := range entries {
				if c.Kind == tracepack.KindData && e.ItemPath == nil {
					t.Errorf("%s: masked whole, want by item", c.Name)
				}
			}
			for _, s := range c.Secrets {
				if bytes.Contains(masked.Payload, s) {
					t.Errorf("%s (encryption %q): the masked payload holds %q", c.Name, enc, s)
				}
				if !bytes.Contains(c.Payload, s) {
					t.Errorf("%s: the carrier does not hold its secret %q", c.Name, s)
				}
			}
		}
	}
}

// The whole-text carrier masks whole; the too-deep one cannot be screened.
func TestFallbackCarriers(t *testing.T) {
	t.Parallel()
	b := policy.Baseline()
	compiled, err := b.Compile("d-1", "m-1")
	if err != nil {
		t.Fatal(err)
	}
	w := WholeText()
	masked, entries, err := compiled.Mask(tracepack.UUID{1}, record(1, &w), false, keys{})
	if err != nil || len(entries) != 1 || entries[0].ItemPath != nil || bytes.Contains(masked.Payload, w.Secrets[0]) {
		t.Errorf("the whole-text carrier = %d entries, %v", len(entries), err)
	}
	d := TooDeep()
	if _, _, err := compiled.Mask(tracepack.UUID{1}, record(2, &d), false, keys{}); !errors.Is(err, redact.ErrCannotScreen) {
		t.Errorf("the too-deep carrier = %v, want ErrCannotScreen", err)
	}
}
