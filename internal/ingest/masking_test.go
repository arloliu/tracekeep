package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/classify"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/policy"
)

// maskingKeys declares d-1 and m-1 in files of synthetic material, through providers, when given, in place of the file provider.
func maskingKeys(t *testing.T, c *catalog.Catalog, providers map[string]keys.Provider) *keys.Keys {
	t.Helper()
	dir := t.TempDir()
	declared := map[string]keys.Declaration{}
	for id, kind := range map[string]string{"d-1": catalog.KindDigest, "m-1": catalog.KindMaster} {
		path := filepath.Join(dir, id)
		if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte(id[:1]), 32))), 0o600); err != nil {
			t.Fatal(err)
		}
		provider := keys.ProviderFile
		if providers != nil {
			provider = "fake"
		}
		declared[id] = keys.Declaration{Provider: provider, Ref: path, Kind: kind}
	}
	k, err := keys.New(keys.Config{Declared: declared, Providers: providers}, c)
	if err != nil {
		t.Fatal(err)
	}

	return k
}

// maskingConfig is the baseline's masking configuration of this binary's classifier, under d-1 and m-1.
func maskingConfig(t *testing.T) Config {
	t.Helper()
	p := policy.Baseline()
	cfg := Config{
		Classifier: classify.NewNamed(classify.Name, maxFrameLen), Name: classify.Name, MaxFrameLen: maxFrameLen,
		Policy: &p, DigestKey: KeyRef{ID: "d-1"}, EncryptionKey: KeyRef{ID: "m-1"}, FormatMinor: 1,
	}
	var err error
	if cfg.Masking, err = p.Compile("d-1", "m-1"); err != nil {
		t.Fatal(err)
	}

	return cfg
}

// maskingPipeline is newPipeline under a masking configuration.
func maskingPipeline(t *testing.T, c *catalog.Catalog, k MaskKeys) *pipeline {
	t.Helper()
	p := newPipeline(t, c)
	for id := range p.x.Configs {
		p.x.Configs[id] = maskingConfig(t)
	}
	p.x.Keys = k

	return p
}

func carrierRecord(c *maskingtest.Carrier, seq uint64) tracepack.Record {
	r := tracepack.Record{Seq: seq, TSUTCNs: now + int64(seq), Kind: c.Kind, Payload: c.Payload}
	if c.Kind == tracepack.KindData {
		r.Dir, r.Fidelity = tracepack.DirHostToEquipment, tracepack.FidelityWireExact
	} else {
		r.Dir, r.Fidelity = tracepack.DirLocal, tracepack.FidelityNotApplicable
	}

	return r
}

// Every baseline carrier, inline and claim-check, is masked before it is batched: the pack's records hold no secret,
// carry quality.redacted, and the pack names the configuration's redaction policy and every entry, in seq order.
func TestMaskingAtIngest(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := maskingPipeline(t, c, maskingKeys(t, c, nil))
		capture := tracepack.UUID{0x51}
		b := policy.Baseline()
		carriers, err := maskingtest.Carriers(&b)
		if err != nil {
			t.Fatal(err)
		}
		p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		for i := range carriers {
			r := carrierRecord(&carriers[i], uint64(i+1))
			m := inline(t, capture, r, nil)
			if i%2 == 1 {
				m = claimCheck(t, p.cl, capture, r, nil)
			}
			d, dec := dispose(t, p.x, m)
			if dec.Action != ActBatch || d.Claim != nil {
				t.Fatalf("%s disposed %+v, claim bytes kept %v", carriers[i].Name, dec, d.Claim != nil)
			}
			p.b.Add(Entry{D: d, Msg: &msg{}}, time.Unix(0, now))
		}
		p.flushAll(t)
		keys := storedKeys(t, p.st)
		if len(keys) != 1 {
			t.Fatalf("packs %q, want one segment", keys)
		}
		recs := records(t, p.st, keys[0])
		if len(recs) != len(carriers)+1 {
			t.Fatalf("%d records, want the start and every carrier", len(recs))
		}
		for i := range carriers {
			r := recs[i+1]
			for _, s := range carriers[i].Secrets {
				if bytes.Contains(r.Payload, s) {
					t.Errorf("%s: the stored record holds %q", carriers[i].Name, s)
				}
			}
			if !r.Quality.Has(tracepack.QualityRedacted) {
				t.Errorf("%s: the stored record is not redacted", carriers[i].Name)
			}
		}
		meta := header(t, p, keys[0]).Meta
		rp := meta.RedactionPolicy
		if rp == nil || rp.PolicyID != "baseline" || rp.PolicyVersion != 1 || rp.KeyID != "d-1" || rp.EncryptionKeyID == nil || *rp.EncryptionKeyID != "m-1" {
			t.Errorf("redaction policy = %+v", rp)
		}
		if len(meta.Redaction) < len(b.ItemRules)+len(b.AnnotationRules) {
			t.Errorf("%d redaction entries, want one per rule at least", len(meta.Redaction))
		}
		for i := 1; i < len(meta.Redaction); i++ {
			if meta.Redaction[i].Seq < meta.Redaction[i-1].Seq {
				t.Fatalf("redaction entries out of seq order at %d", i)
			}
		}
		verify(t, p.st, keys[0])
		if h := header(t, p, keys[0]); h.FormatMinor < 1 {
			t.Errorf("format_minor %d, want 1 at least", h.FormatMinor)
		}
	})
}

func header(t *testing.T, p *pipeline, key string) *tracepack.PackHeader {
	t.Helper()
	obj, err := p.st.Open(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	r, err := tracepack.Open(t.Context(), obj, obj.Size(), tracepack.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}

	return r.Header()
}

// failing is a key provider that is down, or that serves other material than the binding's.
type failing struct{ err error }

func (f failing) Material(context.Context, string, string) ([]byte, string, error) {
	return nil, "", f.err
}

// A record masking cannot screen is nak'd; one whose key is refused parks; one whose key is unavailable is nak'd; none is batched.
func TestMaskingFailures(t *testing.T) {
	t.Parallel()
	deep := maskingtest.TooDeep()
	b := policy.Baseline()
	carriers, err := maskingtest.Carriers(&b)
	if err != nil {
		t.Fatal(err)
	}
	ppbody := carriers[0]
	for name, tc := range map[string]struct {
		providers map[string]keys.Provider
		bind      bool // bind d-1 to other material first, so the provider's material mismatches it
		carrier   *maskingtest.Carrier
		want      Decision
	}{
		"cannot screen": {carrier: &deep, want: Decision{Action: ActNak, Reason: ReasonCannotScreen, Alert: true}},
		"key refused":   {bind: true, carrier: &ppbody, want: Decision{Action: ActPark, Reason: ReasonKeyRefused, Alert: true}},
		"key unavailable": {
			providers: map[string]keys.Provider{"fake": failing{&keys.ProviderError{Provider: "fake", Op: "read", Reason: "unreachable"}}},
			carrier:   &ppbody, want: Decision{Action: ActNak, Reason: ReasonKeyUnavailable, Alert: true},
		},
	} {
		catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
			if tc.bind {
				if _, err := c.BindKey(t.Context(), catalog.KeyBinding{KeyID: "d-1", Kind: catalog.KindDigest, ProviderRef: "file:/elsewhere", Fingerprint: "other"}); err != nil {
					t.Fatal(err)
				}
			}
			p := maskingPipeline(t, c, maskingKeys(t, c, tc.providers))
			capture := tracepack.UUID{0x52}
			p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
			d, dec := dispose(t, p.x, inline(t, capture, carrierRecord(tc.carrier, 1), nil))
			if dec != tc.want {
				t.Errorf("%s: disposed %+v, want %+v", name, dec, tc.want)
			}
			if d.Redaction != nil {
				t.Errorf("%s: entries kept on a delivery that was not masked", name)
			}
		})
	}
}

// A policy-less configuration leaves a record as received and writes no redaction metadata.
func TestPolicyLessConfigurationMasksNothing(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		capture := tracepack.UUID{0x53}
		b := policy.Baseline()
		carriers, err := maskingtest.Carriers(&b)
		if err != nil {
			t.Fatal(err)
		}
		p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		p.feed(t, inline(t, capture, carrierRecord(&carriers[0], 1), nil))
		p.flushAll(t)
		keys := storedKeys(t, p.st)
		recs := records(t, p.st, keys[0])
		if !bytes.Equal(recs[1].Payload, carriers[0].Payload) || recs[1].Quality.Has(tracepack.QualityRedacted) {
			t.Error("a policy-less configuration changed the record")
		}
		if m := header(t, p, keys[0]).Meta; m.RedactionPolicy != nil || len(m.Redaction) != 0 {
			t.Errorf("redaction metadata under a policy-less configuration: %+v, %d", m.RedactionPolicy, len(m.Redaction))
		}
	})
}
