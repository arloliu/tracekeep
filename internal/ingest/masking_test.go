package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/pem"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/classify"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/policy"
)

// maskingKeys declares d-1 and m-1 in files of synthetic material.
func maskingKeys(t *testing.T, c *catalog.Catalog) *keys.Keys {
	t.Helper()

	return maskingKeysAt(t, c, t.TempDir(), nil)
}

// maskingKeysAt is maskingKeys with the key files under dir, d-1 at dir/d-1 and m-1 at dir/m-1, read through providers when given.
func maskingKeysAt(t *testing.T, c *catalog.Catalog, dir string, providers map[string]keys.Provider) *keys.Keys {
	t.Helper()
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
		p := maskingPipeline(t, c, maskingKeys(t, c))
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

// A record masking cannot screen is nak'd; one whose key no longer matches its binding parks as key-refused under the key-binding-mismatch incident;
// one whose key is otherwise refused parks under key-refused; one whose key is unavailable is nak'd; none is batched.
// Each keeps why it failed, which the alert logs and counts, and none of that names material or content.
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
		setup     func(t *testing.T, c *catalog.Catalog, dir string)
		carrier   *maskingtest.Carrier
		want      Decision
		incident  string
		metric    string
	}{
		"cannot screen": {carrier: &deep, want: Decision{Action: ActNak, Reason: ReasonCannotScreen, Alert: true}, incident: ReasonCannotScreen,
			metric: "tracekeep_ingest_cannot_screen_total 1"},
		"binding mismatch": {
			// d-1 bound at its own file with another material's fingerprint: the file's material no longer matches.
			setup: func(t *testing.T, c *catalog.Catalog, dir string) {
				t.Helper()
				if _, err := c.BindKey(t.Context(), catalog.KeyBinding{KeyID: "d-1", Kind: catalog.KindDigest, ProviderRef: "file:" + filepath.Join(dir, "d-1"), Fingerprint: keys.Fingerprint([]byte("other material, 32 bytes long..."))}); err != nil {
					t.Fatal(err)
				}
			},
			carrier: &ppbody, want: Decision{Action: ActPark, Reason: ReasonKeyRefused, Alert: true}, incident: IncidentKeyBindingMismatch,
			metric: "tracekeep_ingest_key_binding_mismatches_total 1",
		},
		"key missing": {
			setup: func(t *testing.T, _ *catalog.Catalog, dir string) {
				t.Helper()
				_ = os.Remove(filepath.Join(dir, "d-1"))
			},
			carrier: &ppbody, want: Decision{Action: ActPark, Reason: ReasonKeyRefused, Alert: true}, incident: ReasonKeyRefused,
		},
		"key unavailable": {
			providers: map[string]keys.Provider{"fake": failing{&keys.ProviderError{Provider: "fake", Op: "read", Reason: "unreachable"}}},
			carrier:   &ppbody, want: Decision{Action: ActNak, Reason: ReasonKeyUnavailable, Alert: true}, incident: ReasonKeyUnavailable,
		},
	} {
		catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
			dir := t.TempDir()
			k := maskingKeysAt(t, c, dir, tc.providers)
			if tc.setup != nil {
				tc.setup(t, c, dir)
			}
			p := maskingPipeline(t, c, k)
			srv := metrics.NewServer()
			m, err := metrics.NewIngest(srv, nil)
			if err != nil {
				t.Fatal(err)
			}
			p.x.Metrics = m
			capture := tracepack.UUID{0x52}
			p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
			d, dec := dispose(t, p.x, inline(t, capture, carrierRecord(tc.carrier, 1), nil))
			if dec != tc.want || d.Incident != tc.incident || d.MaskErr == nil {
				t.Errorf("%s: disposed %+v, incident %q, error %v; want %+v under %q", name, dec, d.Incident, d.MaskErr, tc.want, tc.incident)
			}
			if d.Redaction != nil {
				t.Errorf("%s: entries kept on a delivery that was not masked", name)
			}
			var log bytes.Buffer
			(&Ingester{Log: slog.New(slog.NewTextHandler(&log, nil))}).alert(d, dec)
			if !strings.Contains(log.String(), "incident="+tc.incident) || !strings.Contains(log.String(), "error=") {
				t.Errorf("%s: the alert logged %q, want the incident and the error", name, log.String())
			}
			for _, secret := range append(tc.carrier.Secrets, bytes.Repeat([]byte("d"), 32), []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("d"), 32)))) {
				if bytes.Contains(log.Bytes(), secret) {
					t.Errorf("%s: the alert logged %q", name, secret)
				}
			}
			if tc.metric != "" && !strings.Contains(scrapeIngest(t, srv), tc.metric) {
				t.Errorf("%s: no %s", name, tc.metric)
			}
		})
	}
}

// A failing Vault, through the keys module, nak's the record as key-unavailable,
// and neither the alert's log line nor the metrics hold the token, the secret id or the server's response body.
func TestVaultFailureHoldsNoSecrets(t *testing.T) {
	t.Parallel()
	const token, secretID, body = "hvs.TOKEN-SENTINEL", "SECRET-ID-SENTINEL", "BODY-SENTINEL"
	// Logins succeed and every read answers a sealed server's 503 with a sentinel body.
	vault := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/login") {
			_, _ = w.Write([]byte(`{"auth":{"client_token":"` + token + `","lease_duration":60,"renewable":true}}`))
			return
		}
		http.Error(w, `{"errors":["`+body+`"]}`, http.StatusServiceUnavailable)
	}))
	t.Cleanup(vault.Close)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: vault.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	b := policy.Baseline()
	carriers, err := maskingtest.Carriers(&b)
	if err != nil {
		t.Fatal(err)
	}
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		srv := metrics.NewServer()
		km, err := metrics.NewKeys(srv)
		if err != nil {
			t.Fatal(err)
		}
		v, err := keys.NewVault(keys.VaultConfig{
			Address: vault.URL, CAFile: ca, Mount: "secret", Timeout: 5 * time.Second,
			Method: keys.VaultAppRole, RoleID: "role-1", SecretID: secretID, Metrics: km,
		})
		if err != nil {
			t.Fatal(err)
		}
		k, err := keys.New(keys.Config{
			Declared: map[string]keys.Declaration{
				"d-1": {Provider: keys.ProviderVault, Ref: "tk/d-1", Kind: catalog.KindDigest},
				"m-1": {Provider: keys.ProviderVault, Ref: "tk/m-1", Kind: catalog.KindMaster},
			},
			Providers: map[string]keys.Provider{keys.ProviderVault: v},
			Metrics:   km,
		}, c)
		if err != nil {
			t.Fatal(err)
		}
		p := maskingPipeline(t, c, k)
		if p.x.Metrics, err = metrics.NewIngest(srv, nil); err != nil {
			t.Fatal(err)
		}
		capture := tracepack.UUID{0x54}
		p.feed(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")))
		d, dec := dispose(t, p.x, inline(t, capture, carrierRecord(&carriers[0], 1), nil))
		want := Decision{Action: ActNak, Reason: ReasonKeyUnavailable, Alert: true}
		if dec != want || d.Incident != ReasonKeyUnavailable || d.MaskErr == nil {
			t.Fatalf("disposed %+v, incident %q, error %v; want %+v", dec, d.Incident, d.MaskErr, want)
		}
		var log bytes.Buffer
		(&Ingester{Log: slog.New(slog.NewTextHandler(&log, nil))}).alert(d, dec)
		if !strings.Contains(log.String(), "status 503") {
			t.Errorf("the alert logged %q, want the provider's status", log.String())
		}
		scrape := scrapeIngest(t, srv)
		for _, want := range []string{
			`tracekeep_key_provider_errors_total{provider="vault",reason="server"}`,
			`outcome="unavailable"`,
			`tracekeep_key_credential_expiry_timestamp{provider="vault"}`,
		} {
			if !strings.Contains(scrape, want) {
				t.Errorf("no %s in the metrics", want)
			}
		}
		for _, s := range []string{"TOKEN-SENTINEL", secretID, body} {
			if strings.Contains(log.String(), s) {
				t.Errorf("the alert logged %s: %q", s, log.String())
			}
			if strings.Contains(scrape, s) {
				t.Errorf("the metrics hold %s", s)
			}
		}
	})
}

// scrapeIngest is srv's metrics in the text format.
func scrapeIngest(t *testing.T, srv *metrics.Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	return rec.Body.String()
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
