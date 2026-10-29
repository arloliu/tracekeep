package keys

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// Vault's responses are external input: parsing never panics, refuses with a reason and never quotes the response,
// and a success is material read at the version asked for.
func FuzzParseKV(f *testing.F) {
	f.Add([]byte(`{"data":{"data":{"material":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},"metadata":{"version":3}}}`), "3")
	f.Add([]byte(`{"data":{"data":null,"metadata":{"version":1,"deletion_time":"2026-10-29T00:00:00Z"}}}`), "1")
	f.Add([]byte(`{"data":{"data":{"material":42},"metadata":{"version":1}}}`), "")
	f.Add([]byte(`not json SENTINEL`), "")
	f.Fuzz(func(t *testing.T, data []byte, version string) {
		m, got, err := parseKV(data, version)
		if err != nil {
			var pe *ProviderError
			if !errors.As(err, &pe) || (pe.Reason != "missing" && pe.Reason != "malformed") || m != nil {
				t.Fatalf("parseKV failed as %v with %x", err, m)
			}
			if len(data) >= 8 && strings.Contains(err.Error(), string(data)) {
				t.Fatalf("error %q quotes the response", err)
			}

			return
		}
		if got == "" || got == "0" || (version != "" && got != version) || m == nil {
			t.Fatalf("parseKV = %x, %q for version %q", m, got, version)
		}
	})
}

func FuzzParseAuth(f *testing.F) {
	f.Add([]byte(`{"auth":{"client_token":"hvs.x","lease_duration":30,"renewable":true}}`))
	f.Add([]byte(`{"auth":{"client_token":"hvs.x","lease_duration":-1}}`))
	f.Add([]byte(`{"auth":{"client_token":"hvs.x","lease_duration":9223372036854775807}}`))
	f.Add([]byte(`{"auth":null}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		g, err := parseAuth("login", data)
		if err != nil {
			if g != (grant{}) || !strings.Contains(err.Error(), "malformed") {
				t.Fatalf("parseAuth failed as %v with %+v", err, g)
			}
			if len(data) >= 8 && bytes.Contains([]byte(err.Error()), data) {
				t.Fatalf("error %q quotes the response", err)
			}

			return
		}
		if g.token == "" || g.ttl < 0 {
			t.Fatalf("parseAuth = %+v", g)
		}
	})
}
