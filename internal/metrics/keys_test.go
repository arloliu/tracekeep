package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestKeys(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewKeys(s)
	if err != nil {
		t.Fatal(err)
	}
	m.Resolution("digest", "cached")
	m.Resolution("digest", "cached")
	m.Resolution("master", "refused")
	m.ProviderCall("vault", "read", "", time.Millisecond)
	m.ProviderCall("vault", "login", "auth", time.Millisecond)
	m.CredentialExpiry("vault", time.Unix(1_800_000_000, 0))
	(*Keys)(nil).Resolution("digest", "cached")
	b := body(t, s)
	for _, want := range []string{
		`tracekeep_key_resolutions_total{kind="digest",outcome="cached"} 2`,
		`tracekeep_key_resolutions_total{kind="master",outcome="refused"} 1`,
		`tracekeep_key_provider_errors_total{provider="vault",reason="auth"} 1`,
		`tracekeep_key_provider_seconds_count{op="read",provider="vault"} 1`,
		`tracekeep_key_credential_expiry_timestamp{provider="vault"} 1.8e+09`,
	} {
		if !strings.Contains(b, want) {
			t.Errorf("no %s in\n%s", want, b)
		}
	}
}
