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

// Counters an alert reads by their increase are exposed from zero, before their first event (operations.md §10.4):
// every key kind's outcomes, every merge outcome, and a tool's declared losses once it writes a segment.
func TestAlertedCountersStartAtZero(t *testing.T) {
	t.Parallel()
	s := NewServer()
	if _, err := NewKeys(s); err != nil {
		t.Fatal(err)
	}
	if _, err := NewCompact(s); err != nil {
		t.Fatal(err)
	}
	in, err := NewIngest(s, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	in.Registered(time.Millisecond, "T-1", 10, false)
	b := body(t, s)
	for _, want := range []string{
		`tracekeep_key_resolutions_total{kind="digest",outcome="refused"} 0`,
		`tracekeep_key_resolutions_total{kind="session",outcome="unavailable"} 0`,
		`tracekeep_compact_merges_total{outcome="refused"} 0`,
		`tracekeep_ingest_declared_losses_total{tool_id="T-1"} 0`,
	} {
		if !strings.Contains(b, want) {
			t.Errorf("no %s in\n%s", want, b)
		}
	}
}
