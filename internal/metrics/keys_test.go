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

// The events an alert reads are stamped with their time, whatever their counters' history (operations.md §10.4):
// a key refused or unavailable, a merge refused, and a tool's declared loss, which can come before any of its segments.
func TestAlertedEventsStamped(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_800_000_000, 0)
	now := func() time.Time { return at }
	s := NewServer()
	keys, err := NewKeys(s)
	if err != nil {
		t.Fatal(err)
	}
	keys.now = now
	compact, err := NewCompact(s)
	if err != nil {
		t.Fatal(err)
	}
	compact.now = now
	in, err := NewIngest(s, now)
	if err != nil {
		t.Fatal(err)
	}
	keys.Resolution("digest", "refused")
	keys.Resolution("session", "unavailable")
	keys.Resolution("master", "cached")
	compact.Merges(0, 0, 0, 0, 1)
	in.Lost("T-1")
	b := body(t, s)
	for _, want := range []string{
		`tracekeep_key_last_problem_timestamp_seconds{kind="digest",outcome="refused"} 1.8e+09`,
		`tracekeep_key_last_problem_timestamp_seconds{kind="session",outcome="unavailable"} 1.8e+09`,
		`tracekeep_compact_last_merge_timestamp_seconds{outcome="refused"} 1.8e+09`,
		`tracekeep_ingest_last_declared_loss_timestamp_seconds{tool_id="T-1"} 1.8e+09`,
	} {
		if !strings.Contains(b, want) {
			t.Errorf("no %s in\n%s", want, b)
		}
	}
	for _, unwanted := range []string{`outcome="cached"} 1.8e+09`, `last_merge_timestamp_seconds{outcome="merged"}`} {
		if strings.Contains(b, unwanted) {
			t.Errorf("%s stamped:\n%s", unwanted, b)
		}
	}
}
