package metrics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func body(t *testing.T, s *Server) string {
	t.Helper()
	_, b := get(t, s.Handler(), "/metrics")

	return b
}

// A deferral keeps the time it began while the message is deferred again for the same reason,
// and is dropped once this replica has not deferred it for deferralStale: another replica took it, or decided it.
func TestDeferralsAgeAndExpire(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewIngest(s)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Unix(1000, 0)
	m.deferred.now = func() time.Time { return clock }
	m.Deferred("a", "unknown-capture")
	clock = clock.Add(deferralStale - time.Second)
	m.Deferred("a", "unknown-capture")
	m.Deferred("b", "unknown-capture")
	if b := body(t, s); !strings.Contains(b, `tracekeep_ingest_deferred_oldest_age_seconds{reason="unknown-capture"} 599`) ||
		!strings.Contains(b, `tracekeep_ingest_deferred_messages{reason="unknown-capture"} 2`) {
		t.Fatalf("a redeferral reset the age, or lost a message:\n%s", b)
	}
	clock = clock.Add(deferralStale + time.Second)
	if b := body(t, s); strings.Contains(b, "tracekeep_ingest_deferred_messages") {
		t.Errorf("deferrals not deferred again within deferralStale still counted:\n%s", b)
	}
	if n := len(m.deferred.byID); n != 0 {
		t.Errorf("%d stale deferrals kept", n)
	}
}

type busFunc func(context.Context) (BusState, error)

func (f busFunc) State(ctx context.Context) (BusState, error) { return f(ctx) }

// A refresh whose source fails keeps that source's previous values and counts the failure.
func TestRefreshKeepsTheLastValuesOnFailure(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewIngest(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(t.Context(), nil, busFunc(func(context.Context) (BusState, error) { return BusState{Messages: 7}, nil })); err != nil {
		t.Fatal(err)
	}
	failing := errors.New("bus down")
	if err := m.Refresh(t.Context(), nil, busFunc(func(context.Context) (BusState, error) { return BusState{}, failing })); !errors.Is(err, failing) {
		t.Fatalf("Refresh = %v, want %v", err, failing)
	}
	b := body(t, s)
	for _, want := range []string{"tracekeep_ingest_stream_backlog_messages 7", "tracekeep_ingest_refresh_failures_total 1"} {
		if !strings.Contains(b, want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
}
