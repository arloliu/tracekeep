package metrics

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

func body(t *testing.T, s *Server) string {
	t.Helper()
	_, b := get(t, s.Handler(), "/metrics")

	return b
}

// A deferral keeps the time it began while the message is deferred again for the same reason, each deferral extending its life
// to deferralGrace past that redelivery; an entry not deferred again in time is dropped, by a later deferral with nothing scraping.
func TestDeferralsAgeAndExpire(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewIngest(s)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(1000, 0)
	clock := t0
	m.deferred.now = func() time.Time { return clock }
	const reason = "unknown-capture"
	m.Deferred("a", reason, time.Minute) // expires at t0+11m
	clock = t0.Add(5 * time.Minute)
	m.Deferred("a", reason, time.Minute) // extended to t0+16m
	m.Deferred("b", reason, time.Minute)
	clock = t0.Add(12 * time.Minute) // past a's first expiry, before its extended one
	if b := body(t, s); !strings.Contains(b, `tracekeep_ingest_deferred_messages{reason="unknown-capture"} 2`) ||
		!strings.Contains(b, `tracekeep_ingest_deferred_oldest_age_seconds{reason="unknown-capture"} 720`) {
		t.Fatalf("a redeferral reset the age, or did not extend the life:\n%s", b)
	}
	m.Deferred("c", reason, 15*time.Minute) // a long redelivery delay: kept until t0+37m
	clock = t0.Add(20 * time.Minute)
	m.Deferred("d", reason, time.Minute) // prunes a and b with no scrape
	if n := len(m.deferred.byID); n != 2 {
		t.Errorf("%d deferrals after the prune, want c and d", n)
	}
	clock = t0.Add(36 * time.Minute)
	if b := body(t, s); !strings.Contains(b, `tracekeep_ingest_deferred_messages{reason="unknown-capture"} 1`) {
		t.Errorf("want c alone, its long redelivery not yet overdue:\n%s", b)
	}
}

type busFunc func(context.Context) (BusState, error)

func (f busFunc) State(ctx context.Context) (BusState, error) { return f(ctx) }

// A refresh whose source fails keeps that source's previous values and counts the failure, the bus's and the catalog's alike.
func TestRefreshKeepsTheLastValuesOnFailure(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		if _, err := c.ActivateConfig(ctx, []byte(`{"classifier":"test","max_frame_len":0}`)); err != nil {
			t.Fatal(err)
		}
		start := &catalog.Identity{Version: catalog.VersionKey{Capture: tracepack.UUID{1}}, Incarnation: 1, StreamSeq: 1}
		desc := &catalog.Descriptor{Bytes: []byte("d"), ToolID: "T1", RecorderInstanceID: tracepack.UUID{2}, Device: "eqp", OriginNS: 1}
		if _, err := c.Admit(ctx, start, desc); err != nil {
			t.Fatal(err)
		}
		s := NewServer()
		m, err := NewIngest(s)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Refresh(ctx, c, busFunc(func(context.Context) (BusState, error) { return BusState{Messages: 7}, nil })); err != nil {
			t.Fatal(err)
		}
		failing := errors.New("bus down")
		cancelled, cancel := context.WithCancel(ctx)
		cancel() // the catalog's reads fail
		err = m.Refresh(cancelled, c, busFunc(func(context.Context) (BusState, error) { return BusState{}, failing }))
		if !errors.Is(err, failing) || !errors.Is(err, context.Canceled) {
			t.Fatalf("Refresh = %v, want both sources' errors", err)
		}
		b := body(t, s)
		for _, want := range []string{
			"tracekeep_ingest_stream_backlog_messages 7", `tracekeep_ingest_captures_open{tool_id="T1"} 1`,
			"tracekeep_ingest_refresh_failures_total 1",
			`tracekeep_ingest_refreshed_timestamp_seconds{source="bus"}`, `tracekeep_ingest_refreshed_timestamp_seconds{source="catalog"}`,
		} {
			if !strings.Contains(b, want) {
				t.Errorf("/metrics lacks %s", want)
			}
		}
	})
}

// A scrape during refreshes sees each source's values from one refresh, never some from one and some from the next.
func TestRefreshPublishesWhole(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewIngest(s)
	if err != nil {
		t.Fatal(err)
	}
	const passes = 200
	var wg sync.WaitGroup
	wg.Go(func() {
		for n := range uint64(passes) {
			_ = m.Refresh(t.Context(), nil, busFunc(func(context.Context) (BusState, error) {
				return BusState{AckPending: n, Messages: n, Bytes: n}, nil
			}))
		}
	})
	value := regexp.MustCompile(`(?m)^tracekeep_ingest_(?:consumer_ack_pending|stream_backlog_messages|stream_backlog_bytes) (\S+)$`)
	for range passes {
		vs := value.FindAllStringSubmatch(body(t, s), -1)
		if len(vs) == 0 {
			continue // no refresh yet
		}
		for _, v := range vs[1:] {
			if v[1] != vs[0][1] {
				t.Fatalf("one scrape mixes refreshes: %v", vs)
			}
		}
	}
	wg.Wait()
}
