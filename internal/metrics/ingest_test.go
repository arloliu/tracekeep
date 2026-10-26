package metrics

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	m, err := NewIngest(s, nil)
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
		m, err := NewIngest(s, nil)
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

// A message held in a batch keeps its deferral while it is held, past the expiry its nak set, its age kept;
// once no longer held, its batch abandoned, it expires deferralGrace after the last hold. Holding a message never deferred adds nothing.
func TestDeferralsHeldWhileBatched(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewIngest(s, nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(1000, 0)
	clock := t0
	m.deferred.now = func() time.Time { return clock }
	m.Deferred("a", "write-failure", time.Minute) // expires at t0+11m unless held
	clock = t0.Add(5 * time.Minute)
	m.Held([]string{"a", "never-deferred"}) // until t0+15m
	clock = t0.Add(14 * time.Minute)
	if b := body(t, s); !strings.Contains(b, `tracekeep_ingest_deferred_messages{reason="write-failure"} 1`) ||
		!strings.Contains(b, `tracekeep_ingest_deferred_oldest_age_seconds{reason="write-failure"} 840`) {
		t.Fatalf("a held deferral expired, or lost its age:\n%s", b)
	}
	clock = t0.Add(16 * time.Minute)
	if b := body(t, s); strings.Contains(b, "tracekeep_ingest_deferred_messages") {
		t.Errorf("a deferral no longer held still counted:\n%s", b)
	}
}

// heldCatalog holds its first read at a barrier: reading closes when it is reached, release lets it go on.
type heldCatalog struct {
	*catalog.Catalog
	reading, release chan struct{}
}

func (h heldCatalog) ParkedCounts(ctx context.Context) (map[string]int, error) {
	close(h.reading)
	<-h.release

	return h.Catalog.ParkedCounts(ctx)
}

// A refresh publishes each source whole, under a controlled schedule: while the bus's read is held, a scrape sees the previous refresh
// of both sources; while the catalog's read is held, the new bus values and the previous catalog values; once it returns, the new of both.
func TestRefreshPublishesWhole(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		if _, err := c.ActivateConfig(ctx, []byte(`{"classifier":"test","max_frame_len":0}`)); err != nil {
			t.Fatal(err)
		}
		admit := func(capture byte) {
			start := &catalog.Identity{Version: catalog.VersionKey{Capture: tracepack.UUID{capture}}, Incarnation: 1, StreamSeq: int64(capture)}
			desc := &catalog.Descriptor{Bytes: []byte("d"), ToolID: "T1", RecorderInstanceID: tracepack.UUID{capture, 1}, Device: "eqp", OriginNS: 1}
			if _, err := c.Admit(ctx, start, desc); err != nil {
				t.Fatal(err)
			}
		}
		s := NewServer()
		m, err := NewIngest(s, nil)
		if err != nil {
			t.Fatal(err)
		}
		admit(1)
		if err := m.Refresh(ctx, c, busFunc(func(context.Context) (BusState, error) { return BusState{AckPending: 1, Messages: 1, Bytes: 1}, nil })); err != nil {
			t.Fatal(err)
		}
		admit(2)
		busReading, busRelease := make(chan struct{}), make(chan struct{})
		held := heldCatalog{Catalog: c, reading: make(chan struct{}), release: make(chan struct{})}
		done := make(chan error, 1)
		go func() {
			done <- m.Refresh(ctx, held, busFunc(func(context.Context) (BusState, error) {
				close(busReading)
				<-busRelease

				return BusState{AckPending: 2, Messages: 2, Bytes: 2}, nil
			}))
		}()
		want := func(stage string, bus, captures int) {
			t.Helper()
			b := body(t, s)
			for _, line := range []string{
				fmt.Sprintf("tracekeep_ingest_consumer_ack_pending %d", bus), fmt.Sprintf("tracekeep_ingest_stream_backlog_messages %d", bus),
				fmt.Sprintf("tracekeep_ingest_stream_backlog_bytes %d", bus),
				fmt.Sprintf(`tracekeep_ingest_captures_open{tool_id="T1"} %d`, captures), `tracekeep_ingest_open_gaps{tool_id="T1"} 0`,
			} {
				if !strings.Contains(b, line) {
					t.Errorf("%s: /metrics lacks %s", stage, line)
				}
			}
		}
		<-busReading
		want("the bus's read held", 1, 1)
		close(busRelease)
		<-held.reading
		want("the catalog's read held", 2, 1)
		close(held.release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		want("the refresh done", 2, 2)
	})
}
