package ingest

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
)

func scrape(t *testing.T, s *metrics.Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)

	return string(body)
}

// trafficState is the bus state Refresh reads, from the replica's consumer and stream.
type trafficState struct{ r *replica }

func (s trafficState) State(ctx context.Context) (metrics.BusState, error) {
	ci, err := s.r.g.Consumer.Info(ctx)
	if err != nil {
		return metrics.BusState{}, err
	}
	si, err := s.r.stream.Info(ctx)
	if err != nil {
		return metrics.BusState{}, err
	}

	return metrics.BusState{AckPending: uint64(ci.NumAckPending), Messages: si.State.Msgs, Bytes: si.State.Bytes}, nil //nolint:gosec // counts are positive
}

// The ingest role's metrics recorded by a real flow: dispositions, a segment registered with its put and registration,
// a deferral with its reason, and on a refresh the open captures, their gaps, the parked versions and the bus's backlog.
func TestIngestMetrics(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		s := metrics.NewServer()
		m, err := metrics.NewIngest(s)
		if err != nil {
			t.Fatal(err)
		}
		r.g.Metrics, r.g.Flusher.Metrics = m, m
		capture := tracepack.UUID{70}
		for _, msg := range traffic(t, capture) {
			r.publish(t, msg, nil)
		}
		r.publish(t, inline(t, tracepack.UUID{71}, dataRecord(1, now), nil), nil) // an unknown capture's record: nak'd
		r.step(t)
		r.clk.flushInterval()
		r.step(t)
		if err := m.Refresh(ctx, c, trafficState{r}); err != nil {
			t.Fatal(err)
		}
		body := scrape(t, s)
		for _, want := range []string{
			`tracekeep_ingest_messages_total{disposition="batch"} 4`,
			`tracekeep_ingest_messages_total{disposition="nak"} 1`,
			`tracekeep_ingest_segments_total{tool_id="T1"} 1`,
			`tracekeep_ingest_put_seconds_count 1`,
			`tracekeep_ingest_registration_seconds_count 1`,
			`tracekeep_ingest_deferred_messages{reason="unknown-capture"} 1`,
			`tracekeep_ingest_captures_open{tool_id="T1"} 1`,
			`tracekeep_ingest_open_gaps{tool_id="T1"} 0`,
			`tracekeep_ingest_stream_backlog_messages 1`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("/metrics lacks %s", want)
			}
		}
		if !strings.Contains(body, `tracekeep_ingest_segment_bytes_total{tool_id="T1"}`) || strings.Contains(body, `tracekeep_ingest_segment_bytes_total{tool_id="T1"} 0`) {
			t.Error("segment bytes not counted")
		}
	})
}

// A component without metrics records nothing and never fails: every test runs the role so, with Metrics nil.
func TestNilMetricsRecordNothing(t *testing.T) {
	t.Parallel()
	var m *metrics.Ingest
	m.Disposed("ack", true)
	m.Deferred("x", "y")
	m.Settled("x")
	m.InProgress(3)
	m.Lost("T1")
	if err := m.Refresh(t.Context(), nil, nil); err != nil {
		t.Errorf("Refresh on nil = %v", err)
	}
}

// A deferral that escalates to parking and falls back to a nak, parking off, stays one deferral from its first nak:
// its age is never reset by the escalation.
func TestParkFallbackKeepsTheDeferralAge(t *testing.T) {
	t.Parallel()
	s := metrics.NewServer()
	m, err := metrics.NewIngest(s)
	if err != nil {
		t.Fatal(err)
	}
	g := &Ingester{Metrics: m, Log: slog.New(slog.DiscardHandler)}
	d := &Delivery{}
	d.Msg.Env.MsgID = "m1"
	g.act(t.Context(), Entry{D: d, Msg: &msg{}}, Decision{Action: ActNak, Reason: ReasonUnknownCapture}, 1)
	time.Sleep(1100 * time.Millisecond)
	g.act(t.Context(), Entry{D: d, Msg: &msg{}}, Decision{Action: ActPark, Reason: ReasonUnknownCapture}, 9)
	match := regexp.MustCompile(`tracekeep_ingest_deferred_oldest_age_seconds\{reason="` + ReasonUnknownCapture + `"\} (\S+)`).FindStringSubmatch(scrape(t, s))
	if match == nil {
		t.Fatal("the deferral was not kept")
	}
	if age, _ := strconv.ParseFloat(match[1], 64); age < 1 {
		t.Errorf("deferred age %v after a park fallback, want the age from the first nak", age)
	}
}
