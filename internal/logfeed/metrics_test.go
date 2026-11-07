package logfeed

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/metrics"
)

// A converter run counts each entry's outcome, each annotation's kind and each unpaired side, by equipment (logfeed.md §15.11),
// a converted entry once its flush registers, and stamps the equipment's last entry at its arrival and each outcome's last count.
func TestConverterMetrics(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		h := newHarness(t, c)
		s := metrics.NewServer()
		m, err := metrics.NewLogfeed(s)
		if err != nil {
			t.Fatal(err)
		}
		h.conv.Metrics = m
		h.conv.HorizonHour = func() int64 { return ingest.HourOf(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()) }
		f := &fixture{t: t}
		lone := msgOf(t, f, map[string]any{"kind": KindHeader, "dir": DirSent, "header": "000182290000000000ff", "sxfy": "S2F41", "tid": "77"})
		h1, b1 := pairMsgs(t, f, 6, 11, true, 1)
		line := msgOf(t, f, map[string]any{"kind": KindUnclassified, "text": "a line"})
		bad := msgOf(t, f, map[string]any{"kind": KindBody, "sxfy": "S6F11", "text": 17})
		h2, b2 := pairMsgs(t, f, 6, 12, false, 1)
		body := msgOf(t, f, map[string]any{"kind": KindBody, "sxfy": "S1F1", "tid": "5", "text": "S1F1 W\n."})
		old := msgOf(t, f, map[string]any{"kind": KindUnclassified, "text": "old", "ts": "2025-06-01 08:00:00.000"})
		h.receive(lone, h1, b1, line, bad, h2, b2, body, old, &fakeMsg{data: []byte("{nope")}, &fakeMsg{data: b1.data})
		h.tick(2 * time.Second)
		if got := scrape(t, s); strings.Contains(got, `outcome="record"`) || strings.Contains(got, "annotations_total{") {
			t.Errorf("entries converted and not yet registered are counted:\n%s", got)
		}
		h.tick(10 * time.Second)
		h.receive(&fakeMsg{data: b2.data}) // below the registered watermark: stale
		got := scrape(t, s)
		for _, want := range []string{
			`tracekeep_logfeed_entries_total{equipment_id="T1",outcome="record"} 4`,
			`tracekeep_logfeed_entries_total{equipment_id="T1",outcome="annotation"} 4`,
			`tracekeep_logfeed_entries_total{equipment_id="T1",outcome="duplicate"} 1`,
			`tracekeep_logfeed_entries_total{equipment_id="T1",outcome="stale"} 1`,
			`tracekeep_logfeed_entries_total{equipment_id="T1",outcome="beyond-retention"} 1`,
			`tracekeep_logfeed_entries_total{equipment_id="T1",outcome="unreadable"} 1`,
			`tracekeep_logfeed_annotations_total{equipment_id="T1",kind="unparsed-entry"} 2`,
			`tracekeep_logfeed_annotations_total{equipment_id="T1",kind="unrecognised-line"} 2`,
			`tracekeep_logfeed_unpaired_total{equipment_id="T1",side="header"} 1`,
			`tracekeep_logfeed_unpaired_total{equipment_id="T1",side="body"} 1`,
			`tracekeep_logfeed_last_entry_timestamp_seconds{equipment_id="T1"} 1.800000012e+09`,
			`tracekeep_logfeed_last_outcome_timestamp_seconds{equipment_id="T1",outcome="unreadable"} 1.8e+09`,
			`tracekeep_logfeed_last_outcome_timestamp_seconds{equipment_id="T1",outcome="record"} 1.800000012e+09`,
		} {
			if !strings.Contains(got, want+"\n") {
				t.Errorf("metrics lack %s", want)
			}
		}
		if t.Failed() {
			t.Log(got)
		}
	})
}

// A flush registered whose acknowledgement fails still counts its entries: their redeliveries are stale, never converted again.
func TestConverterCountsARegisteredFlushWhoseAcknowledgementFails(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		h := newHarness(t, c)
		s := metrics.NewServer()
		m, err := metrics.NewLogfeed(s)
		if err != nil {
			t.Fatal(err)
		}
		h.conv.Metrics = m
		hd, bd := pairMsgs(t, &fixture{t: t}, 6, 11, true, 1)
		bd.ackErr = errors.New("connection closed")
		h.receive(hd, bd)
		h.tick(2 * time.Second)
		h.now = h.now.Add(10 * time.Second)
		if err := h.conv.Tick(t.Context(), h.now); err == nil {
			t.Fatal("a flush whose acknowledgement failed succeeded")
		}
		if got := scrape(t, s); !strings.Contains(got, `tracekeep_logfeed_entries_total{equipment_id="T1",outcome="record"} 2`+"\n") {
			t.Errorf("the registered flush's entries uncounted:\n%s", got)
		}
	})
}

func scrape(t *testing.T, s *metrics.Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)

	return string(body)
}
