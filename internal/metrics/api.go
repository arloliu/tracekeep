package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// API is the api role's metrics (operations.md §10.4 api), for the code that exists:
// requests by operation and code with their durations, and auth's refusals.
// Every method records nothing on a nil *API.
type API struct {
	authFailures, requests, downloadURLs, restores, digests, logins, readsIncomplete, exports *prometheus.CounterVec
	downloadsRefused, digestInputs, readsRefused, recordsStreamed, exportBytes                prometheus.Counter
	readsInFlight                                                                             prometheus.Gauge
	duration                                                                                  *prometheus.HistogramVec
}

// NewAPI registers the api role's collectors on s.
func NewAPI(s *Server) (*API, error) {
	m := &API{
		authFailures: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "auth_failures_total",
			Help: "Requests auth refused, by reason (api.md §10.2)."}, []string{"reason"}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "requests_total",
			Help: "Requests by operation and Connect code, ok for success; refused ones included."}, []string{"operation", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "tracekeep", Subsystem: "api", Name: "request_seconds",
			Help: "A request's duration by operation.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)}, []string{"operation"}),
	}
	m.downloadURLs = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "download_urls_total",
		Help: "Download URLs issued, by kind: presigned by the store, or served by the API."}, []string{"kind"})
	m.downloadsRefused = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "download_urls_refused_total",
		Help: "Download URLs refused as pack-superseded: the object's deadline too near, or the pack deleted."})
	m.restores = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "restores_total",
		Help: "Restores by outcome: ok, or the reason it was refused (masking-keys.md §9)."}, []string{"outcome"})
	m.digests = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "digests_total",
		Help: "Digest requests by outcome: ok, or the reason it was refused (masking-keys.md §9)."}, []string{"outcome"})
	m.digestInputs = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "digest_inputs_total",
		Help: "Inputs digested by Digest requests answered ok."})
	m.logins = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "logins_total",
		Help: "People's logins started and callbacks answered, by step and outcome: ok, refused or unavailable (api.md §10.2)."}, []string{"step", "outcome"})
	m.readsInFlight = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "api", Name: "record_reads_in_flight",
		Help: "QueryRecords streams and FindTransaction lookups being served (api.md §10.1 *Records*)."})
	m.readsRefused = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "record_reads_refused_total",
		Help: "Record reads refused as resource_exhausted, beyond the replica's concurrent reads."})
	m.recordsStreamed = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "records_streamed_total",
		Help: "Record versions QueryRecords streamed."})
	m.readsIncomplete = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "record_reads_incomplete_total",
		Help: "Parts of record reads that could not be read, by reason."}, []string{"reason"})
	m.exports = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "exports_total",
		Help: "Export jobs this replica ended, by outcome: done or failed (api.md §10.1 *Exports*)."}, []string{"outcome"})
	m.exportBytes = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "export_bytes_total",
		Help: "Bytes of the export bundles this replica completed."})
	for _, c := range []prometheus.Collector{
		m.exports, m.exportBytes,
		m.authFailures, m.requests, m.duration, m.downloadURLs, m.downloadsRefused, m.restores, m.digests, m.digestInputs, m.logins,
		m.readsInFlight, m.readsRefused, m.recordsStreamed, m.readsIncomplete,
	} {
		if err := s.Registry().Register(c); err != nil {
			return nil, fmt.Errorf("metrics: api: %w", err)
		}
	}

	return m, nil
}

// AuthFailure counts one request auth refused for reason.
func (m *API) AuthFailure(reason string) {
	if m == nil {
		return
	}
	m.authFailures.WithLabelValues(reason).Inc()
}

// Login counts one step of a person's login with its outcome.
func (m *API) Login(step, outcome string) {
	if m == nil {
		return
	}
	m.logins.WithLabelValues(step, outcome).Inc()
}

// Request records one request of operation answered with code, which took d.
func (m *API) Request(operation, code string, d time.Duration) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(operation, code).Inc()
	m.duration.WithLabelValues(operation).Observe(d.Seconds())
}

// DownloadURL counts one download URL issued of kind, presigned or api.
func (m *API) DownloadURL(kind string) {
	if m == nil {
		return
	}
	m.downloadURLs.WithLabelValues(kind).Inc()
}

// DownloadRefused counts one download URL refused as pack-superseded.
func (m *API) DownloadRefused() {
	if m == nil {
		return
	}
	m.downloadsRefused.Inc()
}

// Restore counts one restore of outcome.
func (m *API) Restore(outcome string) {
	if m == nil {
		return
	}
	m.restores.WithLabelValues(outcome).Inc()
}

// Digest counts one digest request of outcome, and its n inputs when it was answered.
func (m *API) Digest(outcome string, n int) {
	if m == nil {
		return
	}
	m.digests.WithLabelValues(outcome).Inc()
	if outcome == "ok" {
		m.digestInputs.Add(float64(n))
	}
}

// ReadStarted and ReadEnded track one record read in flight; ReadRefused counts one refused beyond the bound.
func (m *API) ReadStarted() {
	if m != nil {
		m.readsInFlight.Inc()
	}
}

// ReadEnded ends one record read in flight.
func (m *API) ReadEnded() {
	if m != nil {
		m.readsInFlight.Dec()
	}
}

// ReadRefused counts one record read refused beyond the replica's concurrent reads.
func (m *API) ReadRefused() {
	if m != nil {
		m.readsRefused.Inc()
	}
}

// RecordsStreamed counts n record versions streamed.
func (m *API) RecordsStreamed(n int) {
	if m != nil {
		m.recordsStreamed.Add(float64(n))
	}
}

// ReadIncomplete counts one part of a record read that could not be read, for reason.
func (m *API) ReadIncomplete(reason string) {
	if m != nil {
		m.readsIncomplete.WithLabelValues(reason).Inc()
	}
}

// Export counts one export job this replica ended with outcome, and size bytes of its bundle when it completed.
func (m *API) Export(outcome string, size int64) {
	if m == nil {
		return
	}
	m.exports.WithLabelValues(outcome).Inc()
	m.exportBytes.Add(float64(size))
}
