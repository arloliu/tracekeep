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
	authFailures, requests, downloadURLs, restores, digests, logins *prometheus.CounterVec
	downloadsRefused, digestInputs                                  prometheus.Counter
	duration                                                        *prometheus.HistogramVec
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
	for _, c := range []prometheus.Collector{m.authFailures, m.requests, m.duration, m.downloadURLs, m.downloadsRefused, m.restores, m.digests, m.digestInputs, m.logins} {
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
