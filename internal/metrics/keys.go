package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Keys is the keys module's metrics (operations.md §10.4 keys), in every role that resolves a key.
// kind is the only per-key label: never a key id, a reference or a credential.
// Every method records nothing on a nil *Keys.
type Keys struct {
	resolutions, providerErrors *prometheus.CounterVec
	providerSeconds             *prometheus.HistogramVec
	credentialExpiry            *prometheus.GaugeVec
}

// NewKeys registers the keys module's collectors on s.
func NewKeys(s *Server) (*Keys, error) {
	m := &Keys{
		resolutions: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Name: "key_resolutions_total",
			Help: "Uses of a key by kind and outcome: cached, resolved, unavailable (a retry may cure it) or refused (masking-keys.md §9)."}, []string{"kind", "outcome"}),
		providerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Name: "key_provider_errors_total",
			Help: "Failed provider calls by provider and reason."}, []string{"provider", "reason"}),
		providerSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "tracekeep", Name: "key_provider_seconds",
			Help: "A provider call's duration by provider and operation: login, renew or read.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 15)},
			[]string{"provider", "op"}),
		credentialExpiry: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "tracekeep", Name: "key_credential_expiry_timestamp",
			Help: "When the provider's current token expires, in Unix seconds."}, []string{"provider"}),
	}
	for _, c := range []prometheus.Collector{m.resolutions, m.providerErrors, m.providerSeconds, m.credentialExpiry} {
		if err := s.Registry().Register(c); err != nil {
			return nil, fmt.Errorf("metrics: keys: %w", err)
		}
	}

	return m, nil
}

// Resolution counts one use of a kind's key with its outcome.
func (m *Keys) Resolution(kind, outcome string) {
	if m == nil {
		return
	}
	m.resolutions.WithLabelValues(kind, outcome).Inc()
}

// ProviderCall records one call to provider for op, which took d and failed for reason, or succeeded when reason is empty.
func (m *Keys) ProviderCall(provider, op, reason string, d time.Duration) {
	if m == nil {
		return
	}
	m.providerSeconds.WithLabelValues(provider, op).Observe(d.Seconds())
	if reason != "" {
		m.providerErrors.WithLabelValues(provider, reason).Inc()
	}
}

// CredentialExpiry records when provider's current token expires.
func (m *Keys) CredentialExpiry(provider string, at time.Time) {
	if m == nil {
		return
	}
	m.credentialExpiry.WithLabelValues(provider).Set(float64(at.Unix()))
}
