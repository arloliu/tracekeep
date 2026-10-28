package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// API is the api role's metrics (operations.md §10.4 api), for the code that exists: auth's refusals.
// Every method records nothing on a nil *API.
type API struct {
	authFailures *prometheus.CounterVec
}

// NewAPI registers the api role's collectors on s.
func NewAPI(s *Server) (*API, error) {
	m := &API{
		authFailures: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "api", Name: "auth_failures_total",
			Help: "Requests auth refused, by reason (api.md §10.2)."}, []string{"reason"}),
	}
	if err := s.Registry().Register(m.authFailures); err != nil {
		return nil, fmt.Errorf("metrics: api: %w", err)
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
