package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// The outcomes of tracekeep_logfeed_entries_total (logfeed.md §15.11).
const (
	OutcomeRecord          = "record"
	OutcomeAnnotation      = "annotation"
	OutcomeDuplicate       = "duplicate"
	OutcomeStale           = "stale"
	OutcomeBeyondRetention = "beyond-retention"
	OutcomeUnreadable      = "unreadable"
)

// Logfeed is the host log feed's metrics (logfeed.md §15.11), each by equipment_id, counted by the replica holding the group's lease.
// Every method records nothing on a nil *Logfeed, so a converter without metrics needs no check.
type Logfeed struct {
	entries, unpaired, annotations *prometheus.CounterVec
	last, lastOutcome              *prometheus.GaugeVec
}

// NewLogfeed registers the host log feed's collectors on s.
func NewLogfeed(s *Server) (*Logfeed, error) {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "logfeed", Name: name, Help: help}, labels)
	}
	m := &Logfeed{
		entries: counter("entries_total", "Feed entries disposed, by equipment and outcome.", "equipment_id", "outcome"),
		unpaired: counter("unpaired_total",
			"Entries pairing left alone, a header no body took or a body no header was pending for, by equipment and side.", "equipment_id", "side"),
		annotations: counter("annotations_total", "Annotations converted, by equipment and kind.", "equipment_id", "kind"),
		last: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "logfeed", Name: "last_entry_timestamp_seconds",
			Help: "When this replica last received an entry of the equipment, in Unix seconds."}, []string{"equipment_id"}),
		lastOutcome: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "logfeed", Name: "last_outcome_timestamp_seconds",
			Help: "When this replica last counted an entry of the equipment with the outcome, in Unix seconds: an event no scrape misses."},
			[]string{"equipment_id", "outcome"}),
	}
	for _, c := range []prometheus.Collector{m.entries, m.unpaired, m.annotations, m.last, m.lastOutcome} {
		if err := s.Registry().Register(c); err != nil {
			return nil, fmt.Errorf("metrics: logfeed: %w", err)
		}
	}

	return m, nil
}

// Entries counts n entries of equipment disposed with outcome at at.
func (m *Logfeed) Entries(equipment, outcome string, n int, at time.Time) {
	if m == nil {
		return
	}
	m.entries.WithLabelValues(equipment, outcome).Add(float64(n))
	m.lastOutcome.WithLabelValues(equipment, outcome).Set(float64(at.UnixNano()) / 1e9)
}

// Unpaired counts n entries of equipment pairing left alone, side header or body.
func (m *Logfeed) Unpaired(equipment, side string, n int) {
	if m == nil {
		return
	}
	m.unpaired.WithLabelValues(equipment, side).Add(float64(n))
}

// Annotations counts n annotations of equipment of kind.
func (m *Logfeed) Annotations(equipment, kind string, n int) {
	if m == nil {
		return
	}
	m.annotations.WithLabelValues(equipment, kind).Add(float64(n))
}

// Arrived stamps an entry of equipment received at.
func (m *Logfeed) Arrived(equipment string, at time.Time) {
	if m == nil {
		return
	}
	m.last.WithLabelValues(equipment).Set(float64(at.UnixNano()) / 1e9)
}
