package metrics

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Compact is the compact role's metrics (operations.md §10.4 compact) for the work it does: the reconciler's and the merge's.
// Every method records nothing on a nil *Compact.
type Compact struct {
	cycle                            *prometheus.HistogramVec
	lastSuccess                      *prometheus.GaugeVec
	leaseHeld                        prometheus.GaugeFunc
	adopted, quarantined, tombstones prometheus.Counter
	merges                           *prometheus.CounterVec
	scopes                           *prometheus.GaugeVec
	conflictsOpen, oldestUnmerged    prometheus.Gauge
	// leaseUntil is when the lease this process last took or renewed lapses, in Unix nanoseconds; 0 when it holds none.
	leaseUntil atomic.Int64
	now        func() time.Time
}

// NewCompact registers the compact role's collectors on s.
func NewCompact(s *Server) (*Compact, error) {
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "compact", Name: name, Help: help})
	}
	m := &Compact{now: time.Now,
		cycle: prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "cycle_seconds",
			Help: "One cycle of a task.", Buckets: prometheus.ExponentialBuckets(0.01, 2, 14)}, []string{"task"}),
		lastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "last_success_timestamp",
			Help: "When a task's cycle last succeeded, in Unix seconds."}, []string{"task"}),
		adopted:     counter("orphans_adopted_total", "Orphaned packs the reconciler registered as adopted."),
		quarantined: counter("orphans_quarantined_total", "Orphans the reconciler quarantined: reported, never registered or deleted."),
		tombstones:  counter("tombstones_total", "Keys the reconciler tombstoned: beyond the horizon or lost to a termination."),
		merges: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "merges_total",
			Help: "Merges by outcome: merged, stale (retried next cycle), conflicted or failed."}, []string{"outcome"}),
		scopes: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "scopes",
			Help: "Scopes by state, as the lease holder last read them."}, []string{"state"}),
		conflictsOpen: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "conflicts_open",
			Help: "Conflicts no operator acknowledged, as the lease holder last read them."}),
		oldestUnmerged: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "oldest_unmerged_scope_age_seconds",
			Help: "How long ago the hour of the oldest open or reopened scope ended, 0 when none has; as the lease holder last read it."}),
	}
	m.leaseHeld = prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "lease_held",
		Help: "1 while this process holds the compactor lease: taken or renewed, and not lapsed since."}, m.held)
	for _, c := range []prometheus.Collector{
		m.cycle, m.lastSuccess, m.leaseHeld, m.adopted, m.quarantined, m.tombstones, m.merges, m.scopes, m.conflictsOpen, m.oldestUnmerged,
	} {
		if err := s.Registry().Register(c); err != nil {
			return nil, fmt.Errorf("metrics: compact: %w", err)
		}
	}

	return m, nil
}

// Cycle observes one cycle of task, and stamps its success.
func (m *Compact) Cycle(task string, d time.Duration, ok bool) {
	if m == nil {
		return
	}
	m.cycle.WithLabelValues(task).Observe(d.Seconds())
	if ok {
		m.lastSuccess.WithLabelValues(task).SetToCurrentTime()
	}
}

// Lease records the compactor lease this process holds as lapsing at until, the zero time when it holds none:
// a lease that is not renewed in time stops counting as held when it lapses, though no cycle runs to notice.
func (m *Compact) Lease(until time.Time) {
	if m == nil {
		return
	}
	var untilNs int64
	if !until.IsZero() {
		untilNs = until.UnixNano()
	}
	m.leaseUntil.Store(untilNs)
}

func (m *Compact) held() float64 {
	if m.now().UnixNano() < m.leaseUntil.Load() {
		return 1
	}

	return 0
}

// Orphans counts what an orphan pass adopted, quarantined and tombstoned.
func (m *Compact) Orphans(adopted, quarantined, tombstoned int) {
	if m == nil {
		return
	}
	m.adopted.Add(float64(adopted))
	m.quarantined.Add(float64(quarantined))
	m.tombstones.Add(float64(tombstoned))
}

// Merges counts what a merge pass did, by outcome.
func (m *Compact) Merges(merged, stale, conflicted, failed int) {
	if m == nil {
		return
	}
	for outcome, n := range map[string]int{"merged": merged, "stale": stale, "conflicted": conflicted, "failed": failed} {
		m.merges.WithLabelValues(outcome).Add(float64(n))
	}
}

// Scopes sets the scope gauges: the count of each state of scopes (every state a scope can take, absent ones 0),
// the open conflicts, and the end of the oldest unmerged scope's hour, the zero time when there is none.
func (m *Compact) Scopes(byState map[string]int, openConflicts int, oldestUnmergedEnd time.Time) {
	if m == nil {
		return
	}
	for _, state := range []string{"open", "settled", "reopened"} {
		m.scopes.WithLabelValues(state).Set(float64(byState[state]))
	}
	m.conflictsOpen.Set(float64(openConflicts))
	age := 0.0
	if !oldestUnmergedEnd.IsZero() {
		age = max(0, m.now().Sub(oldestUnmergedEnd).Seconds())
	}
	m.oldestUnmerged.Set(age)
}
