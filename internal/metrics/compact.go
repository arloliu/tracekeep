package metrics

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Compact is the compact role's metrics (operations.md §10.4 compact) for the work it does:
// the reconciler's, the merge's, the close's, deletion's and retention's.
// Every method records nothing on a nil *Compact.
type Compact struct {
	cycle                            *prometheus.HistogramVec
	lastSuccess                      *prometheus.GaugeVec
	leaseHeld                        prometheus.GaugeFunc
	adopted, quarantined, tombstones prometheus.Counter
	deletions                        prometheus.Counter
	merges, closes                   *prometheus.CounterVec
	scopes                           *prometheus.GaugeVec
	conflictsOpen, oldestUnmerged    prometheus.Gauge
	claimObjects, claimBytes         prometheus.Gauge
	survivors                        prometheus.Gauge
	exportDeleted, exportFailures    prometheus.Counter
	exportJobs                       *prometheus.GaugeVec
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
		tombstones: counter("tombstones_total",
			"Keys the compactor tombstoned: beyond the horizon, lost to a termination, a merged input deleted or an hour evicted."),
		deletions: counter("deletions_total", "Merged inputs deleted D after their merge."),
		merges: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "merges_total",
			Help: "Merges by outcome: merged, stale (retried next cycle), conflicted or failed."}, []string{"outcome"}),
		closes: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "closes_total",
			Help: "Captures closed by a close intent, by trigger: inactivity, successor or operator."}, []string{"trigger"}),
		scopes: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "scopes",
			Help: "Scopes by state, as the lease holder last read them."}, []string{"state"}),
		conflictsOpen: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "conflicts_open",
			Help: "Conflicts no operator acknowledged, as the lease holder last read them."}),
		oldestUnmerged: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "oldest_unmerged_scope_age_seconds",
			Help: "How long ago the hour of the oldest open or reopened scope ended, 0 when none has; as the lease holder last read it."}),
		claimObjects: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "claim_objects",
			Help: "Claim objects the catalog knows, in every state, as the lease holder last read them."}),
		claimBytes: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "claim_object_bytes",
			Help: "The bytes of the claim objects the catalog knows, as the lease holder last read them."}),
		survivors: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "claim_objects_survivors",
			Help: "Claim objects retention kept for an unresolved parked version, as the lease holder last read them."}),
	}
	m.exportDeleted = counter("export_objects_deleted_total", "Export objects the cleanup deleted: abandoned attempts and expired bundles.")
	m.exportFailures = counter("export_cleanup_failures_total", "Export cleanup steps that failed, retried the next cycle.")
	m.exportJobs = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "export_jobs",
		Help: "Export jobs by state, as the lease holder last read them."}, []string{"state"})
	m.leaseHeld = prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "compact", Name: "lease_held",
		Help: "1 while this process holds the compactor lease: taken or renewed, and not lapsed since."}, m.held)
	for _, c := range []prometheus.Collector{
		m.cycle, m.lastSuccess, m.leaseHeld, m.adopted, m.quarantined, m.tombstones, m.deletions, m.merges, m.closes, m.scopes,
		m.conflictsOpen, m.oldestUnmerged, m.claimObjects, m.claimBytes, m.survivors, m.exportDeleted, m.exportFailures, m.exportJobs,
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
func (m *Compact) Merges(merged, stale, conflicted, failed, refused int) {
	if m == nil {
		return
	}
	for outcome, n := range map[string]int{"merged": merged, "stale": stale, "conflicted": conflicted, "failed": failed, "refused": refused} {
		m.merges.WithLabelValues(outcome).Add(float64(n))
	}
}

// Scopes sets the scope gauges: the count of each state of scopes (every state a scope can take, absent ones 0),
// the open conflicts, and the end of the oldest unmerged scope's hour, the zero time when there is none.
func (m *Compact) Scopes(byState map[string]int, openConflicts int, oldestUnmergedEnd time.Time) {
	if m == nil {
		return
	}
	for _, state := range []string{"open", "settled", "reopened", "unmergeable"} {
		m.scopes.WithLabelValues(state).Set(float64(byState[state]))
	}
	m.conflictsOpen.Set(float64(openConflicts))
	age := 0.0
	if !oldestUnmergedEnd.IsZero() {
		age = max(0, m.now().Sub(oldestUnmergedEnd).Seconds())
	}
	m.oldestUnmerged.Set(age)
}

// Closes counts the captures a close pass closed, by trigger.
func (m *Compact) Closes(byTrigger map[string]int) {
	if m == nil {
		return
	}
	for trigger, n := range byTrigger {
		m.closes.WithLabelValues(trigger).Add(float64(n))
	}
}

// Retention counts the merged inputs deletion deleted and the keys deletion and retention tombstoned.
func (m *Compact) Retention(deleted, tombstoned int) {
	if m == nil {
		return
	}
	m.deletions.Add(float64(deleted))
	m.tombstones.Add(float64(tombstoned))
}

// Claims sets the claim object gauges: the objects the catalog knows, their bytes, and the survivors among them.
func (m *Compact) Claims(objects, bytes, survivors int64) {
	if m == nil {
		return
	}
	m.claimObjects.Set(float64(objects))
	m.claimBytes.Set(float64(bytes))
	m.survivors.Set(float64(survivors))
}

// ExportCleanup counts one export cleanup's deleted objects and failed steps.
func (m *Compact) ExportCleanup(deleted, failed int) {
	if m == nil {
		return
	}
	m.exportDeleted.Add(float64(deleted))
	m.exportFailures.Add(float64(failed))
}

// ExportJobs sets the export job gauges, every state, absent ones 0.
func (m *Compact) ExportJobs(byState map[string]int) {
	if m == nil {
		return
	}
	for _, st := range []string{"queued", "running", "done", "failed", "cancelled"} {
		m.exportJobs.WithLabelValues(st).Set(float64(byState[st]))
	}
}
