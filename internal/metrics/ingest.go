package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// Ingest is the ingest role's metrics (operations.md §10.4 ingest) whose data phase 1 produces.
// Every method records nothing on a nil *Ingest, so a component without metrics needs no check.
type Ingest struct {
	messages, segments, segmentBytes, conflicts, losses *prometheus.CounterVec
	redelivered, inProgress                             prometheus.Counter
	put, registration                                   prometheus.Histogram
	parked, capturesOpen, openGaps                      *prometheus.GaugeVec
	ackPending, backlogMessages, backlogBytes           prometheus.Gauge
	deferred                                            *deferrals
}

// NewIngest registers the ingest role's collectors on s.
func NewIngest(s *Server) (*Ingest, error) {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "ingest", Name: name, Help: help}, labels)
	}
	gauge := func(name, help string, labels ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "ingest", Name: name, Help: help}, labels)
	}
	one := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "tracekeep", Subsystem: "ingest", Name: name, Help: help})
	}
	m := &Ingest{
		messages:     counter("messages_total", "Deliveries disposed, by disposition.", "disposition"),
		segments:     counter("segments_total", "Segments registered, conflict segments included, by tool.", "tool_id"),
		segmentBytes: counter("segment_bytes_total", "Bytes of the segments registered, by tool.", "tool_id"),
		conflicts:    counter("conflicts_total", "Conflict segments registered, by tool.", "tool_id"),
		losses:       counter("declared_losses_total", "Seqs ingest declared lost, records beyond retention, by tool.", "tool_id"),
		redelivered: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "ingest", Name: "redelivered_total",
			Help: "Deliveries that were redeliveries."}),
		inProgress: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "ingest", Name: "in_progress_acks_total",
			Help: "In-progress acknowledgements sent for held messages."}),
		put: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "tracekeep", Subsystem: "ingest", Name: "put_seconds",
			Help: "The put of a segment to the store.", Buckets: prometheus.ExponentialBuckets(0.01, 2, 12)}),
		registration: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "tracekeep", Subsystem: "ingest", Name: "registration_seconds",
			Help: "The catalog registration of a segment.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 12)}),
		parked:          gauge("parked_messages", "Parked versions not yet resolved, by reason.", "reason"),
		capturesOpen:    gauge("captures_open", "Open captures, by tool.", "tool_id"),
		openGaps:        gauge("open_gaps", "Holes currently unfilled in the open captures, by tool.", "tool_id"),
		ackPending:      one("consumer_ack_pending", "Messages the traffic consumer delivered and holds unacknowledged."),
		backlogMessages: one("stream_backlog_messages", "Messages on the traffic stream."),
		backlogBytes:    one("stream_backlog_bytes", "Bytes on the traffic stream."),
		deferred:        &deferrals{byID: map[string]deferral{}, now: time.Now},
	}
	for _, c := range []prometheus.Collector{
		m.messages, m.segments, m.segmentBytes, m.conflicts, m.losses, m.redelivered, m.inProgress, m.put, m.registration,
		m.parked, m.capturesOpen, m.openGaps, m.ackPending, m.backlogMessages, m.backlogBytes, m.deferred,
	} {
		if err := s.Registry().Register(c); err != nil {
			return nil, fmt.Errorf("metrics: ingest: %w", err)
		}
	}

	return m, nil
}

// Disposed counts one delivery's disposition, and whether it was a redelivery.
func (m *Ingest) Disposed(disposition string, redelivered bool) {
	if m == nil {
		return
	}
	m.messages.WithLabelValues(disposition).Inc()
	if redelivered {
		m.redelivered.Inc()
	}
}

// Deferred notes the message id nak'd for reason, from the first time it was; Settled forgets it once it is decided otherwise.
// A message another replica receives meanwhile stays counted here until this replica sees it again: the gauge is per replica.
func (m *Ingest) Deferred(id, reason string) {
	if m == nil {
		return
	}
	m.deferred.add(id, reason)
}

// Settled forgets the message id's deferral.
func (m *Ingest) Settled(id string) {
	if m == nil {
		return
	}
	m.deferred.remove(id)
}

// InProgress counts in-progress acknowledgements.
func (m *Ingest) InProgress(n int) {
	if m == nil || n == 0 {
		return
	}
	m.inProgress.Add(float64(n))
}

// Put observes a segment's put.
func (m *Ingest) Put(d time.Duration) {
	if m == nil {
		return
	}
	m.put.Observe(d.Seconds())
}

// Registered observes a segment's registration and counts the segment of tool, a conflict segment or not.
func (m *Ingest) Registered(d time.Duration, tool string, bytes int64, conflict bool) {
	if m == nil {
		return
	}
	m.registration.Observe(d.Seconds())
	m.segments.WithLabelValues(tool).Inc()
	m.segmentBytes.WithLabelValues(tool).Add(float64(bytes))
	if conflict {
		m.conflicts.WithLabelValues(tool).Inc()
	}
}

// Lost counts a seq of tool ingest declared lost.
func (m *Ingest) Lost(tool string) {
	if m == nil {
		return
	}
	m.losses.WithLabelValues(tool).Inc()
}

// BusState is the traffic consumer's and stream's backlog.
type BusState struct {
	// AckPending are the messages the consumer delivered and holds unacknowledged.
	AckPending uint64
	// Messages and Bytes are the stream's.
	Messages, Bytes uint64
}

// Consumer is what Refresh reads of the traffic consumer and its stream.
type Consumer interface {
	State(ctx context.Context) (BusState, error)
}

// Refresh sets the gauges read from the catalog and the bus: the unresolved parked versions by reason,
// the open captures and their unfilled holes by tool, and the traffic consumer's and stream's backlog.
// A pass over every open capture's completeness, it runs on a timer, never per delivery.
func (m *Ingest) Refresh(ctx context.Context, cat *catalog.Catalog, cons Consumer) error {
	if m == nil {
		return nil
	}
	parked, err := cat.ParkedCounts(ctx)
	if err != nil {
		return err
	}
	m.parked.Reset()
	for reason, n := range parked {
		m.parked.WithLabelValues(reason).Set(float64(n))
	}
	open, err := cat.OpenCaptures(ctx)
	if err != nil {
		return err
	}
	captures, gaps := map[string]int{}, map[string]int{}
	for _, c := range open {
		cp, err := cat.Completeness(ctx, c.ID)
		if err != nil {
			return err
		}
		captures[c.ToolID]++
		gaps[c.ToolID] += len(cp.Gaps)
	}
	m.capturesOpen.Reset()
	m.openGaps.Reset()
	for tool, n := range captures {
		m.capturesOpen.WithLabelValues(tool).Set(float64(n))
		m.openGaps.WithLabelValues(tool).Set(float64(gaps[tool]))
	}
	if cons == nil {
		return nil
	}
	st, err := cons.State(ctx)
	if err != nil {
		return err
	}
	m.ackPending.Set(float64(st.AckPending))
	m.backlogMessages.Set(float64(st.Messages))
	m.backlogBytes.Set(float64(st.Bytes))

	return nil
}

// deferrals are the messages this replica nak'd and has not seen decided otherwise since:
// deferred_messages{reason} and deferred_oldest_age_seconds{reason}, computed at each scrape.
type deferrals struct {
	mu   sync.Mutex
	byID map[string]deferral
	now  func() time.Time
}

type deferral struct {
	reason string
	since  time.Time
}

var (
	deferredDesc = prometheus.NewDesc("tracekeep_ingest_deferred_messages", "Messages nak'd and not yet decided otherwise, by reason.",
		[]string{"reason"}, nil)
	deferredAgeDesc = prometheus.NewDesc("tracekeep_ingest_deferred_oldest_age_seconds", "Age of the oldest deferred message, by reason.",
		[]string{"reason"}, nil)
)

func (d *deferrals) add(id, reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if old, ok := d.byID[id]; ok && old.reason == reason {
		return
	}
	d.byID[id] = deferral{reason: reason, since: d.now()}
}

func (d *deferrals) remove(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.byID, id)
}

// Describe implements prometheus.Collector.
func (d *deferrals) Describe(ch chan<- *prometheus.Desc) {
	ch <- deferredDesc
	ch <- deferredAgeDesc
}

// Collect implements prometheus.Collector.
func (d *deferrals) Collect(ch chan<- prometheus.Metric) {
	d.mu.Lock()
	counts, oldest := map[string]int{}, map[string]time.Time{}
	for _, f := range d.byID {
		counts[f.reason]++
		if o, ok := oldest[f.reason]; !ok || f.since.Before(o) {
			oldest[f.reason] = f.since
		}
	}
	now := d.now()
	d.mu.Unlock()
	for reason, n := range counts {
		ch <- prometheus.MustNewConstMetric(deferredDesc, prometheus.GaugeValue, float64(n), reason)
		ch <- prometheus.MustNewConstMetric(deferredAgeDesc, prometheus.GaugeValue, now.Sub(oldest[reason]).Seconds(), reason)
	}
}
