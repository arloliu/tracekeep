package metrics

import (
	"context"
	"errors"
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
	refreshFailures                                     prometheus.Counter
	snap                                                *snapshot
	deferred                                            *deferrals
}

// NewIngest registers the ingest role's collectors on s.
func NewIngest(s *Server) (*Ingest, error) {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "ingest", Name: name, Help: help}, labels)
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
		refreshFailures: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "tracekeep", Subsystem: "ingest", Name: "refresh_failures_total",
			Help: "Refreshes of the catalog and bus gauges that failed, the gauges keeping their previous values."}),
		snap:     &snapshot{},
		deferred: &deferrals{byID: map[string]deferral{}, now: time.Now},
	}
	for _, c := range []prometheus.Collector{
		m.messages, m.segments, m.segmentBytes, m.conflicts, m.losses, m.redelivered, m.inProgress, m.put, m.registration,
		m.refreshFailures, m.snap, m.deferred,
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

// Deferred notes the message id nak'd for reason, from the first time it was for that reason; Settled forgets it once it is decided otherwise.
// The gauge is per replica: a message another replica receives meanwhile is dropped once this one has not deferred it for deferralStale.
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

// Refresh reads the gauges of the catalog and the bus: the traffic consumer's and stream's backlog,
// then the unresolved parked versions by reason and the open captures and their unfilled holes by tool.
// Each source's gauges change together, at once, and only when the whole source was read: a failure keeps its previous values
// and counts in refresh_failures_total. A pass over every open capture's completeness, it runs on a timer, never per delivery.
func (m *Ingest) Refresh(ctx context.Context, cat *catalog.Catalog, cons Consumer) error {
	if m == nil {
		return nil
	}
	var errs []error
	if cons != nil {
		if st, err := cons.State(ctx); err != nil {
			errs = append(errs, err)
		} else {
			m.snap.setBus(st)
		}
	}
	if cat != nil {
		if c, err := readCatalog(ctx, cat); err != nil {
			errs = append(errs, err)
		} else {
			m.snap.setCatalog(c)
		}
	}
	err := errors.Join(errs...)
	if err != nil {
		m.refreshFailures.Inc()
	}

	return err
}

// catalogState is the catalog's part of a refresh.
type catalogState struct {
	parked, captures, gaps map[string]int
}

func readCatalog(ctx context.Context, cat *catalog.Catalog) (catalogState, error) {
	parked, err := cat.ParkedCounts(ctx)
	if err != nil {
		return catalogState{}, err
	}
	open, err := cat.OpenCaptures(ctx)
	if err != nil {
		return catalogState{}, err
	}
	c := catalogState{parked: parked, captures: map[string]int{}, gaps: map[string]int{}}
	for _, o := range open {
		cp, err := cat.Completeness(ctx, o.ID)
		if err != nil {
			return catalogState{}, err
		}
		c.captures[o.ToolID]++
		c.gaps[o.ToolID] += len(cp.Gaps)
	}

	return c, nil
}

// snapshot holds the gauges Refresh reads, each source's replaced whole under the mutex, so a scrape never sees a refresh half done.
type snapshot struct {
	mu      sync.Mutex
	bus     BusState
	busRead bool
	cat     catalogState
}

var (
	parkedDesc       = prometheus.NewDesc("tracekeep_ingest_parked_messages", "Parked versions not yet resolved, by reason.", []string{"reason"}, nil)
	capturesOpenDesc = prometheus.NewDesc("tracekeep_ingest_captures_open", "Open captures, by tool.", []string{"tool_id"}, nil)
	openGapsDesc     = prometheus.NewDesc("tracekeep_ingest_open_gaps", "Holes currently unfilled in the open captures, by tool.", []string{"tool_id"}, nil)
	ackPendingDesc   = prometheus.NewDesc("tracekeep_ingest_consumer_ack_pending",
		"Messages the traffic consumer delivered and holds unacknowledged.", nil, nil)
	backlogMessagesDesc = prometheus.NewDesc("tracekeep_ingest_stream_backlog_messages", "Messages on the traffic stream.", nil, nil)
	backlogBytesDesc    = prometheus.NewDesc("tracekeep_ingest_stream_backlog_bytes", "Bytes on the traffic stream.", nil, nil)
)

func (s *snapshot) setBus(b BusState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bus, s.busRead = b, true
}

func (s *snapshot) setCatalog(c catalogState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cat = c
}

// Describe implements prometheus.Collector.
func (s *snapshot) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{parkedDesc, capturesOpenDesc, openGapsDesc, ackPendingDesc, backlogMessagesDesc, backlogBytesDesc} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (s *snapshot) Collect(ch chan<- prometheus.Metric) {
	s.mu.Lock()
	b, busRead, c := s.bus, s.busRead, s.cat
	s.mu.Unlock()
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	if busRead {
		gauge(ackPendingDesc, float64(b.AckPending))
		gauge(backlogMessagesDesc, float64(b.Messages))
		gauge(backlogBytesDesc, float64(b.Bytes))
	}
	for reason, n := range c.parked {
		gauge(parkedDesc, float64(n), reason)
	}
	for tool, n := range c.captures {
		gauge(capturesOpenDesc, float64(n), tool)
		gauge(openGapsDesc, float64(c.gaps[tool]), tool)
	}
}

// deferralStale is how long a deferral this replica no longer sees is kept: past every nak delay and the redelivery of a parked copy,
// a message not redelivered here since was redelivered to another replica, or decided there.
const deferralStale = 10 * time.Minute

// deferrals are the messages this replica nak'd and has not seen decided otherwise since:
// deferred_messages{reason} and deferred_oldest_age_seconds{reason}, computed at each scrape.
// An entry not deferred again within deferralStale is dropped, which bounds the map by the deferral rate.
type deferrals struct {
	mu   sync.Mutex
	byID map[string]deferral
	now  func() time.Time
}

type deferral struct {
	reason      string
	since, seen time.Time
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
	now := d.now()
	old, ok := d.byID[id]
	if !ok || old.reason != reason {
		old = deferral{reason: reason, since: now}
	}
	old.seen = now
	d.byID[id] = old
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

// Collect implements prometheus.Collector; it drops the stale entries.
func (d *deferrals) Collect(ch chan<- prometheus.Metric) {
	d.mu.Lock()
	now := d.now()
	counts, oldest := map[string]int{}, map[string]time.Time{}
	for id, f := range d.byID {
		if now.Sub(f.seen) > deferralStale {
			delete(d.byID, id)
			continue
		}
		counts[f.reason]++
		if o, ok := oldest[f.reason]; !ok || f.since.Before(o) {
			oldest[f.reason] = f.since
		}
	}
	d.mu.Unlock()
	for reason, n := range counts {
		ch <- prometheus.MustNewConstMetric(deferredDesc, prometheus.GaugeValue, float64(n), reason)
		ch <- prometheus.MustNewConstMetric(deferredAgeDesc, prometheus.GaugeValue, now.Sub(oldest[reason]).Seconds(), reason)
	}
}
