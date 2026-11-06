package producer

import (
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// blockedLog blocks every call until release is closed, and counts the calls that began;
// while pass is set, a call returns at once, uncounted.
type blockedLog struct {
	calls   atomic.Int64
	pass    atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (l *blockedLog) call() {
	if l.pass.Load() {
		return
	}
	l.calls.Add(1)
	select {
	case l.entered <- struct{}{}:
	default:
	}
	<-l.release
}

func (l *blockedLog) Info(string, ...any)  { l.call() }
func (l *blockedLog) Warn(string, ...any)  { l.call() }
func (l *blockedLog) Error(string, ...any) { l.call() }

// A Logger that blocks stalls only the sink goroutine: the lifecycle and every sinkLogger caller,
// nats.go's callbacks among them, keep going, past a full queue too;
// once it is released Close joins the sink, and no line reaches the Logger after Close returns.
func TestBlockedLoggerStallsOnlyTheSink(t *testing.T) {
	t.Parallel()

	log := &blockedLog{entered: make(chan struct{}, 1), release: make(chan struct{})}
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b", Source: "test-source"}, WithBus(newRecBus()), WithLogger(log))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Begin(t.Context(), CaptureDescriptor{EquipmentID: "T1", Device: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	<-log.entered // the rotation's log line holds the sink inside the Logger

	for range 4 * sinkEvents { // more than the queue holds: the surplus is dropped, never waited for
		sinkLogger{p}.Warn("producer: traffic connection lost")
	}
	if err := p.Rotate(t.Context(), CaptureDescriptor{EquipmentID: "T2", Device: "eqp"}); err != nil {
		t.Fatalf("Rotate while the Logger is blocked: %v", err)
	}

	close(log.release)
	p.End(t.Context())
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	after := log.calls.Load()
	sinkLogger{p}.Warn("producer: traffic connection lost") // as a callback nats.go runs after Close would
	if got := log.calls.Load(); got != after {
		t.Errorf("%d Logger calls after Close returned", got-after)
	}
	if after > 1+sinkEvents+2 { // the held line, a full queue, and what End and Rotate queued after the drain began
		t.Errorf("%d Logger calls: the queue did not bound them", after)
	}
}

// gaugeMetrics records which current_capture series are set, and every counter as name or name{reason}.
type gaugeMetrics struct {
	nopMetrics
	mu     sync.Mutex
	set    map[string]bool
	counts map[string]float64
	// hold, when set, blocks every Add until it closes.
	hold chan struct{}
	// final is set once the publisher stopped, so a report after it is Close's; finals counts those reports,
	// each signalling finalEntered and waiting for finalRelease when they are set, then setting finalDone.
	final        atomic.Bool
	finals       atomic.Int32
	finalEntered chan struct{}
	finalRelease chan struct{}
	finalDone    atomic.Bool
	// applied, when set, receives the labels of each current_capture series set.
	applied chan map[string]string
}

func (m *gaugeMetrics) Add(name string, labels map[string]string, delta float64) {
	if m.hold != nil {
		<-m.hold
	}
	if r, ok := labels["reason"]; ok {
		name += "{" + r + "}"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = map[string]float64{}
	}
	m.counts[name] += delta
}

func (m *gaugeMetrics) count(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.counts[name]
}

func (m *gaugeMetrics) Set(name string, labels map[string]string, _ float64) {
	if name == "bus_connected" && m.final.Load() { // once per report
		m.finals.Add(1)
		if m.finalEntered != nil {
			m.finalEntered <- struct{}{}
		}
		if m.finalRelease != nil {
			<-m.finalRelease
		}
		m.finalDone.Store(true)
	}
	if name == "current_capture" {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.set[labels["capture_id"]] = true
		if m.applied != nil {
			m.applied <- labels
		}
	}
}

func (m *gaugeMetrics) Delete(name string, labels map[string]string) {
	if name == "current_capture" {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.set, labels["capture_id"])
	}
}

func (m *gaugeMetrics) series() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Collect(maps.Keys(m.set))
}

// current_capture is set for the capture Begin opens; then the sink is held in the Logger with its queue full when End clears the capture,
// so an event that would delete current_capture has no room; once the sink is released and the producer closed, the gauge is gone all the same.
func TestCurrentCaptureGaugeSurvivesAFullSinkQueue(t *testing.T) {
	t.Parallel()

	log := &blockedLog{entered: make(chan struct{}, 1), release: make(chan struct{})}
	log.pass.Store(true)
	m := &gaugeMetrics{set: map[string]bool{}, applied: make(chan map[string]string, 1)}
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b", Source: "test-source"}, WithBus(newRecBus()), WithLogger(log), WithMetrics(m))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Begin(t.Context(), CaptureDescriptor{EquipmentID: "T1", Device: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// Unless the gauge is seen set, its absence after Close proves nothing.
	select {
	case labels := <-m.applied:
		if want := p.rec.Current().ID.String(); labels["capture_id"] != want {
			t.Fatalf("current_capture set for %s, want Begin's capture %s", labels["capture_id"], want)
		}
	case <-time.After(10 * time.Second): // a guard only: the sink applies it at once
		t.Fatal("current_capture never set for Begin's capture")
	}

	log.pass.Store(false)
	sinkLogger{p}.Warn("producer: traffic connection lost")
	<-log.entered // the line holds the sink inside the Logger
	for range 2 * sinkEvents {
		sinkLogger{p}.Warn("producer: traffic connection lost")
	}
	if rep := p.End(t.Context()); rep.CaptureID == (tracepack.UUID{}) {
		t.Fatalf("End = %+v, want the capture closed", rep)
	}

	close(log.release)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := m.series(); len(got) != 0 {
		t.Errorf("current_capture still set for %v after End and Close", got)
	}
}
