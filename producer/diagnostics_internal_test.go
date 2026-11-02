package producer

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// LineLog records every whole log line, its level, message and fields, as one string,
// and signals each on Seen while it has room; the package's external tests use it too.
type LineLog struct {
	mu    sync.Mutex
	lines []string
	Seen  chan string
}

// NewLineLog returns a LineLog whose Seen holds 64 lines.
func NewLineLog() *LineLog { return &LineLog{Seen: make(chan string, 64)} }

func (l *LineLog) line(level, msg string, args []any) {
	s := level + " " + msg + " " + fmt.Sprint(args...)
	l.mu.Lock()
	l.lines = append(l.lines, s)
	l.mu.Unlock()
	select {
	case l.Seen <- s:
	default:
	}
}

// Info implements Logger.
func (l *LineLog) Info(msg string, args ...any) { l.line("INFO", msg, args) }

// Warn implements Logger.
func (l *LineLog) Warn(msg string, args ...any) { l.line("WARN", msg, args) }

// Error implements Logger.
func (l *LineLog) Error(msg string, args ...any) { l.line("ERROR", msg, args) }

// Lines returns the lines recorded so far.
func (l *LineLog) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.lines)
}

// closeAsync runs Close on a goroutine of its own and closes the returned channel once it returned.
func closeAsync(p *Producer) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = p.Close()
	}()

	return done
}

// newSinkProducer builds a producer on the package's recBus with log, m and the extra options.
func newSinkProducer(t *testing.T, log Logger, m Metrics, opts ...Option) *Producer {
	t.Helper()
	all := append([]Option{WithBus(newRecBus()), WithLogger(log), WithMetrics(m)}, opts...)
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	return p
}

// A Logger or a Metrics held while Begin, more Rotates than the sink's queue holds, and End run
// delays no lifecycle call and loses no rotation: each is counted where it happens and reported once released.
func TestRotationsAreCountedWhileTheIntegratorIsHeld(t *testing.T) {
	t.Parallel()
	const rotations = sinkEvents + 44
	for _, held := range []string{"logger", "metrics"} {
		t.Run(held, func(t *testing.T) {
			t.Parallel()
			log := &blockedLog{entered: make(chan struct{}, 1), release: make(chan struct{})}
			m := &gaugeMetrics{set: map[string]bool{}}
			release := log.release
			if held == "metrics" {
				close(log.release)
				m.hold = make(chan struct{})
				release = m.hold
			}
			p := newSinkProducer(t, log, m)
			if err := p.Begin(t.Context(), testDescriptor()); err != nil {
				t.Fatalf("Begin: %v", err)
			}
			for i := range rotations {
				d := testDescriptor()
				if i%2 == 0 {
					d.DeviceName = "eqp-2"
				}
				if err := p.Rotate(t.Context(), d); err != nil {
					t.Fatalf("Rotate %d: %v", i, err)
				}
			}
			// End's deadline bounds its wait for a publisher held in Metrics; it synchronizes nothing.
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if rep := p.End(ctx); rep.CaptureID == (tracepack.UUID{}) {
				t.Fatalf("End = %+v, want the capture closed", rep)
			}

			close(release)
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			for reason, want := range map[string]float64{"start": 1, "hotreload": rotations, "stop": 1, "fault": 0, "seq_limit": 0} {
				if got := m.count("capture_rotations_total{" + reason + "}"); got != want {
					t.Errorf("capture_rotations_total{%s} = %v, want %v", reason, got, want)
				}
			}
		})
	}
}

// With the Logger held, the lines the sink's queue has no room for are counted where they are dropped,
// and the lines still queued when Close stops the sink are counted too, none of them reaching the Logger.
func TestDroppedLinesAreCounted(t *testing.T) {
	t.Parallel()
	const lines = sinkEvents + 44
	log := &blockedLog{entered: make(chan struct{}, 1), release: make(chan struct{})}
	m := &gaugeMetrics{set: map[string]bool{}}
	p := newSinkProducer(t, log, m)
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	<-log.entered // the rotation's line holds the sink inside the Logger, its queue empty
	for range lines {
		sinkLogger{p}.Warn("producer: traffic connection lost")
	}
	if got := p.linesDropped.Load(); got != lines-sinkEvents {
		t.Errorf("%d lines counted dropped with the queue full, want %d", got, lines-sinkEvents)
	}

	closed := closeAsync(p)
	<-p.sinkQuit // barrier: Close stops the sink with its queue still full
	close(log.release)
	<-closed
	if got := m.count("log_lines_dropped_total"); got != lines {
		t.Errorf("log_lines_dropped_total = %v, want %d: the overflow and every line still queued", got, lines)
	}
	if got := log.calls.Load(); got != 1 {
		t.Errorf("%d Logger calls, want only the held one", got)
	}
	if n := len(p.events); n != 0 {
		t.Errorf("%d lines left queued after Close", n)
	}
}

// The fault worker's last lines, emitted after the publisher's last report with the queue full,
// are counted by Close's own final report.
func TestFaultWorkersLastEventIsReportedByClose(t *testing.T) {
	t.Parallel()
	log := &blockedLog{entered: make(chan struct{}, 1), release: make(chan struct{})}
	m := &gaugeMetrics{set: map[string]bool{}}
	stopped := make(chan struct{})
	p := newSinkProducer(t, log, m, func(o *options) { o.hooks.stopped = func() { close(stopped) } })
	gap := make(chan struct{})
	p.faultGap = func() {
		close(gap)
		<-stopped // the fault worker goes on only once the publisher made its last report
	}
	if err := p.Begin(t.Context(), testDescriptor()); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	<-log.entered
	for range sinkEvents {
		sinkLogger{p}.Warn("producer: traffic connection lost")
	}
	c := p.rec.Current()
	c.Fault()
	p.rec.SignalFault(c)
	<-gap // barrier: the fault worker holds between its two lifecycle-lock sections

	closed := closeAsync(p)
	<-p.sinkQuit
	close(log.release)
	<-closed
	// The fault's line and its abandoned rotation's line, both past a full queue, and the queue Close found.
	if got, want := m.count("log_lines_dropped_total"), float64(2+sinkEvents); got != want {
		t.Errorf("log_lines_dropped_total = %v, want %v", got, want)
	}
}

// A line emitted after Close's mark, as a bus callback nats.go runs late would, is neither queued nor counted.
func TestLineAfterTheCloseMarkIsDiscarded(t *testing.T) {
	t.Parallel()
	p := newSinkProducer(t, nopLogger{}, &gaugeMetrics{set: map[string]bool{}})
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dropped := p.linesDropped.Load()
	for range sinkEvents + 1 {
		sinkLogger{p}.Warn("producer: traffic connection lost")
	}
	if n := len(p.events); n != 0 {
		t.Errorf("%d lines queued after Close", n)
	}
	if got := p.linesDropped.Load(); got != dropped {
		t.Errorf("%d lines counted dropped after Close", got-dropped)
	}
}

// An emission admitted before Close's mark and held before its enqueue keeps Close at the mark;
// released, it is delivered or counted once before the final report, with room in the queue and without,
// and nothing stays queued.
func TestEmissionInFlightSettlesBeforeTheFinalReport(t *testing.T) {
	t.Parallel()
	for _, full := range []bool{false, true} {
		t.Run(map[bool]string{false: "room", true: "full"}[full], func(t *testing.T) {
			t.Parallel()
			lines := NewLineLog()
			blocked := &blockedLog{entered: make(chan struct{}, 1), release: make(chan struct{})}
			var log Logger = lines
			if full {
				log = blocked
			}
			m := &gaugeMetrics{set: map[string]bool{}}
			p := newSinkProducer(t, log, m)
			held, release, marked := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var armed atomic.Bool
			p.admitted = func() {
				if armed.CompareAndSwap(true, false) {
					close(held)
					<-release
				}
			}
			p.marked = func() { close(marked) }
			if full {
				if err := p.Begin(t.Context(), testDescriptor()); err != nil {
					t.Fatalf("Begin: %v", err)
				}
				<-blocked.entered
				for range sinkEvents {
					sinkLogger{p}.Warn("producer: traffic connection lost")
				}
			}
			armed.Store(true)
			go sinkLogger{p}.Warn("producer: held line")
			<-held

			closed := closeAsync(p)
			<-marked // barrier: Close set its mark and waits for the emission in flight
			select {
			case <-closed:
				t.Fatal("Close returned with an emission in flight")
			default:
			}
			close(release)
			if full {
				<-p.sinkQuit
				close(blocked.release)
			}
			<-closed

			dropped := m.count("log_lines_dropped_total")
			if full {
				if dropped != 1+sinkEvents {
					t.Errorf("log_lines_dropped_total = %v, want the held line and the queue", dropped)
				}
			} else {
				delivered := 0
				for _, l := range lines.Lines() {
					if strings.Contains(l, "held line") {
						delivered++
					}
				}
				if delivered+int(dropped) != 1 {
					t.Errorf("the held line delivered %d times and counted dropped %v times, want once in all", delivered, dropped)
				}
			}
			if n := len(p.events); n != 0 {
				t.Errorf("%d lines left queued after Close", n)
			}
		})
	}
}

// Close called twice and concurrently makes one final report, and both calls return only after it.
func TestConcurrentCloseReportsOnce(t *testing.T) {
	t.Parallel()
	m := &gaugeMetrics{set: map[string]bool{}, finalEntered: make(chan struct{}, 1), finalRelease: make(chan struct{})}
	p := newSinkProducer(t, nopLogger{}, m, func(o *options) { o.hooks.stopped = func() { m.final.Store(true) } })

	first := closeAsync(p)
	<-m.finalEntered // barrier: the first Close is inside its final report
	var early atomic.Bool
	second := make(chan struct{})
	go func() {
		defer close(second)
		_ = p.Close()
		early.Store(!m.finalDone.Load())
	}()
	select {
	case <-first:
		t.Fatal("the first Close returned inside its final report")
	default:
	}
	close(m.finalRelease)
	<-first
	<-second
	_ = p.Close()

	if early.Load() {
		t.Error("the second Close returned before the final report")
	}
	if got := m.finals.Load(); got != 1 {
		t.Errorf("%d final reports, want 1", got)
	}
}
