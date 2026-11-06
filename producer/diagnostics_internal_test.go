package producer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

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
	s := level + " " + msg + " " + fmt.Sprint(args)
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

// logSentinels are what an error's text may carry and no log line of the SDK may:
// a URL's user information and query, a token, a password, an object name and a backend's text.
var logSentinels = []string{"sentinel-user", "sentinel-password", "sentinel-query", "sentinel-token", "sentinel-object", "sentinel-backend"}

// SentinelError returns an error nested twice whose text holds every one of logSentinels.
func SentinelError() error {
	u := &url.Error{
		Op:  "dial",
		URL: "nats://sentinel-user:sentinel-password@bus.invalid:4222/?q=sentinel-query&auth=sentinel-token",
		Err: errors.New("sentinel-backend refused sentinel-object"),
	}

	return fmt.Errorf("outer: %w", fmt.Errorf("middle: %w", u))
}

// CheckFieldLine fails t when line holds any of logSentinels or describes no error by a field the SDK builds.
func CheckFieldLine(t *testing.T, line string) {
	t.Helper()
	for _, s := range logSentinels {
		if strings.Contains(line, s) {
			t.Errorf("line %q holds %s", line, s)
		}
	}
	if !strings.Contains(line, "class ") && !strings.Contains(line, "status ") && !strings.Contains(line, "context ") {
		t.Errorf("line %q describes no error by its fields", line)
	}
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
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b", Source: "test-source"}, all...)
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
					d.Device = "eqp-2"
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

// Close judges quiescence by the count after its mark, never by a zero an earlier emission saw:
// emitter A leaves the count at zero and holds before acting on it, emitter B is admitted and holds before its enqueue,
// Close sets its mark and waits; A resumes and must not settle Close, which waits for B, delivered or counted before Close returns.
func TestStaleZeroDoesNotSettleClose(t *testing.T) {
	t.Parallel()
	lines := NewLineLog()
	m := &gaugeMetrics{set: map[string]bool{}}
	p := newSinkProducer(t, lines, m)
	aLeft, aGo, bHeld, bGo, marked := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var holdLeft, holdAdmitted atomic.Bool
	p.left = func() {
		if holdLeft.CompareAndSwap(true, false) {
			close(aLeft)
			<-aGo
		}
	}
	p.admitted = func() {
		if holdAdmitted.CompareAndSwap(true, false) {
			close(bHeld)
			<-bGo
		}
	}
	p.marked = func() { close(marked) }

	holdLeft.Store(true)
	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		sinkLogger{p}.Warn("producer: line A")
	}()
	<-aLeft // A left the count at zero
	holdAdmitted.Store(true)
	bDone := make(chan struct{})
	go func() {
		defer close(bDone)
		sinkLogger{p}.Warn("producer: line B")
	}()
	<-bHeld // B is admitted, its enqueue not made
	closed := closeAsync(p)
	<-marked // Close set its mark; B is in flight
	close(aGo)
	<-aDone
	select {
	case <-p.settled:
		t.Fatal("Close settled on A's stale zero with B in flight")
	default:
	}
	close(bGo)
	<-bDone
	<-closed

	delivered := 0
	for _, l := range lines.Lines() {
		if strings.Contains(l, "line B") {
			delivered++
		}
	}
	if dropped := m.count("log_lines_dropped_total"); delivered+int(dropped) != 1 {
		t.Errorf("B delivered %d times and counted dropped %v times, want once in all", delivered, dropped)
	}
	if n := len(p.events); n != 0 {
		t.Errorf("%d lines left queued after Close", n)
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

// The connection's disconnect, async-error and closed callbacks log an error by the fields the SDK builds, never its text.
func TestBusCallbacksLogFieldsNotText(t *testing.T) {
	t.Parallel()
	tun := Tunables{}.withDefaults()
	b, err := newNATSBus(&Config{Servers: []string{"nats://127.0.0.1:4222"}}, &tun, nil)
	if err != nil {
		t.Fatalf("newNATSBus: %v", err)
	}
	log := NewLineLog()
	b.log = log
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"nested", SentinelError(), "class other"},
		{"jetstream", fmt.Errorf("publish: %w", &jetstream.APIError{Code: 503, ErrorCode: 10077, Description: "sentinel-backend holds sentinel-object"}), "status 503 error_code 10077"},
		{"deadline", fmt.Errorf("%w: %w", context.DeadlineExceeded, SentinelError()), "context deadline_exceeded"},
		{"network", fmt.Errorf("read: %w", &net.OpError{Op: "dial", Net: "tcp", Err: SentinelError()}), "class network"},
		{"nats", fmt.Errorf("%w: %w", nats.ErrAuthorization, SentinelError()), "class authorization"},
	} {
		before := len(log.Lines())
		b.opts.DisconnectedErrCB(nil, tc.err)
		b.opts.AsyncErrorCB(nil, nil, tc.err)
		b.closedWith(tc.err)
		lines := log.Lines()[before:]
		if len(lines) != 3 {
			t.Fatalf("%s: %d lines, want 3", tc.name, len(lines))
		}
		for _, l := range lines {
			CheckFieldLine(t, l)
			if !strings.Contains(l, tc.want) {
				t.Errorf("%s: line %q, want %q", tc.name, l, tc.want)
			}
		}
	}
}

// failDialer fails every dial with err.
type failDialer struct{ err error }

func (d failDialer) Dial(string, string) (net.Conn, error) { return nil, d.err }

// The first dial's failure is logged by its fields: neither the server URL's credentials nor the dialer's text reach the line.
func TestFirstDialFailureLogsFieldsNotText(t *testing.T) {
	t.Parallel()
	tun := Tunables{}.withDefaults()
	cfg := Config{Servers: []string{"nats://sentinel-user:sentinel-password@127.0.0.1:1/?q=sentinel-query"}}
	b, err := newNATSBus(&cfg, &tun, nil)
	if err != nil {
		t.Fatalf("newNATSBus: %v", err)
	}
	log := NewLineLog()
	b.log = log
	b.opts.RetryOnFailedConnect = false
	b.opts.CustomDialer = failDialer{err: &net.OpError{Op: "dial", Net: "tcp", Err: SentinelError()}}
	b.dial(&tun)
	lines := log.Lines()
	if len(lines) != 1 {
		t.Fatalf("lines %q, want the dial's one", lines)
	}
	CheckFieldLine(t, lines[0])
}

// infoFailBus is a recBus whose stream reads fail with err.
type infoFailBus struct {
	*recBus
	err error
}

func (b infoFailBus) StreamInfo(context.Context, string) (StreamInfo, error) {
	return StreamInfo{}, b.err
}

// The automatic preflight's failure is logged by its fields, never the bus error's text.
func TestAutomaticPreflightFailureLogsFieldsNotText(t *testing.T) {
	t.Parallel()
	log := NewLineLog()
	p := newSinkProducer(t, log, nopMetrics{}, WithBus(infoFailBus{recBus: newRecBus(), err: SentinelError()}))
	p.autoPreflight()
	select {
	case l := <-log.Seen:
		CheckFieldLine(t, l)
	case <-t.Context().Done():
		t.Fatal("no preflight line")
	}
}
