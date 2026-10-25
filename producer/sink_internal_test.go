package producer

import (
	"sync/atomic"
	"testing"
)

// blockedLog blocks every call until release is closed, and counts the calls that began.
type blockedLog struct {
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (l *blockedLog) call() {
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
	p, err := New(Config{SubjectPrefix: "tpk", ClaimBucket: "b"}, WithBus(newRecBus()), WithLogger(log))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Begin(t.Context(), CaptureDescriptor{ToolID: "T1", DeviceName: "eqp"}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	<-log.entered // the rotation's log line holds the sink inside the Logger

	for range 4 * sinkEvents { // more than the queue holds: the surplus is dropped, never waited for
		sinkLogger{p}.Warn("producer: traffic connection lost")
	}
	if err := p.Rotate(t.Context(), CaptureDescriptor{ToolID: "T2", DeviceName: "eqp"}); err != nil {
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
