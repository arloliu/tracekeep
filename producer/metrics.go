package producer

// Metrics receives the producer's measurements; an integrator wires it to its own registry (no Prometheus dependency).
// The producer calls it only from its own goroutines, never from an observer callback:
// callbacks count into atomics the publisher reports from,
// so a slow or allocating implementation cannot reach the connection's critical path.
// Names and labels are listed in docs/specs/tracekeep/operations.md.
type Metrics interface {
	// Add adds delta to the counter name with labels.
	Add(name string, labels map[string]string, delta float64)
	// Set sets the gauge name with labels.
	Set(name string, labels map[string]string, value float64)
	// Observe records value in the histogram name with labels.
	Observe(name string, labels map[string]string, value float64)
	// Delete removes the series name with labels, as current_capture's previous label set at a rotation.
	Delete(name string, labels map[string]string)
}

// Logger receives the producer's log lines; *slog.Logger satisfies it.
// The producer logs only from its own goroutines, never from an observer callback,
// and an alert is one Warn or Error per state change, not per record.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type nopMetrics struct{}

var _ Metrics = nopMetrics{}

type nopLogger struct{}

var _ Logger = nopLogger{}

func (nopMetrics) Add(string, map[string]string, float64)     {}
func (nopMetrics) Set(string, map[string]string, float64)     {}
func (nopMetrics) Observe(string, map[string]string, float64) {}
func (nopMetrics) Delete(string, map[string]string)           {}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
