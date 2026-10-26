package metrics

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Body)

	return rec.Code, string(body)
}

// /healthz answers while the process runs; /readyz answers ok only when every registered check passes, naming a failing one;
// /metrics serves the registry.
func TestServer(t *testing.T) {
	t.Parallel()
	s := NewServer()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "tracekeep_test_total", Help: "a test counter"})
	s.Registry().MustRegister(c)
	c.Inc()
	h := s.Handler()
	if code, _ := get(t, h, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", code)
	}
	if code, _ := get(t, h, "/readyz"); code != http.StatusOK {
		t.Errorf("/readyz with no check = %d, want 200", code)
	}
	failing := errors.New("consumer not bound")
	s.Ready("ingest", func(context.Context) error { return failing })
	if code, body := get(t, h, "/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "ingest: consumer not bound") {
		t.Errorf("/readyz with a failing check = %d %q, want 503 naming it", code, body)
	}
	failing = nil
	s.Ready("ingest", func(context.Context) error { return nil })
	if code, _ := get(t, h, "/readyz"); code != http.StatusOK {
		t.Errorf("/readyz once ready = %d, want 200", code)
	}
	if code, body := get(t, h, "/metrics"); code != http.StatusOK || !strings.Contains(body, "tracekeep_test_total 1") {
		t.Errorf("/metrics = %d, want the registry's counter", code)
	}
}

// Serve listens until its context ends.
func TestServe(t *testing.T) {
	t.Parallel()
	s := NewServer()
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, "127.0.0.1:0") }()
	stop()
	if err := <-done; err != nil {
		t.Errorf("Serve = %v, want nil at its context's end", err)
	}
}
