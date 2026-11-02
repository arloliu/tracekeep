package metrics

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Body)

	return rec.Code, string(body)
}

// /healthz answers while the process runs; /readyz answers ok only when every registered check passes, naming a failing one without its error's text;
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
	// A check's error may name a driver's host or hold a credential; the unauthenticated body names the check only.
	failing := errors.New("consumer not bound: dial tcp db.example.invalid:3306: user tk password=pw-sentinel")
	s.Ready("ingest", func(context.Context) error { return failing })
	s.Ready("compact", func(context.Context) error { return nil })
	code, body := get(t, h, "/readyz")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "ingest") || strings.Contains(body, "compact") {
		t.Errorf("/readyz with a failing check = %d %q, want 503 naming it alone", code, body)
	}
	for _, leak := range []string{"consumer not bound", "db.example.invalid", "3306", "pw-sentinel"} {
		if strings.Contains(body, leak) {
			t.Errorf("/readyz body %q holds %q from the check's error", body, leak)
		}
	}
	s.Ready("ingest", func(context.Context) error { return nil })
	if code, _ := get(t, h, "/readyz"); code != http.StatusOK {
		t.Errorf("/readyz once ready = %d, want 200", code)
	}
	if code, body := get(t, h, "/metrics"); code != http.StatusOK || !strings.Contains(body, "tracekeep_test_total 1") {
		t.Errorf("/metrics = %d, want the registry's counter", code)
	}
}

// Start binds at once, failing on an address it cannot bind, and serves until Close.
func TestStartAndClose(t *testing.T) {
	t.Parallel()
	s := NewServer()
	if err := s.Start(t.Context(), "127.0.0.1:0", nil); err != nil {
		t.Fatal(err)
	}
	if err := NewServer().Start(t.Context(), s.Addr().String(), nil); err == nil {
		t.Error("Start on a bound address = nil, want its error")
	}
	resp, err := http.Get("http://" + s.Addr().String() + "/healthz") //nolint:noctx // a test against a local listener
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if err := s.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	if _, err := http.Get("http://" + s.Addr().String() + "/healthz"); err == nil { //nolint:noctx,bodyclose // fails to connect
		t.Error("served after Close")
	}
}

// Serving that ends before Close, its listener failing, is reported to failed, and Close returns it too.
func TestServingFailureReported(t *testing.T) {
	t.Parallel()
	s := NewServer()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	reported := make(chan error, 1)
	s.serve(l, func(err error) { reported <- err })
	_ = l.Close()
	if err := <-reported; !errors.Is(err, ErrServe) {
		t.Errorf("reported %v, want ErrServe", err)
	}
	if err := s.Close(); !errors.Is(err, ErrServe) {
		t.Errorf("Close = %v, want the serving error", err)
	}
}

// A scrape still running when the shutdown times out is cut off: Close returns and the request's context ends.
func TestCloseCutsOffABlockedRequest(t *testing.T) {
	t.Parallel()
	s := NewServer()
	s.shutdownTimeout = 10 * time.Millisecond
	entered, ended := make(chan struct{}), make(chan struct{})
	s.Ready("blocked", func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		close(ended)

		return ctx.Err()
	})
	if err := s.Start(t.Context(), "127.0.0.1:0", nil); err != nil {
		t.Fatal(err)
	}
	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+s.Addr().String()+"/readyz", nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered
	if err := s.Close(); err == nil {
		t.Error("Close = nil, want the shutdown's timeout")
	}
	select {
	case <-ended:
	case <-time.After(readyTimeout / 2):
		t.Error("the blocked request outlived Close")
	}
}

// A connection idle past the idle timeout is closed, so idle clients never hold the listener's connections for good.
func TestIdleConnectionClosed(t *testing.T) {
	t.Parallel()
	s := NewServer()
	s.idleTimeout = 50 * time.Millisecond
	if err := s.Start(t.Context(), "127.0.0.1:0", nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: metrics\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second)) // bounds a connection never closed
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Errorf("reading an idle connection = %v, want EOF: the server closes it", err)
	}
}
