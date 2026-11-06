// Package metrics is the service's Prometheus metrics and health endpoints (operations.md §10.4):
// a registry, a listener serving /metrics, /healthz and /readyz, and one collector struct per role.
// A component records through a pointer to its role's struct; a nil pointer records nothing.
// Labels are bounded: equipment_id (about 1,500 values), reasons and outcomes; capture_id is never a label.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/pprof" //nolint:gosec // G108: its handlers go on the metrics listener's own mux, only when metrics.pprof is on
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// readyTimeout bounds one /readyz answer.
const readyTimeout = 5 * time.Second

// idleTimeout closes a keep-alive connection that sends no request for this long.
const idleTimeout = 2 * time.Minute

// Server holds the registry and the readiness checks, and serves them.
type Server struct {
	// Pprof, set before Handler or Start, also serves Go's profiles under /debug/pprof/.
	Pprof bool

	reg    *prometheus.Registry
	mu     sync.Mutex
	checks map[string]func(context.Context) error

	srv             *http.Server
	addr            net.Addr
	done            chan error
	shutdownTimeout time.Duration
	idleTimeout     time.Duration
}

// NewServer returns a server whose registry holds the Go runtime's and the process's collectors.
func NewServer() *Server {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return &Server{reg: reg, checks: map[string]func(context.Context) error{}, shutdownTimeout: 5 * time.Second, idleTimeout: idleTimeout}
}

// Registry is the registry the roles register their collectors with.
func (s *Server) Registry() *prometheus.Registry { return s.reg }

// Ready registers, or replaces, the readiness check named name: /readyz answers ok only while every check returns nil,
// and otherwise names each failing check, never its error.
func (s *Server) Ready(name string, check func(context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks[name] = check
}

// Handler serves /metrics, /healthz (the process runs) and /readyz (every check passes), and /debug/pprof/ when Pprof is set.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", s.readyz)
	if s.Pprof {
		// The import also registers them on http.DefaultServeMux, which the service never serves.
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	return mux
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	s.mu.Lock()
	names := make([]string, 0, len(s.checks))
	for name := range s.checks {
		names = append(names, name)
	}
	checks := make(map[string]func(context.Context) error, len(s.checks))
	maps.Copy(checks, s.checks)
	s.mu.Unlock()
	slices.Sort(names)
	// The listener is unauthenticated: the body names each failing check, never its error's text (operations.md §10.4).
	var failed []string
	for _, name := range names {
		if err := checks[name](ctx); err != nil {
			failed = append(failed, name)
		}
	}
	if len(failed) > 0 {
		http.Error(w, "not ready: "+strings.Join(failed, ", "), http.StatusServiceUnavailable)
		return
	}
	_, _ = fmt.Fprintln(w, "ok")
}

// ErrServe marks the error that ended serving the metrics before Close.
var ErrServe = errors.New("metrics: serving ended")

// Start binds addr, failing at once when it cannot, and serves the handler on it in the background until Close;
// should serving end before Close, failed, if not nil, is called with its error, which ErrServe marks.
func (s *Server) Start(ctx context.Context, addr string, failed func(error)) error {
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", addr, err)
	}
	s.serve(l, failed)

	return nil
}

func (s *Server) serve(l net.Listener, failed func(error)) {
	s.srv = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: s.idleTimeout}
	s.addr = l.Addr()
	s.done = make(chan error, 1)
	go func() {
		err := s.srv.Serve(l)
		if !errors.Is(err, http.ErrServerClosed) {
			err = fmt.Errorf("%w: %s: %w", ErrServe, l.Addr(), err)
			if failed != nil {
				failed(err)
			}
		}
		s.done <- err
	}()
}

// Addr is the address Start bound, nil before it.
func (s *Server) Addr() net.Addr { return s.addr }

// Close shuts the listener Start opened down and waits for it, closing the connections still open after shutdownTimeout;
// it returns the error that ended serving, if any.
func (s *Server) Close() error {
	if s.srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	err := s.srv.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, s.srv.Close())
	}
	if serr := <-s.done; !errors.Is(serr, http.ErrServerClosed) {
		err = errors.Join(err, serr)
	}
	s.srv = nil

	return err
}
