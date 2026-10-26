// Package metrics is the service's Prometheus metrics and health endpoints (operations.md §10.4):
// a registry, a listener serving /metrics, /healthz and /readyz, and one collector struct per role.
// A component records through a pointer to its role's struct; a nil pointer records nothing.
// Labels are bounded: tool_id (about 1,500 values), reasons and outcomes; capture_id is never a label.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// readyTimeout bounds one /readyz answer.
const readyTimeout = 5 * time.Second

// Server holds the registry and the readiness checks, and serves them.
type Server struct {
	reg    *prometheus.Registry
	mu     sync.Mutex
	checks map[string]func(context.Context) error
}

// NewServer returns a server whose registry holds the Go runtime's and the process's collectors.
func NewServer() *Server {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return &Server{reg: reg, checks: map[string]func(context.Context) error{}}
}

// Registry is the registry the roles register their collectors with.
func (s *Server) Registry() *prometheus.Registry { return s.reg }

// Ready registers, or replaces, the readiness check named name: /readyz answers ok only while every check returns nil.
func (s *Server) Ready(name string, check func(context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks[name] = check
}

// Handler serves /metrics, /healthz (the process runs) and /readyz (every check passes).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", s.readyz)

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
	for k, v := range s.checks {
		checks[k] = v
	}
	s.mu.Unlock()
	slices.Sort(names)
	var errs []error
	for _, name := range names {
		if err := checks[name](ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	_, _ = fmt.Fprintln(w, "ok")
}

// Serve serves the handler on addr until ctx ends, then shuts the listener down.
func (s *Server) Serve(ctx context.Context, addr string) error {
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", addr, err)
	}
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(l) }()
	select {
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err := srv.Shutdown(sctx)
		if serr := <-done; !errors.Is(serr, http.ErrServerClosed) {
			err = errors.Join(err, serr)
		}

		return err
	case err := <-done:
		return fmt.Errorf("metrics: serve %s: %w", addr, err)
	}
}
