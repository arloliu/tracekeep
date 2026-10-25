// Package testbus runs an embedded nats-server with JetStream for tests.
//
// The server listens on localhost at a random port and keeps its file storage under the test's temporary directory,
// so no test reaches a network or a shared server.
// The producer SDK never imports this package; its tests use producertest.
package testbus

import (
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

const readyTimeout = 10 * time.Second

// Options adjusts StartServer; a zero field takes the server's default.
type Options struct {
	// MaxPayload is the server's max_payload.
	MaxPayload int32
}

// Server is an embedded server that a test can stop and start again on the same address and storage,
// as a traffic bus goes down and comes back.
type Server struct {
	t    testing.TB
	opts server.Options
	srv  *server.Server
	url  string
}

// Start runs an embedded nats-server with JetStream and returns its client URL.
//
// The server is shut down when the test ends, before its storage directory is removed.
//
// Parameters:
//   - t: the test that owns the server
//
// Returns:
//   - string: the nats:// URL to connect to
func Start(t testing.TB) string {
	t.Helper()

	return StartServer(t, Options{}).URL()
}

// StartServer runs an embedded nats-server with JetStream, adjusted by o, and returns it.
//
// The server is shut down when the test ends, before its storage directory is removed.
//
// Parameters:
//   - t: the test that owns the server
//   - o: the settings that differ from the server's defaults
//
// Returns:
//   - *Server: the running server
func StartServer(t testing.TB, o Options) *Server {
	t.Helper()

	// TempDir first: cleanups run in reverse, so the server stops before its store is removed.
	s := &Server{t: t, opts: server.Options{
		Host:       "127.0.0.1",
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		MaxPayload: o.MaxPayload,
		NoLog:      true,
		NoSigs:     true,
	}}
	s.start()
	// Restarts reuse the first port, so the clients' URLs stay valid.
	addr, ok := s.srv.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("testbus: server address %v is not TCP", s.srv.Addr())
	}
	s.opts.Port = addr.Port
	s.url = s.srv.ClientURL()
	t.Cleanup(s.Stop)

	return s
}

// URL returns the nats:// URL to connect to; it does not change across a restart.
func (s *Server) URL() string { return s.url }

// Stop shuts the server down and waits until it is gone; its clients see the connection close.
// Stopping a stopped server does nothing.
func (s *Server) Stop() {
	if s.srv == nil {
		return
	}
	s.srv.Shutdown()
	s.srv.WaitForShutdown()
	s.srv = nil
}

// Restart starts a stopped server again on the same address with the same storage.
func (s *Server) Restart() {
	s.t.Helper()
	if s.srv != nil {
		s.t.Fatal("testbus: restart of a running server")
	}
	s.start()
}

func (s *Server) start() {
	s.t.Helper()
	opts := s.opts
	srv, err := server.NewServer(&opts)
	if err != nil {
		s.t.Fatalf("testbus: new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(readyTimeout) {
		srv.Shutdown()
		s.t.Fatalf("testbus: server not ready within %s", readyTimeout)
	}
	s.srv = srv
}
