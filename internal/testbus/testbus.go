// Package testbus runs an embedded nats-server with JetStream for tests.
//
// The server listens on localhost at a random port and keeps its file storage under the test's temporary directory,
// so no test reaches a network or a shared server.
// The producer SDK never imports this package; its tests use producertest.
package testbus

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

const readyTimeout = 10 * time.Second

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

	// TempDir first: cleanups run in reverse, so the server stops before its store is removed.
	storeDir := t.TempDir()

	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  storeDir,
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("testbus: new server: %v", err)
	}

	go srv.Start()
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	if !srv.ReadyForConnections(readyTimeout) {
		t.Fatalf("testbus: server not ready within %s", readyTimeout)
	}

	return srv.ClientURL()
}
