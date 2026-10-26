// Package testbus runs an embedded nats-server with JetStream for tests and for tracekeep dev.
//
// A test's server listens on localhost at a random port and keeps its file storage under the test's temporary directory,
// so no test reaches a network or a shared server; Serve runs one with no test to own it.
// The producer SDK never imports this package; its tests use producertest.
package testbus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const readyTimeout = 10 * time.Second

// Options adjusts StartServer; a zero field takes the server's default.
type Options struct {
	// MaxPayload is the server's max_payload.
	MaxPayload int32
	// User and Password, when set, are the one account a client must authenticate as.
	User, Password string
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
		Username:   o.User,
		Password:   o.Password,
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

// RestartWith starts a stopped server again on the same address with the same storage, adjusted by o,
// as a deployment changes its credentials or limits.
func (s *Server) RestartWith(o Options) {
	s.t.Helper()
	s.opts.MaxPayload, s.opts.Username, s.opts.Password = o.MaxPayload, o.User, o.Password
	s.Restart()
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

// StartCluster runs n embedded servers with JetStream clustered over routes on localhost and returns a client URL naming them all,
// once JetStream has a meta leader, so a stream created with n replicas is replicated across the servers.
// The servers are shut down when the test ends.
//
// Parameters:
//   - t: the test that owns the servers
//   - n: the number of servers, three for a stream of three replicas
//
// Returns:
//   - string: a comma-separated nats:// URL list to connect to
func StartCluster(t testing.TB, n int) string {
	t.Helper()
	// Every server routes to every other, so each port is chosen before any server starts.
	// shortcut: another process may take a released port before its server binds it; retry the cluster if that ever flakes.
	routes := make([]*url.URL, n)
	ports := make([]int, n)
	for i := range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("testbus: route port: %v", err)
		}
		addr, ok := l.Addr().(*net.TCPAddr)
		_ = l.Close()
		if !ok {
			t.Fatalf("testbus: route address %v is not TCP", l.Addr())
		}
		ports[i] = addr.Port
		routes[i] = &url.URL{Scheme: "nats-route", Host: fmt.Sprintf("127.0.0.1:%d", ports[i])}
	}
	srvs := make([]*server.Server, n)
	for i := range n {
		opts := server.Options{
			ServerName: fmt.Sprintf("tb-%d", i), Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
			Cluster: server.ClusterOpts{Name: "testbus", Host: "127.0.0.1", Port: ports[i]},
			Routes:  routes,
			NoLog:   true, NoSigs: true,
		}
		srv, err := server.NewServer(&opts)
		if err != nil {
			t.Fatalf("testbus: new server: %v", err)
		}
		go srv.Start()
		t.Cleanup(func() {
			srv.Shutdown()
			srv.WaitForShutdown()
		})
		srvs[i] = srv
	}
	urls := make([]string, n)
	for i, srv := range srvs {
		if !srv.ReadyForConnections(readyTimeout) {
			t.Fatalf("testbus: server %d not ready within %s", i, readyTimeout)
		}
		urls[i] = srv.ClientURL()
	}
	// A stream is placed only once the meta leader knows every peer and each server is current with it.
	await(t, func() bool {
		return slices.ContainsFunc(srvs, func(s *server.Server) bool { return s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == n }) &&
			!slices.ContainsFunc(srvs, func(s *server.Server) bool { return !s.JetStreamIsCurrent() })
	})

	return strings.Join(urls, ",")
}

// await polls f until it returns a value other than the zero one, within the readiness bound:
// no event announces a route listener or a meta leader's election.
func await[T comparable](t testing.TB, f func() T) T {
	t.Helper()
	var zero T
	deadline := time.Now().Add(readyTimeout)
	for {
		if v := f(); v != zero {
			return v
		}
		if time.Now().After(deadline) {
			t.Fatalf("testbus: cluster not formed within %s", readyTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// AwaitStream waits until the replicated stream s answers publishes from js:
// a stream just created on a cluster elects its leader, and its subjects' interest reaches every server, after the creation returns.
// It probes with a publish the stream refuses, expecting a last sequence no stream reaches, so the probe stores nothing.
func AwaitStream(t testing.TB, js jetstream.JetStream, s jetstream.Stream) {
	t.Helper()
	subject := strings.NewReplacer(">", "probe", "*", "probe").Replace(s.CachedInfo().Config.Subjects[0])
	await(t, func() bool {
		_, err := js.Publish(t.Context(), subject, nil, jetstream.WithExpectLastSequence(math.MaxInt64))
		var api *jetstream.APIError

		return errors.As(err, &api) && api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
	})
}

// Snapshot reads a snapshot of stream over nc, its consumers included, as an operator's backup does,
// and returns the stream's configuration and the snapshot's bytes for Restore.
func Snapshot(t testing.TB, nc *nats.Conn, stream string) (server.StreamConfig, []byte) {
	t.Helper()
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("testbus: snapshot: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	req, err := json.Marshal(server.JSApiStreamSnapshotRequest{DeliverSubject: inbox})
	if err != nil {
		t.Fatal(err)
	}
	m, err := nc.Request(fmt.Sprintf(server.JSApiStreamSnapshotT, stream), req, readyTimeout)
	if err != nil {
		t.Fatalf("testbus: snapshot of %s: %v", stream, err)
	}
	var resp server.JSApiStreamSnapshotResponse
	if err := json.Unmarshal(m.Data, &resp); err != nil || resp.Error != nil || resp.Config == nil {
		t.Fatalf("testbus: snapshot of %s: %v %+v", stream, err, resp.Error)
	}
	var data bytes.Buffer
	for {
		chunk, err := sub.NextMsg(readyTimeout)
		if err != nil {
			t.Fatalf("testbus: snapshot of %s: %v", stream, err)
		}
		// The end carries no data, and a status only after an acknowledged chunk or an error; each acknowledged chunk paces the next.
		if len(chunk.Data) == 0 {
			if status := chunk.Header.Get("Status"); status != "" && status != "204" {
				t.Fatalf("testbus: snapshot of %s ended with status %s %s", stream, status, chunk.Header.Get("Description"))
			}

			return *resp.Config, data.Bytes()
		}
		data.Write(chunk.Data)
		if chunk.Reply == "" { // the last chunk asks for no acknowledgement
			continue
		}
		if err := chunk.Respond(nil); err != nil {
			t.Fatalf("testbus: snapshot of %s: %v", stream, err)
		}
	}
}

// Restore restores a stream from a snapshot Snapshot read, as an operator restores a backup:
// the stream must not exist, and it comes back with its messages and its durable consumers,
// under a new creation time on nats-server v2.15.1.
func (s *Server) Restore(t testing.TB, cfg server.StreamConfig, data []byte) {
	t.Helper()
	if _, err := s.srv.GlobalAccount().RestoreStreamV2(&cfg, bytes.NewReader(data)); err != nil {
		t.Fatalf("testbus: restore of %s: %v", cfg.Name, err)
	}
}

// Serve runs an embedded nats-server with JetStream storing under dir, for tracekeep dev, which has no test to own it:
// the caller shuts it down. It listens on localhost, on port when it is not zero and on a free port otherwise.
//
// Parameters:
//   - dir: the JetStream storage directory, kept across runs
//   - port: the client port, 0 for a free one
//
// Returns:
//   - *server.Server: the running server; its ClientURL is the nats:// URL to connect to
//   - error: the server could not start
func Serve(dir string, port int) (*server.Server, error) {
	if port == 0 {
		port = -1
	}
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: port, JetStream: true, StoreDir: dir, NoLog: true, NoSigs: true,
	})
	if err != nil {
		return nil, fmt.Errorf("testbus: new server: %w", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(readyTimeout) {
		srv.Shutdown()
		return nil, fmt.Errorf("testbus: server not ready within %s", readyTimeout)
	}

	return srv, nil
}
