package producer

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/producer/busconfig"
)

// asyncMaxPending bounds the asynchronous publishes awaiting their ack;
// past it a publish stalls for StallWait and then fails, which the publisher retries as a failed attempt.
const asyncMaxPending = 4096

// natsBus is the default Bus: nats.go's JetStream over the connection New dials or WithConn brings
// (docs/specs/tracekeep/producer-sdk.md §4.7).
// Every publish carries the message id and the expected stream, and no client retries:
// every retry is the publisher's own.
// A dialed bus has no connection until its first dial returns; until then it reports no max_payload,
// so the publisher counts it as down, and every other call fails with errNotConnected.
type natsBus struct {
	stall time.Duration
	// owned is set when the SDK dials the connection, so close closes it.
	owned bool
	// opts are the dial's options, applied once at New so a configuration error fails New.
	opts nats.Options
	// log takes the connection's state changes; New points it at the producer's bounded sink.
	log Logger

	conn atomic.Pointer[natsConn]
	// up is closed once conn is set.
	up     chan struct{}
	mu     sync.Mutex
	closed bool
}

// natsConn is a connection with the JetStream context built on it.
type natsConn struct {
	nc *nats.Conn
	js jetstream.JetStream
}

var _ Bus = (*natsBus)(nil)

type natsFuture struct{ f jetstream.PubAckFuture }

type natsObjects struct{ os jetstream.ObjectStore }

// errNotConnected reports a call on a dialed bus whose first dial has not returned.
var errNotConnected = errors.New("producer: traffic connection not established yet")

// newNATSBus builds the default bus over nc, or, when nc is nil, one that dial connects from cfg.
func newNATSBus(cfg *Config, tun *Tunables, nc *nats.Conn) (*natsBus, error) {
	b := &natsBus{stall: tun.StallWait, owned: nc == nil, up: make(chan struct{}), log: nopLogger{}}
	if nc != nil {
		if nc.Opts.ReconnectBufSize >= 0 {
			return nil, fmt.Errorf("%w: the connection's reconnect buffer is enabled; set nats.ReconnectBufSize(-1)", ErrInvalidConfig)
		}
		c, err := newNATSConn(nc, tun)
		if err != nil {
			return nil, err
		}
		b.conn.Store(c)
		close(b.up)

		return b, nil
	}
	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("%w: no servers, and neither WithConn nor WithBus", ErrInvalidConfig)
	}
	opts, err := dialOptions(cfg, b)
	if err != nil {
		return nil, err
	}
	b.opts = nats.GetDefaultOptions()
	for _, s := range cfg.Servers {
		b.opts.Servers = append(b.opts.Servers, strings.TrimSpace(s))
	}
	for _, opt := range opts {
		if err := opt(&b.opts); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
		}
	}

	return b, nil
}

func newNATSConn(nc *nats.Conn, tun *Tunables) (*natsConn, error) {
	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncMaxPending(asyncMaxPending),
		jetstream.WithPublishAsyncTimeout(tun.PublishTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("producer: jetstream: %w", err)
	}

	return &natsConn{nc: nc, js: js}, nil
}

// dialOptions are every correctness-bearing option the SDK owns for its connection:
// the reconnect buffer disabled, so a publish while disconnected fails at once (producer.md §4.2 *Large records*),
// unlimited reconnects, even past a repeated authentication error, which nats.go would otherwise take as final,
// and a failed first connection retried in the background.
// The connection's state changes go to b.log, read when nats.go calls back.
func dialOptions(cfg *Config, b *natsBus) ([]nats.Option, error) {
	auth, err := authOptions(&cfg.Auth)
	if err != nil {
		return nil, err
	}
	opts := append([]nats.Option{
		nats.Name("tracekeep-producer"),
		nats.ReconnectBufSize(-1),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
		nats.IgnoreAuthErrorAbort(),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			b.log.Warn("producer: traffic connection lost", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			b.log.Info("producer: traffic connection restored", "server", nc.ConnectedUrlRedacted())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			b.log.Warn("producer: traffic connection error", "error", err)
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if err := nc.LastError(); err != nil {
				b.log.Error("producer: traffic connection closed", "error", err)
			}
		}),
	}, auth...)

	return append(opts, tlsOptions(&cfg.TLS)...), nil
}

// dial makes the first connection, which nats.go attempts on the calling goroutine before it retries in the background,
// so New runs it on a goroutine of its own and never waits for the cluster (producer.md §4.3).
// A bus closed meanwhile closes the connection as soon as it exists.
func (b *natsBus) dial(tun *Tunables) {
	nc, err := b.opts.Connect()
	if err != nil {
		b.log.Error("producer: traffic connection cannot be made", "error", err)
		return
	}
	c, err := newNATSConn(nc, tun)
	if err != nil {
		nc.Close()
		b.log.Error("producer: traffic connection cannot be made", "error", err)

		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		nc.Close()
		return
	}
	b.conn.Store(c)
	close(b.up)
}

// authOptions returns the one authentication option a configures, or none.
func authOptions(a *AuthConfig) ([]nats.Option, error) {
	switch {
	case a.CredsFile != "":
		return []nats.Option{nats.UserCredentials(a.CredsFile)}, nil
	case a.NKeySeedFile != "":
		opt, err := nats.NkeyOptionFromSeed(a.NKeySeedFile)
		if err != nil {
			return nil, fmt.Errorf("%w: nkey seed: %w", ErrInvalidConfig, err)
		}

		return []nats.Option{opt}, nil
	case a.User != "":
		return []nats.Option{nats.UserInfo(a.User, a.Password)}, nil
	case a.Token != "":
		return []nats.Option{nats.Token(a.Token)}, nil
	default:
		return nil, nil
	}
}

// tlsOptions turns TLS on when any TLS material is configured; a tls:// server URL turns it on as well.
func tlsOptions(t *TLSConfig) []nats.Option {
	if t.CAFile == "" && t.CertFile == "" && t.ServerName == "" {
		return nil
	}
	// Secure first: RootCAs and ClientCert amend the configuration it installs.
	opts := []nats.Option{nats.Secure(&tls.Config{ServerName: t.ServerName, MinVersion: tls.VersionTLS12})}
	if t.CAFile != "" {
		opts = append(opts, nats.RootCAs(t.CAFile))
	}
	if t.CertFile != "" {
		opts = append(opts, nats.ClientCert(t.CertFile, t.KeyFile))
	}

	return opts
}

// natsMsg copies m's headers into a nats.Msg.
func natsMsg(m *Msg) *nats.Msg {
	h := make(nats.Header, len(m.Header))
	for k, v := range m.Header {
		h[k] = []string{v}
	}

	return &nats.Msg{Subject: m.Subject, Header: h, Data: m.Data}
}

// MaxPayload implements Bus; it is 0 until the first connection reports the server's limit.
func (b *natsBus) MaxPayload() int {
	c := b.conn.Load()
	if c == nil {
		return 0
	}

	return int(c.nc.MaxPayload())
}

// Publish implements Bus with the synchronous option set: the caller's deadline and no stall wait.
func (b *natsBus) Publish(ctx context.Context, m *Msg) (Ack, error) {
	c := b.conn.Load()
	if c == nil {
		return Ack{}, errNotConnected
	}
	ack, err := c.js.PublishMsg(ctx, natsMsg(m),
		jetstream.WithMsgID(m.MsgID), jetstream.WithExpectStream(m.ExpectStream), jetstream.WithRetryAttempts(0))
	if err != nil {
		return Ack{}, err
	}

	return Ack{Stream: ack.Stream, Seq: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

// PublishAsync implements Bus with the asynchronous option set: the stall wait, and τ as the JetStream context's timeout.
func (b *natsBus) PublishAsync(m *Msg) (AckFuture, error) {
	c := b.conn.Load()
	if c == nil {
		return nil, errNotConnected
	}
	f, err := c.js.PublishMsgAsync(natsMsg(m),
		jetstream.WithMsgID(m.MsgID), jetstream.WithExpectStream(m.ExpectStream), jetstream.WithRetryAttempts(0),
		jetstream.WithStallWait(b.stall))
	if err != nil {
		return nil, err
	}

	return natsFuture{f: f}, nil
}

// Objects implements Bus.
// It refuses a bucket whose stream allows direct reads (T-28): the handle routes reads by the setting it reads now,
// so a direct read could reach a lagging replica and a stale "not found" would let a put replace another version.
// The setting is read before the handle binds, and only tracekeep migrate changes it, turning it off,
// so the handle never holds it on once the check passed; a refused bucket is tried again at the next attempt.
func (b *natsBus) Objects(ctx context.Context, bucket string) (Objects, error) {
	c := b.conn.Load()
	if c == nil {
		return nil, errNotConnected
	}
	stream := busconfig.ClaimStreamName(bucket)
	s, err := c.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("producer: claim bucket %s: %w", bucket, err)
	}
	if s.CachedInfo().Config.AllowDirect {
		return nil, fmt.Errorf("producer: claim bucket %s: its stream %s allows direct reads, which a lagging replica can answer", bucket, stream)
	}
	os, err := c.js.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("producer: claim bucket %s: %w", bucket, err)
	}

	return natsObjects{os: os}, nil
}

// CheckConn implements Bus.
func (b *natsBus) CheckConn() []Problem {
	c := b.conn.Load()
	if c == nil {
		return []Problem{{Check: "tls", Detail: "TLS not verified: " + errNotConnected.Error()}}
	}
	var out []Problem
	if c.nc.Opts.ReconnectBufSize >= 0 {
		out = append(out, Problem{Check: "reconnect_buffer", Detail: "the reconnect buffer is enabled, so a publish while disconnected is flushed later instead of failing"})
	}
	switch _, err := c.nc.TLSConnectionState(); {
	case errors.Is(err, nats.ErrConnectionNotTLS):
		out = append(out, Problem{Check: "tls", Detail: "the connection to the traffic cluster does not use TLS"})
	case err != nil:
		out = append(out, Problem{Check: "tls", Detail: fmt.Sprintf("TLS not verified: %v", err)})
	default:
	}

	return out
}

// StreamInfo implements Bus.
func (b *natsBus) StreamInfo(ctx context.Context, name string) (StreamInfo, error) {
	c := b.conn.Load()
	if c == nil {
		return StreamInfo{}, errNotConnected
	}
	s, err := c.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return StreamInfo{}, fmt.Errorf("%s: %w", name, ErrStreamNotFound)
	}
	if err != nil {
		return StreamInfo{}, err
	}

	return StreamInfoOf(&s.CachedInfo().Config), nil
}

// waitUp returns the connection once the first dial has returned it, or nil when quit closes first.
func (b *natsBus) waitUp(quit <-chan struct{}) *nats.Conn {
	select {
	case <-b.up:
		return b.conn.Load().nc
	case <-quit:
		return nil
	}
}

// close stops the JetStream publisher, whose reply subscription would outlive the producer on a connection WithConn brought,
// and closes the connection the SDK dialed, now or, if the first dial is still running, as soon as it returns.
func (b *natsBus) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	c := b.conn.Load()
	if c == nil {
		return
	}
	c.js.CleanupPublisher()
	if b.owned {
		c.nc.Close()
	}
}

// Poll implements AckFuture.
func (f natsFuture) Poll() (Ack, bool, error) {
	select {
	case ack := <-f.f.Ok():
		return Ack{Stream: ack.Stream, Seq: ack.Sequence, Duplicate: ack.Duplicate}, true, nil
	case err := <-f.f.Err():
		return Ack{}, true, err
	default:
		return Ack{}, false, nil
	}
}

// GetInfo implements Objects.
func (o natsObjects) GetInfo(ctx context.Context, name string) (ObjectInfo, error) {
	info, err := o.os.GetInfo(ctx, name)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return ObjectInfo{}, fmt.Errorf("%s: %w", name, ErrObjectNotFound)
	}
	if err != nil {
		return ObjectInfo{}, err
	}

	return objectInfo(info), nil
}

// Put implements Objects, with the chunk size meta names so every chunk fits max_payload.
func (o natsObjects) Put(ctx context.Context, meta ObjectMeta, data []byte) (ObjectInfo, error) {
	info, err := o.os.Put(ctx, jetstream.ObjectMeta{
		Name:     meta.Name,
		Metadata: meta.Metadata,
		Opts:     &jetstream.ObjectMetaOptions{ChunkSize: meta.ChunkSize},
	}, bytes.NewReader(data))
	if err != nil {
		return ObjectInfo{}, err
	}

	return objectInfo(info), nil
}

// Get implements Objects.
func (o natsObjects) Get(ctx context.Context, name string) ([]byte, error) {
	data, err := o.os.GetBytes(ctx, name)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil, fmt.Errorf("%s: %w", name, ErrObjectNotFound)
	}

	return data, err
}

// Delete implements Objects.
func (o natsObjects) Delete(ctx context.Context, name string) error {
	return o.os.Delete(ctx, name)
}

func objectInfo(info *jetstream.ObjectInfo) ObjectInfo {
	return ObjectInfo{Name: info.Name, Size: info.Size, Digest: info.Digest, Metadata: info.Metadata}
}
