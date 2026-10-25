package producer

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
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
type natsBus struct {
	nc    *nats.Conn
	js    jetstream.JetStream
	stall time.Duration
	// owned is set when New dialed nc, so Close closes it.
	owned bool
}

var _ Bus = (*natsBus)(nil)

type natsFuture struct{ f jetstream.PubAckFuture }

type natsObjects struct{ os jetstream.ObjectStore }

// newNATSBus builds the default bus over nc, or over a connection it dials from cfg when nc is nil.
// The dial never waits for the cluster: an unreachable cluster is retried in the background,
// and until it answers the bus reports no max_payload, so the publisher counts it as down (producer.md §4.3).
func newNATSBus(cfg *Config, tun *Tunables, nc *nats.Conn, log Logger) (*natsBus, error) {
	owned := nc == nil
	if owned {
		var err error
		if nc, err = dial(cfg, log); err != nil {
			return nil, err
		}
	} else if nc.Opts.ReconnectBufSize >= 0 {
		return nil, fmt.Errorf("%w: the connection's reconnect buffer is enabled; set nats.ReconnectBufSize(-1)", ErrInvalidConfig)
	}
	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncMaxPending(asyncMaxPending),
		jetstream.WithPublishAsyncTimeout(tun.PublishTimeout),
	)
	if err != nil {
		if owned {
			nc.Close()
		}

		return nil, fmt.Errorf("producer: jetstream: %w", err)
	}

	return &natsBus{nc: nc, js: js, stall: tun.StallWait, owned: owned}, nil
}

// dial connects to the traffic cluster with every correctness-bearing option the SDK owns:
// the reconnect buffer disabled, so a publish while disconnected fails at once (producer.md §4.2 *Large records*),
// unlimited reconnects, even past a repeated authentication error, which nats.go would otherwise take as final,
// and the first connection retried in the background instead of failing New.
// The connection's state changes and errors go to log, from nats.go's callback goroutine.
func dial(cfg *Config, log Logger) (*nats.Conn, error) {
	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("%w: no servers, and neither WithConn nor WithBus", ErrInvalidConfig)
	}
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
			log.Warn("producer: traffic connection lost", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("producer: traffic connection restored", "server", nc.ConnectedUrlRedacted())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			log.Warn("producer: traffic connection error", "error", err)
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if err := nc.LastError(); err != nil {
				log.Error("producer: traffic connection closed", "error", err)
			}
		}),
	}, auth...)
	opts = append(opts, tlsOptions(&cfg.TLS)...)
	nc, err := nats.Connect(strings.Join(cfg.Servers, ","), opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: connect: %w", ErrInvalidConfig, err)
	}

	return nc, nil
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
func (b *natsBus) MaxPayload() int { return int(b.nc.MaxPayload()) }

// Publish implements Bus with the synchronous option set: the caller's deadline and no stall wait.
func (b *natsBus) Publish(ctx context.Context, m *Msg) (Ack, error) {
	ack, err := b.js.PublishMsg(ctx, natsMsg(m),
		jetstream.WithMsgID(m.MsgID), jetstream.WithExpectStream(m.ExpectStream), jetstream.WithRetryAttempts(0))
	if err != nil {
		return Ack{}, err
	}

	return Ack{Stream: ack.Stream, Seq: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

// PublishAsync implements Bus with the asynchronous option set: the stall wait, and τ as the JetStream context's timeout.
func (b *natsBus) PublishAsync(m *Msg) (AckFuture, error) {
	f, err := b.js.PublishMsgAsync(natsMsg(m),
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
	stream := busconfig.ClaimStreamName(bucket)
	s, err := b.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("producer: claim bucket %s: %w", bucket, err)
	}
	if s.CachedInfo().Config.AllowDirect {
		return nil, fmt.Errorf("producer: claim bucket %s: its stream %s allows direct reads, which a lagging replica can answer", bucket, stream)
	}
	os, err := b.js.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("producer: claim bucket %s: %w", bucket, err)
	}

	return natsObjects{os: os}, nil
}

// CheckConn implements Bus.
func (b *natsBus) CheckConn() []Problem {
	var out []Problem
	if b.nc.Opts.ReconnectBufSize >= 0 {
		out = append(out, Problem{Check: "reconnect_buffer", Detail: "the reconnect buffer is enabled, so a publish while disconnected is flushed later instead of failing"})
	}
	switch _, err := b.nc.TLSConnectionState(); {
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
	s, err := b.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return StreamInfo{}, fmt.Errorf("%s: %w", name, ErrStreamNotFound)
	}
	if err != nil {
		return StreamInfo{}, err
	}

	return StreamInfoOf(&s.CachedInfo().Config), nil
}

// close stops the JetStream publisher, whose reply subscription would outlive the producer on a connection WithConn brought,
// and closes the connection when New dialed it.
func (b *natsBus) close() {
	b.js.CleanupPublisher()
	if b.owned {
		b.nc.Close()
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
