package producer

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Headers of a bus message beyond tracepack's, as the bus implementation writes them.
const (
	headerMsgID          = "Nats-Msg-Id"
	headerExpectedStream = "Nats-Expected-Stream"
)

// Metadata keys of a claim object (docs/specs/tracekeep/producer.md §4.2 *Large records*):
// the record's 44-byte header and its ts_utc_ns, both hex.
const (
	// ObjectMetaRecordHeader holds the record's 44-byte header, hex.
	ObjectMetaRecordHeader = "tpk-record-header"
	// ObjectMetaTSUTCNs holds the record's ts_utc_ns, hex.
	ObjectMetaTSUTCNs = "tpk-ts-utc-ns"
)

// ErrObjectNotFound reports a claim object that does not exist.
var ErrObjectNotFound = errors.New("producer: object not found")

// ErrStreamNotFound reports a stream that does not exist.
var ErrStreamNotFound = errors.New("producer: stream not found")

// Bus is the narrow seam the core publishes through (docs/specs/tracekeep/producer-sdk.md §4.7).
// Its types are the SDK's own: the default implementation wraps nats.go,
// and producertest's fake implements it without a connection.
// Every implementation publishes under the SDK's publication policy:
// the message id and expected stream Msg names, no client retries, the synchronous or asynchronous option set.
type Bus interface {
	// MaxPayload returns the server's max_payload.
	MaxPayload() int
	// Publish publishes m with the synchronous option set and waits for its ack within ctx.
	Publish(ctx context.Context, m *Msg) (Ack, error)
	// PublishAsync publishes m with the asynchronous option set; the future reports its ack.
	PublishAsync(m *Msg) (AckFuture, error)
	// Objects returns the claim bucket's object store.
	Objects(ctx context.Context, bucket string) (Objects, error)
	// CheckConn reports what is wrong with the connection for Preflight: TLS not in use, the reconnect buffer enabled.
	CheckConn() []Problem
	// StreamInfo returns the named stream's settings, or an error wrapping ErrStreamNotFound.
	StreamInfo(ctx context.Context, name string) (StreamInfo, error)
}

// Msg is one bus message.
type Msg struct {
	// Subject is the capture's subject.
	Subject string
	// Header holds the tracepack headers (Tpk-*); the implementation adds Nats-Msg-Id and Nats-Expected-Stream.
	Header map[string]string
	// Data is the body.
	Data []byte
	// MsgID is "<capture_id>.<seq>", by which the stream stores a retry once.
	MsgID string
	// ExpectStream is the traffic stream the publish must land in.
	ExpectStream string
}

// Ack is the stream's acknowledgement of a publish.
type Ack struct {
	// Stream and Seq name where the message was stored.
	Stream string
	Seq    uint64
	// Duplicate reports a retry the stream had already stored.
	Duplicate bool
}

// AckFuture reports the outcome of an asynchronous publish: exactly one of its channels delivers.
type AckFuture interface {
	Ok() <-chan Ack
	Err() <-chan error
}

// Objects is the claim bucket: put, read back, inspect and delete claim objects.
// Every call observes only its context, so the caller bounds each one.
type Objects interface {
	// GetInfo returns the object's info, or an error wrapping ErrObjectNotFound;
	// it reads the stream's leader, never a lagging replica.
	GetInfo(ctx context.Context, name string) (ObjectInfo, error)
	// Put stores data as the object meta names; the object is complete only when Put returns nil.
	Put(ctx context.Context, meta ObjectMeta, data []byte) (ObjectInfo, error)
	// Get reads the whole object back.
	Get(ctx context.Context, name string) ([]byte, error)
	// Delete removes the object.
	Delete(ctx context.Context, name string) error
}

// ObjectMeta describes an object to put.
type ObjectMeta struct {
	Name     string
	Metadata map[string]string
	// ChunkSize is the chunk the store splits the object into, chosen to fit max_payload.
	ChunkSize uint32
}

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Name string
	Size uint64
	// Digest is "SHA-256=<padded base64url>", as tracepack.BusPayloadDigest writes it.
	Digest   string
	Metadata map[string]string
}

// StreamInfo is a stream's settings as Preflight checks them;
// the enumerations are spelled as nats.go's String methods spell them, e.g. "WorkQueue", "File", "s2".
type StreamInfo struct {
	Name                                 string
	Subjects                             []string
	Retention, Storage, Compression      string
	MaxAge                               time.Duration
	MaxBytes, MaxMsgs, MaxMsgsPerSubject int64
	AllowDirect                          bool
	// Derived reports a mirror or sources, whose sequences are not the stream's own.
	Derived bool
}

// StreamInfoOf returns the settings of c as a Bus reports them.
func StreamInfoOf(c *jetstream.StreamConfig) StreamInfo {
	return StreamInfo{
		Name:              c.Name,
		Subjects:          c.Subjects,
		Retention:         c.Retention.String(),
		Storage:           c.Storage.String(),
		Compression:       c.Compression.String(),
		MaxAge:            c.MaxAge,
		MaxBytes:          c.MaxBytes,
		MaxMsgs:           c.MaxMsgs,
		MaxMsgsPerSubject: c.MaxMsgsPerSubject,
		AllowDirect:       c.AllowDirect,
		Derived:           c.Mirror != nil || len(c.Sources) > 0,
	}
}

// headerBlockLen returns the length of m's encoded NATS header block, Nats-Msg-Id and Nats-Expected-Stream included:
// "NATS/1.0\r\n", one "Key: Value\r\n" per header, and the closing "\r\n".
func headerBlockLen(m *Msg) int {
	n := len("NATS/1.0\r\n") + len("\r\n")
	add := func(k, v string) { n += len(k) + len(": ") + len(v) + len("\r\n") }
	add(headerMsgID, m.MsgID)
	add(headerExpectedStream, m.ExpectStream)
	for k, v := range m.Header {
		add(k, v)
	}

	return n
}
