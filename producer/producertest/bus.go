package producertest

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer"
)

// DefaultMaxPayload is the fake server's max_payload unless SetMaxPayload changes it.
const DefaultMaxPayload = 1 << 20

// Bus is an in-memory producer.Bus: it stores what a stream would, deduplicating by message id,
// keeps claim objects per bucket, and lets a test fail publishes and object operations.
// Asynchronous publishes resolve before PublishAsync returns.
type Bus struct {
	mu         sync.Mutex
	maxPayload int
	msgs       []producer.Msg
	ids        map[string]bool
	buckets    map[string]map[string]object
	failPub    func(m *producer.Msg) error
	failObj    func(op, name string) error
	uncertain  func(name string) bool
	hold       bool
	held       []heldPublish
	asyncCalls map[string]int
	changed    chan struct{}
}

type heldPublish struct {
	msg producer.Msg
	f   future
}

type object struct {
	data []byte
	info producer.ObjectInfo
}

type objects struct {
	b      *Bus
	bucket string
}

type future struct {
	ok  chan producer.Ack
	err chan error
}

var (
	_ producer.Bus       = (*Bus)(nil)
	_ producer.Objects   = objects{}
	_ producer.AckFuture = future{}
)

// NewBus returns an empty bus.
func NewBus() *Bus {
	return &Bus{
		maxPayload: DefaultMaxPayload,
		ids:        make(map[string]bool),
		buckets:    make(map[string]map[string]object),
		asyncCalls: make(map[string]int),
		changed:    make(chan struct{}),
	}
}

// SetMaxPayload sets the max_payload the bus reports.
func (b *Bus) SetMaxPayload(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maxPayload = n
}

// FailPublish makes every publish for which f returns an error fail with it; nil restores success.
func (b *Bus) FailPublish(f func(m *producer.Msg) error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failPub = f
}

// FailObjects makes every object operation ("info", "put", "get", "delete") for which f returns an error fail with it.
func (b *Bus) FailObjects(f func(op, name string) error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failObj = f
}

// FailPutsUncertain makes every put for which f returns true fail the way a late chunk error does:
// the object's metadata, digest included, is stored with only part of its bytes, and Put returns an error.
func (b *Bus) FailPutsUncertain(f func(name string) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.uncertain = f
}

// HoldAsync makes asynchronous publishes wait, unresolved, until ReleaseAsync; false resolves them at once again.
func (b *Bus) HoldAsync(hold bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hold = hold
}

// ReleaseAsync resolves every held asynchronous publish: stored and acked when err is nil, failed with err otherwise.
func (b *Bus) ReleaseAsync(err error) {
	b.mu.Lock()
	held := b.held
	b.held = nil
	b.mu.Unlock()
	for _, h := range held {
		if err != nil {
			h.f.err <- err
			continue
		}
		ack, serr := b.store(&h.msg)
		if serr != nil {
			h.f.err <- serr
		} else {
			h.f.ok <- ack
		}
	}
}

// WaitAsync waits until an asynchronous publish of msgID was made.
func (b *Bus) WaitAsync(ctx context.Context, msgID string) error {
	for {
		b.mu.Lock()
		n, ch := b.asyncCalls[msgID], b.changed
		b.mu.Unlock()
		if n > 0 {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return fmt.Errorf("producertest: waiting for a publish of %s: %w", msgID, ctx.Err())
		}
	}
}

// AsyncCalls returns how many asynchronous publishes named msgID.
func (b *Bus) AsyncCalls(msgID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.asyncCalls[msgID]
}

// Messages returns the stored messages in the order the stream accepted them.
func (b *Bus) Messages() []producer.Msg {
	b.mu.Lock()
	defer b.mu.Unlock()

	return slices.Clone(b.msgs)
}

// WaitMessages waits until the stream holds at least n messages and returns them.
func (b *Bus) WaitMessages(ctx context.Context, n int) ([]producer.Msg, error) {
	for {
		b.mu.Lock()
		if len(b.msgs) >= n {
			msgs := slices.Clone(b.msgs)
			b.mu.Unlock()

			return msgs, nil
		}
		ch := b.changed
		b.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return b.Messages(), fmt.Errorf("producertest: waiting for %d messages: %w", n, ctx.Err())
		}
	}
}

// Object returns a stored claim object's data and info.
func (b *Bus) Object(bucket, name string) ([]byte, producer.ObjectInfo, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	o, ok := b.buckets[bucket][name]

	return o.data, o.info, ok
}

// PutRawObject stores an object as given, its digest included, as an earlier or foreign put could have left it.
func (b *Bus) PutRawObject(bucket string, data []byte, info producer.ObjectInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bucketLocked(bucket)[info.Name] = object{data: slices.Clone(data), info: info}
}

// MaxPayload implements producer.Bus.
func (b *Bus) MaxPayload() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.maxPayload
}

// Publish implements producer.Bus.
func (b *Bus) Publish(ctx context.Context, m *producer.Msg) (producer.Ack, error) {
	if err := ctx.Err(); err != nil {
		return producer.Ack{}, err
	}

	return b.store(m)
}

// PublishAsync implements producer.Bus.
func (b *Bus) PublishAsync(m *producer.Msg) (producer.AckFuture, error) {
	f := future{ok: make(chan producer.Ack, 1), err: make(chan error, 1)}
	b.mu.Lock()
	b.asyncCalls[m.MsgID]++
	b.notifyLocked()
	if b.hold {
		c := *m
		c.Header = maps.Clone(m.Header)
		c.Data = slices.Clone(m.Data)
		b.held = append(b.held, heldPublish{msg: c, f: f})
		b.mu.Unlock()

		return f, nil
	}
	b.mu.Unlock()
	ack, err := b.store(m)
	if err != nil {
		f.err <- err
	} else {
		f.ok <- ack
	}

	return f, nil
}

// Objects implements producer.Bus.
func (b *Bus) Objects(_ context.Context, bucket string) (producer.Objects, error) {
	return objects{b: b, bucket: bucket}, nil
}

func (b *Bus) store(m *producer.Msg) (producer.Ack, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failPub != nil {
		if err := b.failPub(m); err != nil {
			return producer.Ack{}, err
		}
	}
	if b.ids[m.MsgID] {
		return producer.Ack{Stream: m.ExpectStream, Duplicate: true}, nil
	}
	b.ids[m.MsgID] = true
	c := *m
	c.Header = maps.Clone(m.Header)
	c.Data = slices.Clone(m.Data)
	b.msgs = append(b.msgs, c)
	b.notifyLocked()

	return producer.Ack{Stream: m.ExpectStream, Seq: uint64(len(b.msgs))}, nil
}

// notifyLocked wakes every waiter; the caller holds mu.
func (b *Bus) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *Bus) bucketLocked(bucket string) map[string]object {
	objs := b.buckets[bucket]
	if objs == nil {
		objs = make(map[string]object)
		b.buckets[bucket] = objs
	}

	return objs
}

func (o objects) fail(op, name string) error {
	if o.b.failObj == nil {
		return nil
	}

	return o.b.failObj(op, name)
}

// GetInfo implements producer.Objects.
func (o objects) GetInfo(_ context.Context, name string) (producer.ObjectInfo, error) {
	o.b.mu.Lock()
	defer o.b.mu.Unlock()
	if err := o.fail("info", name); err != nil {
		return producer.ObjectInfo{}, err
	}
	obj, ok := o.b.buckets[o.bucket][name]
	if !ok {
		return producer.ObjectInfo{}, fmt.Errorf("%s: %w", name, producer.ErrObjectNotFound)
	}

	return obj.info, nil
}

// Put implements producer.Objects.
func (o objects) Put(_ context.Context, meta producer.ObjectMeta, data []byte) (producer.ObjectInfo, error) {
	o.b.mu.Lock()
	defer o.b.mu.Unlock()
	if err := o.fail("put", meta.Name); err != nil {
		return producer.ObjectInfo{}, err
	}
	info := producer.ObjectInfo{
		Name:     meta.Name,
		Size:     uint64(len(data)),
		Digest:   tracepack.BusPayloadDigest(data),
		Metadata: maps.Clone(meta.Metadata),
	}
	if o.b.uncertain != nil && o.b.uncertain(meta.Name) {
		o.b.bucketLocked(o.bucket)[meta.Name] = object{data: slices.Clone(data[:len(data)/2]), info: info}
		return producer.ObjectInfo{}, fmt.Errorf("producertest: %s: late chunk error", meta.Name)
	}
	o.b.bucketLocked(o.bucket)[meta.Name] = object{data: slices.Clone(data), info: info}

	return info, nil
}

// Get implements producer.Objects.
func (o objects) Get(_ context.Context, name string) ([]byte, error) {
	o.b.mu.Lock()
	defer o.b.mu.Unlock()
	if err := o.fail("get", name); err != nil {
		return nil, err
	}
	obj, ok := o.b.buckets[o.bucket][name]
	if !ok {
		return nil, fmt.Errorf("%s: %w", name, producer.ErrObjectNotFound)
	}

	return slices.Clone(obj.data), nil
}

// Delete implements producer.Objects.
func (o objects) Delete(_ context.Context, name string) error {
	o.b.mu.Lock()
	defer o.b.mu.Unlock()
	if err := o.fail("delete", name); err != nil {
		return err
	}
	delete(o.b.buckets[o.bucket], name)

	return nil
}

// Ok implements producer.AckFuture.
func (f future) Ok() <-chan producer.Ack { return f.ok }

// Err implements producer.AckFuture.
func (f future) Err() <-chan error { return f.err }
