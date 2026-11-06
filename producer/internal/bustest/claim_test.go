package bustest_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/busconfig"
	"github.com/arloliu/tracekeep/producer/internal/link"
)

var errUncertain = errors.New("claimBus: upload uncertain")

// claimBus is the default bus whose first claim put completes and is then reported uncertain,
// after leave turns the object into what an uncertain upload can leave; it records each object operation.
type claimBus struct {
	producer.Bus
	leave func(ctx context.Context, name string, data []byte)
	// expire makes the first read-back run on an expired deadline, as a read that times out does.
	expire bool

	mu      sync.Mutex
	ops     []string
	getErrs []error
}

type claimObjects struct {
	producer.Objects
	b *claimBus
}

func (b *claimBus) Objects(ctx context.Context, bucket string) (producer.Objects, error) {
	o, err := b.Bus.Objects(ctx, bucket)
	if err != nil {
		return nil, err
	}

	return claimObjects{Objects: o, b: b}, nil
}

// record notes op and reports whether it is the first of its kind.
func (b *claimBus) record(op string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	first := !slices.Contains(b.ops, op)
	b.ops = append(b.ops, op)

	return first
}

func (o claimObjects) Put(ctx context.Context, meta producer.ObjectMeta, data []byte) (producer.ObjectInfo, error) {
	first := o.b.record("put")
	info, err := o.Objects.Put(ctx, meta, data)
	if !first || err != nil {
		return info, err
	}
	if o.b.leave != nil {
		o.b.leave(ctx, meta.Name, data)
	}

	return producer.ObjectInfo{}, errUncertain
}

func (o claimObjects) Get(ctx context.Context, name string) ([]byte, error) {
	if o.b.record("get") && o.b.expire {
		expired, cancel := context.WithDeadline(ctx, time.Now())
		defer cancel()
		ctx = expired
	}
	data, err := o.Objects.Get(ctx, name)
	o.b.mu.Lock()
	o.b.getErrs = append(o.b.getErrs, err)
	o.b.mu.Unlock()

	return data, err
}

func (o claimObjects) Delete(ctx context.Context, name string) error {
	o.b.record("delete")
	return o.Objects.Delete(ctx, name)
}

func (b *claimBus) recorded() ([]string, []error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return slices.Clone(b.ops), slices.Clone(b.getErrs)
}

// rawInfo reads an object's info as nats.go stores it.
func (b *bus) rawInfo(ctx context.Context, t *testing.T, name string) *jetstream.ObjectInfo {
	t.Helper()
	os, err := b.js.ObjectStore(ctx, bucket)
	if err != nil {
		t.Errorf("bucket: %v", err)
		return nil
	}
	info, err := os.GetInfo(ctx, name)
	if err != nil {
		t.Errorf("info of %s: %v", name, err)
		return nil
	}

	return info
}

// shorten returns a leave that points the object's metadata, the whole payload's, at the chunks of its first half.
func (b *bus) shorten(t *testing.T) func(context.Context, string, []byte) {
	return func(ctx context.Context, name string, data []byte) {
		os, err := b.js.ObjectStore(ctx, bucket)
		if err != nil {
			t.Errorf("bucket: %v", err)
			return
		}
		info := b.rawInfo(ctx, t, name)
		if info == nil {
			return
		}
		half, err := os.PutBytes(ctx, name+".half", data[:len(data)/2])
		if err != nil {
			t.Errorf("put half: %v", err)
			return
		}
		short := *info
		short.NUID, short.Chunks = half.NUID, half.Chunks
		m := nats.NewMsg("$O." + bucket + ".M." + base64.URLEncoding.EncodeToString([]byte(name)))
		m.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
		if m.Data, err = json.Marshal(&short); err != nil {
			t.Errorf("metadata: %v", err)
			return
		}
		if _, err := b.js.PublishMsg(ctx, m); err != nil {
			t.Errorf("publish metadata: %v", err)
		}
	}
}

// purgeChunks returns a leave that purges the object's chunks and keeps its metadata, as nats.go does on a late chunk error.
func (b *bus) purgeChunks(t *testing.T) func(context.Context, string, []byte) {
	return func(ctx context.Context, name string, _ []byte) {
		info := b.rawInfo(ctx, t, name)
		if info == nil {
			return
		}
		s, err := b.js.Stream(ctx, busconfig.ClaimStreamName(bucket))
		if err != nil {
			t.Errorf("claim stream: %v", err)
			return
		}
		if err := s.Purge(ctx, jetstream.WithPurgeSubject("$O."+bucket+".C."+info.NUID)); err != nil {
			t.Errorf("purge chunks: %v", err)
		}
	}
}

// A claim put reported uncertain leaves a candidate the SDK reads back on the real bus:
// a read that fails without proof keeps it and is tried again, finding it whole,
// while a short object and metadata whose chunks were purged are proven incomplete, deleted and put again.
func TestClaimReadBackOnTheRealBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// leave turns the stored object into what the uncertain upload left; nil leaves it whole.
		leave  func(b *bus, t *testing.T) func(ctx context.Context, name string, data []byte)
		expire bool
		want   []string
		proven bool
	}{
		{
			name:   "transient read error",
			expire: true,
			want:   []string{"put", "get", "get"},
		},
		{
			name:   "short object",
			leave:  (*bus).shorten,
			want:   []string{"put", "get", "delete", "put"},
			proven: true,
		},
		{
			name:   "chunks purged",
			leave:  (*bus).purgeChunks,
			want:   []string{"put", "get", "delete", "put"},
			proven: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBus(t)
			nc, err := nats.Connect(b.url, nats.ReconnectBufSize(-1))
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			t.Cleanup(nc.Close)
			def, err := link.Bus(nc)
			if err != nil {
				t.Fatalf("default bus: %v", err)
			}
			pb, ok := def.(producer.Bus)
			if !ok {
				t.Fatalf("default bus %T is no producer.Bus", def)
			}
			cb := &claimBus{Bus: pb, expire: tt.expire}
			if tt.leave != nil {
				cb.leave = tt.leave(b, t)
			}
			// A read the SDK cannot tell from a timeout would cost it a whole τ per attempt.
			p, err := producer.New(producer.Config{SubjectPrefix: prefix, ClaimBucket: bucket, Source: "test-source", Tunables: producer.Tunables{PublishTimeout: 2 * time.Second}},
				producer.WithBus(cb))
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = p.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := p.Begin(ctx, producer.CaptureDescriptor{EquipmentID: "T1", Device: "eqp"}); err != nil {
				t.Fatalf("Begin: %v", err)
			}
			rec := link.Recorder(p)
			c := rec.Current()
			large := frame(1536 << 10) // two chunks
			observe(rec, large)

			b.fetch(t, 2)
			os, err := b.js.ObjectStore(ctx, bucket)
			if err != nil {
				t.Fatalf("bucket: %v", err)
			}
			name, _ := tpbus.MsgID(c.ID, 1)
			if data, err := os.GetBytes(ctx, name); err != nil || !bytes.Equal(data, large) {
				t.Fatalf("claim object: %d bytes, %v; want the whole payload", len(data), err)
			}
			ops, getErrs := cb.recorded()
			if !slices.Equal(ops, tt.want) {
				t.Errorf("object operations = %v, want %v", ops, tt.want)
			}
			if len(getErrs) == 0 {
				t.Fatal("no read-back")
			}
			if proven := errors.Is(getErrs[0], producer.ErrObjectIncomplete); proven != tt.proven || getErrs[0] == nil {
				t.Errorf("first read-back: %v; want an error, proving the object incomplete: %v", getErrs[0], tt.proven)
			}
		})
	}
}
