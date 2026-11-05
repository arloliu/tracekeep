package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkrecords "github.com/arloliu/tracekeep/internal/records"
	"github.com/arloliu/tracekeep/internal/store"
)

// Markers no ingest log may carry (operations.md §10.4): a dependency's text, and a catalog error's credential and query.
const (
	storeSentinel   = "store-sentinel"
	credSentinel    = "pw-sentinel"
	querySentinel   = "q-sentinel"
	catalogHost     = "db.invalid"
	catalogURLError = "dial mysql://u:" + credSentinel + "@" + catalogHost + ":3306/tk?tls=" + querySentinel + ": refused"
)

// logRecord is one log record: its message and every attribute, groups flattened to dotted keys.
type logRecord struct {
	msg    string
	fields map[string]string
}

// logBook records every log record it is handed.
type logBook struct {
	mu      sync.Mutex
	records []logRecord
	// seen, when set, is told each record's message.
	seen func(msg string)
}

func (b *logBook) Enabled(context.Context, slog.Level) bool { return true }
func (b *logBook) WithAttrs([]slog.Attr) slog.Handler       { return b }
func (b *logBook) WithGroup(string) slog.Handler            { return b }

func (b *logBook) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{msg: r.Message, fields: map[string]string{}}
	var add func(prefix string, a slog.Attr)
	add = func(prefix string, a slog.Attr) {
		v := a.Value.Resolve()
		if v.Kind() == slog.KindGroup {
			inner := prefix
			if a.Key != "" { // a group without a key is inlined
				inner += a.Key + "."
			}
			for _, g := range v.Group() {
				add(inner, g)
			}

			return
		}
		rec.fields[prefix+a.Key] = v.String()
	}
	r.Attrs(func(a slog.Attr) bool { add("", a); return true })
	b.mu.Lock()
	b.records = append(b.records, rec)
	seen := b.seen
	b.mu.Unlock()
	if seen != nil {
		seen(r.Message)
	}

	return nil
}

// event returns the first record logged with msg, failing the test when there is none, so an unexercised path cannot pass.
func (b *logBook) event(t *testing.T, msg string) logRecord {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.records {
		if r.msg == msg {
			return r
		}
	}
	t.Fatalf("no %q logged among %d records", msg, len(b.records))

	return logRecord{}
}

// clean fails the test for each marker found in any field of any record, each marker checked apart.
func (b *logBook) clean(t *testing.T, markers ...string) {
	t.Helper()
	b.cleanBut(t, func(string, string) bool { return false }, markers...)
}

// cleanBut is clean skipping the fields skip names.
func (b *logBook) cleanBut(t *testing.T, skip func(key, value string) bool, markers ...string) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.records {
		for _, m := range markers {
			if m == "" {
				t.Fatal("an empty marker")
			}
			for k, v := range r.fields {
				if skip(k, v) {
					continue
				}
				if strings.Contains(v, m) || strings.Contains(k, m) {
					t.Errorf("%q logs %s=%q, which holds %q", r.msg, k, v, m)
				}
			}
			if strings.Contains(r.msg, m) {
				t.Errorf("the message %q holds %q", r.msg, m)
			}
		}
	}
}

// keyedStore is a store whose puts and deletes fail with an error naming the key and the dependency's sentinel,
// a put's carrying a cancellation; keys records each key it was given.
type keyedStore struct {
	store.ObjectStore
	put bool

	mu   sync.Mutex
	keys []string
}

func (s *keyedStore) named(key string) {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
}

func (s *keyedStore) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	s.named(key)
	if s.put {
		return fmt.Errorf("store: put %s: %s: %w", key, storeSentinel, context.Canceled)
	}

	return s.ObjectStore.Put(ctx, key, r, size)
}

func (s *keyedStore) Delete(_ context.Context, key string) error {
	s.named(key)

	return fmt.Errorf("store: delete %s: %s", key, storeSentinel)
}

func (s *keyedStore) key(t *testing.T) string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.keys) == 0 {
		t.Fatal("no key reached the store")
	}

	return s.keys[0]
}

// A flush's failure is logged without the object's key or the store's text, the store's error keeping its class,
// and a catalog error's text is logged as the API presents it, its diagnostic kept and its credential and query cut,
// never reclassified as the store's.
func TestFlushLogsCarryNoKey(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		setup func(f *Flusher, st *keyedStore, c *catalog.Catalog)
		// class is what the failed flush still is to errors.Is; store reports it the store's.
		class error
		store bool
	}{
		{
			name:  "put",
			setup: func(_ *Flusher, st *keyedStore, _ *catalog.Catalog) { st.put = true },
			class: context.Canceled, store: true,
		},
		{
			name: "beyond-horizon delete",
			setup: func(f *Flusher, _ *keyedStore, _ *catalog.Catalog) {
				f.HorizonHour = func() int64 { return HourOf(now) + 1 }
			},
			class: tkrecords.ErrStore, store: true,
		},
		{
			name: "registration",
			setup: func(f *Flusher, _ *keyedStore, _ *catalog.Catalog) {
				f.registerPack = func(context.Context, *catalog.Pack, catalog.Registration) (catalog.PackOutcome, int, error) {
					return 0, 0, fmt.Errorf("catalog: register pack: %s: %w", catalogURLError, context.DeadlineExceeded)
				}
			},
			class: context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				r := newReplica(t, c)
				book := &logBook{}
				r.g.Log = slog.New(book)
				st := &keyedStore{ObjectStore: r.st}
				r.g.Flusher.Store = st
				tc.setup(r.g.Flusher, st, c)
				var passErr error
				r.g.PassSettled = func(_ time.Time, _ []*Batch, err error) { passErr = err }
				capture := tracepack.UUID{0xd1, byte(len(tc.name))}
				r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
				r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
				r.step(t)
				r.clk.flushInterval()
				r.step(t) // the batch is due

				e := book.event(t, "ingest: flush failed")
				if !errors.Is(passErr, tc.class) {
					t.Errorf("flush = %v, want it still %v", passErr, tc.class)
				}
				if got := errors.Is(passErr, tkrecords.ErrStore); got != tc.store {
					t.Errorf("flush = %v, the store's %v; want %v", passErr, got, tc.store)
				}
				markers := []string{storeSentinel, credSentinel, querySentinel}
				if tc.store {
					markers = append(markers, st.key(t))
				} else if !strings.Contains(e.fields["error"], catalogHost) {
					t.Errorf("error %q, want the catalog's diagnostic kept", e.fields["error"])
				}
				book.clean(t, markers...)
			})
		})
	}
}

// failingObjects is a claim bucket whose fetches fail: a read with read when set, the fetch with err when set,
// else the fetch with a text naming the object and the bucket's sentinel.
type failingObjects struct {
	jetstream.ObjectStore
	err, read error
}

func (o failingObjects) Get(_ context.Context, name string, _ ...jetstream.GetObjectOpt) (jetstream.ObjectResult, error) {
	switch {
	case o.read != nil:
		return failingRead{err: o.read}, nil
	case o.err != nil:
		return nil, o.err
	}

	return nil, fmt.Errorf("nats: get %s: %s", name, storeSentinel)
}

// failingRead is a claim object whose read fails with err.
type failingRead struct {
	jetstream.ObjectResult
	err error
}

func (r failingRead) Read([]byte) (int, error) { return 0, r.err }
func (failingRead) Close() error               { return nil }

// A claim object's fetch is keyless at the reader: its error names neither the object, whose name is the message id, nor the bucket's text,
// a missing object still ErrClaimNotFound and a cancellation still one.
func TestClaimFetchIsKeyless(t *testing.T) {
	t.Parallel()
	const name = "claim-name-sentinel"
	for _, tc := range []struct {
		name  string
		store jetstream.ObjectStore
		class error
	}{
		{"failure", failingObjects{}, tkrecords.ErrStore},
		{"missing", failingObjects{err: fmt.Errorf("get %s: %w", name, jetstream.ErrObjectNotFound)}, ErrClaimNotFound},
		{"cancelled", failingObjects{err: fmt.Errorf("get %s: %s: %w", name, storeSentinel, context.Canceled)}, context.Canceled},
		{"read", failingObjects{read: fmt.Errorf("read %s: %s: %w", name, storeSentinel, context.Canceled)}, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ObjectClaims{Store: tc.store}.Get(t.Context(), name, 8)
			if !errors.Is(err, tc.class) {
				t.Errorf("Get = %v, want it still %v", err, tc.class)
			}
			if err != nil && (strings.Contains(err.Error(), name) || strings.Contains(err.Error(), storeSentinel)) {
				t.Errorf("Get = %q, which names the object or the bucket's text", err)
			}
		})
	}
}

// Every delivery's log names its stream sequence and incarnation, never its message id, which names its claim object:
// a failed disposition with the claim fetch's error, a failed copy row, acknowledgement, eviction and parking, and an alert;
// each logs its event, the claim object missing is still claim-missing, and a cancellation stays one.
func TestDeliveryLogsCarryNoMessageID(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		r := newReplica(t, c)
		book := &logBook{}
		r.g.Log = slog.New(book)
		capture := tracepack.UUID{0xd2}
		var ids []string
		fetch := func() (*Delivery, jetstream.Msg) {
			d, m := r.fetch(t)
			ids = append(ids, d.Msg.Env.MsgID)
			return d, m
		}

		// The claim fetch's failure, through the disposition.
		start := claimCheck(t, newClaims(), capture, startRecord(t, now), descriptorOf(t, "eqp"))
		r.publish(t, start, nil)
		r.g.Executor.Claims = ObjectClaims{Store: failingObjects{}}
		_, sm := fetch()
		r.g.handle(ctx, sm, r.g.membership.Generation)
		e := book.event(t, "ingest: disposition failed")
		if e.fields["stream_seq"] == "" || e.fields["incarnation"] == "" {
			t.Errorf("disposition failed %v, want its stream sequence and incarnation", e.fields)
		}
		// The claim object missing is still claim-missing.
		r.g.Executor.Claims = ObjectClaims{Store: failingObjects{err: jetstream.ErrObjectNotFound}}
		if d, _, err := r.g.Executor.Dispose(ctx, start); err != nil || d.Facts.Claim != ClaimMissing {
			t.Errorf("Dispose = %v, %v; want claim-missing", d.Facts.Claim, err)
		}

		// A cancelled acknowledgement, eviction and parking, and an alert.
		r.publish(t, inline(t, capture, dataRecord(1, now+1), nil), nil)
		d, m := fetch()
		e2 := Entry{D: d, Msg: m}
		r.g.acknowledge(cancelled, e2)
		book.event(t, "ingest: ack failed")
		r.g.evict(cancelled, e2)
		book.event(t, "ingest: eviction of a parked version failed")
		r.g.park(cancelled, e2, ReasonUnknownCapture, 1)
		book.event(t, "ingest: parking failed")
		r.g.alert(d, Decision{Alert: true, Reason: ReasonUnknownCapture})
		book.event(t, "ingest: alert")

		// A cancelled copy row, on the parked consumer.
		other := tracepack.UUID{0xd3}
		parkTraffic(t, r, other)
		pg := r.parkedConsumer(t)
		pg.Log = r.g.Log
		batch, err := pg.Consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		cm, ok := <-batch.Messages()
		if !ok {
			t.Fatal("no parked copy")
		}
		ids = append(ids, cm.Headers().Get("Nats-Msg-Id"))
		pg.handle(cancelled, cm, pg.membership.Generation)
		book.event(t, "ingest: copy row failed")

		book.clean(t, storeSentinel)
		// The catalog's own text names a version by its capture and seq, which spell the message id; it is the catalog's to change.
		catalogText := func(k, v string) bool { return k == "error" && strings.HasPrefix(v, "catalog: ") }
		book.cleanBut(t, catalogText, ids...)
	})
}

// failingFetch is a consumer whose fetches fail with an error quoting the bus's URL, its credential and query.
type failingFetch struct{ jetstream.Consumer }

func (failingFetch) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return nil, errors.New(strings.Replace(catalogURLError, "mysql", "nats", 1))
}

// A failed step is logged as the API presents a dependency's error: its diagnostic kept, its credential and query cut.
func TestStepFailureLogIsPresented(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		ctx, cancel := context.WithCancel(t.Context())
		book := &logBook{seen: func(msg string) {
			if msg == "ingest: step failed" {
				cancel()
			}
		}}
		r.g.Log = slog.New(book)
		r.g.Consumer = failingFetch{Consumer: r.g.Consumer}
		if err := r.g.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if e := book.event(t, "ingest: step failed"); !strings.Contains(e.fields["error"], catalogHost) {
			t.Errorf("error %q, want the dependency's diagnostic kept", e.fields["error"])
		}
		book.clean(t, credSentinel, querySentinel)
	})
}
