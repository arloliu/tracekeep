package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
)

// smallBound is a claim bound below dataRecord's 14-byte payload, and largeBound one at it.
const (
	smallBound = 13
	largeBound = 14
)

// countingClaims counts the fetches of each object from the claim store it wraps.
type countingClaims struct {
	ClaimStore
	mu   sync.Mutex
	gets map[string]int
}

func (c *countingClaims) Get(ctx context.Context, name string, size int64) ([]byte, error) {
	c.mu.Lock()
	if c.gets == nil {
		c.gets = map[string]int{}
	}
	c.gets[name]++
	c.mu.Unlock()

	return c.ClaimStore.Get(ctx, name, size)
}

func (c *countingClaims) fetched(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.gets[name]
}

// A claim declared above the bound is a resource refusal, never malformed: unresolved, it is deferred unfetched and parked as too-large,
// whatever its capture's state; resolved, it settles as any other, from its resolution and without a fetch.
// Raised, the bound lets the same claim through.
func TestOversizedClaimDeferredUnfetched(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		cl := newClaims()
		x := executor(t, c, cl)
		x.MaxRecordBytes = smallBound
		tooLarge := Decision{Action: ActPark, Reason: ReasonTooLarge, Alert: true}

		unknown := claimCheck(t, cl, tracepack.UUID{0xc7, 1}, dataRecord(1, now), nil)
		if _, dec := dispose(t, x, unknown); dec != tooLarge {
			t.Errorf("an oversized claim of an unknown capture = %+v, want %+v", dec, tooLarge)
		}

		rejected := tracepack.UUID{0xc7, 2}
		if _, dec := dispose(t, x, inline(t, rejected, startRecord(t, now), descriptorOf(t, " "))); dec.Reason != ReasonCaptureRejected {
			t.Fatalf("an unaccepted start = %+v, want the capture rejected", dec)
		}
		ofRejected := claimCheck(t, cl, rejected, dataRecord(1, now), nil)
		if _, dec := dispose(t, x, ofRejected); dec != tooLarge {
			t.Errorf("an oversized claim of a rejected capture = %+v, want %+v", dec, tooLarge)
		}

		admitted := tracepack.UUID{0xc7, 3}
		if _, dec := dispose(t, x, inline(t, admitted, startRecord(t, now), descriptorOf(t, "eqp"))); dec.Action != ActBatch {
			t.Fatalf("a start = %+v, want admitted", dec)
		}
		ofAdmitted := claimCheck(t, cl, admitted, dataRecord(1, now), nil)
		if _, dec := dispose(t, x, ofAdmitted); dec != tooLarge {
			t.Errorf("an oversized claim of an admitted capture = %+v, want %+v", dec, tooLarge)
		}
		staged := claimCheck(t, cl, admitted, dataRecord(2, now), nil)
		d, _ := dispose(t, x, staged)
		stage(t, c, d)
		if _, dec := dispose(t, x, staged); dec.Action != ActAck || dec.Reason != ReasonAlreadyStaged {
			t.Errorf("an oversized claim of a staged version = %+v, want acknowledged as already staged", dec)
		}
		for name, m := range map[string]Message{"an unknown capture's": unknown, "a rejected capture's": ofRejected, "an admitted capture's": ofAdmitted, "a staged": staged} {
			if n := cl.fetched(m.Env.MsgID); n != 0 {
				t.Errorf("%s oversized claim was fetched %d times, want none", name, n)
			}
		}

		x.MaxRecordBytes = largeBound
		if _, dec := dispose(t, x, ofAdmitted); dec.Action != ActBatch || cl.fetched(ofAdmitted.Env.MsgID) != 1 {
			t.Errorf("the claim under a raised bound = %+v, fetched %d times; want fetched once and batched", dec, cl.fetched(ofAdmitted.Env.MsgID))
		}
	})
}

// object is a claim object as a read of it goes: its bytes, then, unless it never ends, its end.
type object struct {
	data    []byte
	endless bool
}

// objectStore serves one object, counting the bytes read from it and the closes of its retrievals.
type objectStore struct {
	jetstream.ObjectStore
	obj object

	mu           sync.Mutex
	read, closed int
}

func (s *objectStore) Get(ctx context.Context, _ string, _ ...jetstream.GetObjectOpt) (jetstream.ObjectResult, error) {
	var r io.Reader = bytes.NewReader(s.obj.data)
	if s.obj.endless {
		r = io.MultiReader(r, endless{ctx})
	}

	return &objectResult{s: s, r: r}, nil
}

func (s *objectStore) GetBytes(ctx context.Context, name string, opts ...jetstream.GetObjectOpt) ([]byte, error) {
	res, err := s.Get(ctx, name, opts...)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	return io.ReadAll(res)
}

func (s *objectStore) counts() (read, closed int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.read, s.closed
}

type objectResult struct {
	jetstream.ObjectResult
	s *objectStore
	r io.Reader
}

func (o *objectResult) Read(p []byte) (int, error) {
	n, err := o.r.Read(p)
	o.s.mu.Lock()
	o.s.read += n
	o.s.mu.Unlock()

	return n, err
}

func (o *objectResult) Close() error {
	o.s.mu.Lock()
	o.s.closed++
	o.s.mu.Unlock()

	return nil
}

// endless is an object's end that never comes: a read waits for its context, as the bucket's reader does.
type endless struct{ ctx context.Context }

func (e endless) Read([]byte) (int, error) {
	<-e.ctx.Done()

	return 0, e.ctx.Err()
}

// A claim object's read takes at most the declared length and one byte, and must reach the object's end within its deadline:
// an extra byte or an early end is a mismatch, as is another digest, and the retrieval is closed whatever the outcome.
func TestClaimReadIsBounded(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		capture := tracepack.UUID{0xc7, 4}
		x := executor(t, c, newClaims())
		if _, dec := dispose(t, x, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp"))); dec.Action != ActBatch {
			t.Fatalf("a start = %+v, want admitted", dec)
		}
		r := dataRecord(1, now)
		payload := r.Payload
		m := claimCheck(t, newClaims(), capture, r, nil)
		other := bytes.Clone(payload)
		other[len(other)-1]++
		mismatch := Decision{Action: ActNak, Reason: ReasonClaimMissing, Alert: true}
		for _, tc := range []struct {
			name string
			obj  object
			want Decision
			err  error
			read int
		}{
			{"the declared length", object{data: payload}, Decision{Action: ActBatch}, nil, len(payload)},
			{"the right prefix and more", object{data: append(bytes.Clone(payload), make([]byte, 1<<20)...)}, mismatch, nil, len(payload) + 1},
			{"truncated", object{data: payload[:len(payload)-1]}, mismatch, nil, len(payload) - 1},
			{"another digest", object{data: other}, mismatch, nil, len(payload)},
			{"the declared bytes and no end", object{data: payload, endless: true}, Decision{}, context.DeadlineExceeded, len(payload)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				st := &objectStore{obj: tc.obj}
				x.Claims = ObjectClaims{Store: st}
				ctx, cancel := context.WithTimeout(t.Context(), bound)
				defer cancel()
				_, dec, err := x.Dispose(ctx, m)
				if !errors.Is(err, tc.err) || dec != tc.want {
					t.Errorf("Dispose = %+v, %v; want %+v, %v", dec, err, tc.want, tc.err)
				}
				if read, closed := st.counts(); read != tc.read || closed != 1 {
					t.Errorf("read %d bytes, closed %d times; want %d read and closed once", read, closed, tc.read)
				}
			})
		}
	})
}

// A claim parked for another reason under a higher bound stays parked once the bound is lowered:
// its copy is redelivered and deferred without a fetch, its version still parked; raised again, the bound lets the copy be staged.
func TestParkedClaimWaitsForItsBound(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		cc := &countingClaims{ClaimStore: r.g.Executor.Claims}
		r.g.Executor.Claims = cc
		r.g.Flusher.hooks[hookWrite] = func() error { return tracepack.ErrRefused }
		capture := tracepack.UUID{0xc7, 5}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		cl := newClaims()
		big := claimCheck(t, cl, capture, dataRecord(1, now+1), nil)
		r.publish(t, big, cl.objs[big.Env.MsgID])
		r.step(t)
		for _, b := range r.g.Batcher.batches {
			for range writeFailureLimit - 1 {
				r.g.refused(b.Entries)
			}
		}
		r.clk.flushInterval()
		r.step(t)
		if vs := versions(t, c, capture); len(vs) != 2 {
			t.Fatalf("%d versions parked, want 2", len(vs))
		}

		r.g.Flusher.hooks[hookWrite] = nil
		r.g.Executor.MaxRecordBytes = smallBound // lowered
		pg := r.parkedConsumer(t)
		pg.RedeliveryDelay = time.Millisecond // the copy returns for each later step
		if err := pg.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		r.clk.flushInterval()
		if err := pg.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		if n := cc.fetched(big.Env.MsgID); n != 1 {
			t.Errorf("the claim was fetched %d times, want only the first, under the higher bound", n)
		}
		v := claimVersion(t, c, capture)
		if v.State != catalog.VersionParked || v.Reason != ReasonWriteFailure {
			t.Errorf("version %+v under the lowered bound, want still parked for its write failure", v)
		}

		r.g.Executor.MaxRecordBytes = largeBound // raised
		if err := pg.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		r.clk.flushInterval()
		if err := pg.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		if v := claimVersion(t, c, capture); v.State != catalog.VersionStaged {
			t.Errorf("version %+v under the raised bound, want staged", v)
		}
	})
}

// A consumer whose bound is lower parks a claim that the parked consumer, its bound higher, stages;
// a later delivery of that version to the first is acknowledged from its resolution, without a fetch.
func TestLowerBoundDefersWhatAHigherStages(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		r := newReplica(t, c)
		cc := &countingClaims{ClaimStore: r.g.Executor.Claims}
		r.g.Executor.Claims = cc
		r.g.Executor.MaxRecordBytes = smallBound
		capture := tracepack.UUID{0xc7, 6}
		r.publish(t, inline(t, capture, startRecord(t, now), descriptorOf(t, "eqp")), nil)
		cl := newClaims()
		big := claimCheck(t, cl, capture, dataRecord(1, now+1), nil)
		r.publish(t, big, cl.objs[big.Env.MsgID])
		r.step(t)
		if v := claimVersion(t, c, capture); v.State != catalog.VersionParked || v.Reason != ReasonTooLarge || cc.fetched(big.Env.MsgID) != 0 {
			t.Fatalf("version %+v, fetched %d times; want parked as too-large, unfetched", v, cc.fetched(big.Env.MsgID))
		}

		pg := r.parkedConsumer(t)
		higher := *r.g.Executor
		higher.MaxRecordBytes = largeBound
		pg.Executor = &higher
		if err := pg.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		r.clk.flushInterval()
		if err := pg.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		if v := claimVersion(t, c, capture); v.State != catalog.VersionStaged || cc.fetched(big.Env.MsgID) != 1 {
			t.Fatalf("version %+v, fetched %d times; want staged by the higher bound's one fetch", v, cc.fetched(big.Env.MsgID))
		}

		_, dec, err := r.g.Executor.Dispose(t.Context(), big)
		if err != nil || dec.Action != ActAck || dec.Reason != ReasonAlreadyStaged || cc.fetched(big.Env.MsgID) != 1 {
			t.Errorf("a later delivery to the lower bound = %+v, %v, fetched %d times; want acknowledged as staged, no fetch", dec, err, cc.fetched(big.Env.MsgID))
		}
	})
}

// claimVersion is the parked version of capture's seq 1, the claim these tests park.
func claimVersion(t *testing.T, c *catalog.Catalog, capture tracepack.UUID) catalog.ParkedVersion {
	t.Helper()
	for _, v := range versions(t, c, capture) {
		if v.Version.Seq == 1 {
			return v
		}
	}
	t.Fatalf("seq 1 of %s is not parked", capture)

	return catalog.ParkedVersion{}
}
