package ingest

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	tpbus "github.com/arloliu/go-secs/tracepack/bus"
	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// The acknowledgement benchmark's fetch: FetchBatch messages at a time, each fetch waiting at most ackMaxWait.
const (
	ackFetchBatch = 100
	ackMaxWait    = 5 * time.Millisecond
)

// fetchAll pulls n messages from cons, as the ingester reads them from a stream of incarnation inc, parked when it is the parking stream's.
func fetchAll(tb testing.TB, cons jetstream.Consumer, n int, inc catalog.Incarnation) []Entry {
	tb.Helper()
	es := make([]Entry, 0, n)
	deadline := time.Now().Add(30 * time.Second)
	for len(es) < n {
		if time.Now().After(deadline) {
			tb.Fatalf("fetched %d messages of %d", len(es), n)
		}
		batch, err := cons.Fetch(ackFetchBatch, jetstream.FetchMaxWait(ackMaxWait))
		if err != nil {
			tb.Fatal(err)
		}
		for m := range batch.Messages() {
			msg, err := readMessage(m)
			if err != nil {
				tb.Fatal(err)
			}
			msg.Incarnation, msg.Parked = inc.ID, inc.Role == catalog.RoleParking
			d := &Delivery{Msg: msg}
			d.Raw, d.Facts = ReadRaw(&d.Msg.Env, d.Msg.Body, 0)
			es = append(es, Entry{D: d, Msg: m})
		}
	}

	return es
}

// drained waits until s holds no message, or 10s: an inline acknowledgement is not confirmed,
// so the server removes its message a moment after Ack returned.
func drained(tb testing.TB, s jetstream.Stream) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err := s.Info(tb.Context())
		if err != nil {
			tb.Fatal(err)
		}
		if info.State.Msgs == 0 {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("the stream holds %d messages, want every one acknowledged", info.State.Msgs)
		}
		time.Sleep(time.Millisecond)
	}
}

// copyRows counts the capture's copy rows in state.
func copyRows(tb testing.TB, c *catalog.Catalog, capture tracepack.UUID, state string) int {
	tb.Helper()
	_, read := c.Pools()
	var n int
	if err := read.QueryRowContext(tb.Context(), `SELECT COUNT(*) FROM parked_copies WHERE capture_id = ? AND state = ?`, capture[:], state).Scan(&n); err != nil {
		tb.Fatal(err)
	}

	return n
}

// ackBench is one acknowledgement benchmark's replica: its consumers raised above n pending, and the next seq to publish.
type ackBench struct {
	r      *replica
	n      int
	cons   jetstream.Consumer
	parked jetstream.Stream
	// parkedInc and parkedCons are the parking stream's incarnation and consumer.
	parkedInc  catalog.Incarnation
	parkedCons jetstream.Consumer
	capture    tracepack.UUID
	cl         *claims
	next       uint64
}

func newAckBench(tb testing.TB, n int) *ackBench {
	tb.Helper()
	ctx := tb.Context()
	r := newReplica(tb, catalogtest.Open(tb, catalog.EngineSQLite))
	cfg := bus.ConsumerConfig{AckWait: 10 * time.Minute, MaxAckPending: 2 * n}
	cons, err := bus.Consumer(ctx, r.stream, r.g.Incarnation, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	parked, err := r.js.Stream(ctx, busconfig.DefaultParkedStream)
	if err != nil {
		tb.Fatal(err)
	}
	inc, err := bus.Incarnate(ctx, r.cat, catalog.RoleParking, parked)
	if err != nil {
		tb.Fatal(err)
	}
	parkedCons, err := bus.Consumer(ctx, parked, inc, cfg)
	if err != nil {
		tb.Fatal(err)
	}

	return &ackBench{r: r, n: n, cons: cons, parked: parked, parkedInc: inc, parkedCons: parkedCons, capture: tracepack.UUID{0xa1}, cl: newClaims()}
}

// traffic publishes n fresh data records on the traffic stream, inline or claim-check, and fetches them.
// shortcut: a claim-check record's object is not put in the bus's claim bucket, since no acknowledgement reads it; put it if one ever does.
func (a *ackBench) traffic(tb testing.TB, claim bool) []Entry {
	tb.Helper()
	for range a.n {
		r := dataRecord(a.next, now)
		m := inline(tb, a.capture, r, nil)
		if claim {
			m = claimCheck(tb, a.cl, a.capture, r, nil)
		}
		a.r.publish(tb, m, nil)
		a.next++
	}

	return fetchAll(tb, a.cons, a.n, a.r.g.Incarnation)
}

// copies parks n fresh data records as the coordinator would, the version begun, its copy stored and advanced to parked,
// then fetches the copies from the parking stream and records each copy's row, as the parked consumer does before it disposes of one.
func (a *ackBench) copies(tb testing.TB) []Entry {
	tb.Helper()
	ctx := tb.Context()
	k := a.r.g.Coordinator
	traffic, err := tpbus.Subject(prefix, "T1", a.capture)
	if err != nil {
		tb.Fatal(err)
	}
	for range a.n {
		m := inline(tb, a.capture, dataRecord(a.next, now), nil)
		d := delivered(m)
		if _, err := k.Begin(ctx, d, ReasonWriteFailure, HourOf(now)-24); err != nil {
			tb.Fatal(err)
		}
		subject, err := parkSubject(busconfig.DefaultParkPrefix, traffic, d.Raw.Version)
		if err != nil {
			tb.Fatal(err)
		}
		a.r.publishTo(tb, subject, m)
		if state, err := k.Advance(ctx, d); err != nil || state != catalog.VersionParked {
			tb.Fatalf("Advance = %v, %v; want parked", state, err)
		}
		// The original's own copy row, which Begin recorded, acked as the coordinator's acknowledgement of it would.
		if err := a.r.cat.AckCopy(ctx, d.identity()); err != nil {
			tb.Fatal(err)
		}
		a.next++
	}
	es := fetchAll(tb, a.parkedCons, a.n, a.parkedInc)
	for _, e := range es {
		if state, err := a.r.cat.RecordCopy(ctx, e.D.identity()); err != nil || state != catalog.VersionParked {
			tb.Fatalf("RecordCopy = %v, %v; want parked", state, err)
		}
	}

	return es
}

// restore asserts an iteration's parked rows, every copy row acked (an original's and its copy's for each of the n versions),
// then deletes them and the versions, so the next iteration starts from the population the first did: none of the capture's.
func (a *ackBench) restore(tb testing.TB) {
	tb.Helper()
	if w, acked := copyRows(tb, a.r.cat, a.capture, "waiting"), copyRows(tb, a.r.cat, a.capture, "acked"); w != 0 || acked != 2*a.n {
		tb.Fatalf("copy rows: %d waiting and %d acked, want none waiting and %d acked", w, acked, 2*a.n)
	}
	write, _ := a.r.cat.Pools()
	for _, q := range []string{`DELETE FROM parked_copies WHERE capture_id = ?`, `DELETE FROM parked WHERE capture_id = ?`} {
		if _, err := write.ExecContext(tb.Context(), q, a.capture[:]); err != nil {
			tb.Fatal(err)
		}
	}
	if vs, err := a.r.cat.ParkedVersions(tb.Context(), &a.capture); err != nil || len(vs) != 0 {
		tb.Fatalf("ParkedVersions = %d, %v after the restore, want none", len(vs), err)
	}
}

// BenchmarkAcknowledge is the acknowledgements of a flush's messages on the embedded bus, Flusher.ack one at a time as a flush runs them (P8):
// an inline record's Ack until it returns, a claim-check record's DoubleAck until the server confirms it,
// and a parked copy's acknowledgement through the coordinator until it completes (its acknowledgement prepared, a DoubleAck, its copy row acked).
//   - Starting state: a replica on its own bus and SQLite catalog, its traffic and parking consumers raised to 2n pending and a long ack wait.
//   - One iteration acknowledges n messages. Outside the timer, n fresh seqs are published (a parked copy's version begun and advanced to parked first)
//     and fetched ackFetchBatch at a time, each fetch waiting at most ackMaxWait, a copy's row recorded; n is a multiple of ackFetchBatch.
//   - Each iteration asserts every acknowledgement returned no error, then, outside the timer, the stream emptied
//     and, for the parked copies, every copy row acked.
//   - Population: the streams are emptied by the acknowledgements; the parked rows an iteration adds are deleted outside the timer
//     once asserted, so every iteration starts from the same catalog.
func BenchmarkAcknowledge(b *testing.B) {
	for _, kind := range []string{"inline", "claim-check", "parked"} {
		for _, n := range benchscale.Sizes(b, 100, 1000, 10000) {
			b.Run(fmt.Sprintf("%s/messages=%d", kind, n), func(b *testing.B) {
				a := newAckBench(b, n)
				f, s, ctx := a.r.g.Flusher, a.r.stream, b.Context()
				if kind == "parked" {
					s = a.parked
				}
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					var es []Entry
					if kind == "parked" {
						es = a.copies(b)
					} else {
						es = a.traffic(b, kind == "claim-check")
					}
					b.StartTimer()
					for _, e := range es {
						if err := f.ack(ctx, e); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					drained(b, s)
					if kind == "parked" {
						a.restore(b)
					}
					b.StartTimer()
				}
			})
		}
	}
}
