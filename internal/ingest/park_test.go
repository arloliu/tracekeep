package ingest

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/bus"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/testbus"
	"github.com/arloliu/tracekeep/producer/busconfig"
)

// transported gives m the traffic subject and the headers a producer's publish stamps, Nats-Expected-Stream included.
func transported(t *testing.T, m Message) Message {
	t.Helper()
	subject, err := tracepack.BusSubject(prefix, "T1", m.Raw().Version.Capture)
	if err != nil {
		t.Fatal(err)
	}
	h := nats.Header{}
	for k, v := range map[string]string{
		tracepack.BusHeaderLayout: m.Env.Layout, tracepack.BusHeaderMsgID: m.Env.MsgID, tracepack.BusHeaderDescriptor: m.Env.Descriptor,
		tracepack.BusHeaderPayloadRef: m.Env.PayloadRef, tracepack.BusHeaderPayloadDigest: m.Env.PayloadDigest,
		jetstream.ExpectedStreamHeader: busconfig.DefaultTrafficStream,
	} {
		if v != "" {
			h.Set(k, v)
		}
	}
	m.Subject, m.Header = subject, h

	return m
}

// delivered is m as a delivery read from its stream.
func delivered(m Message) *Delivery {
	d := &Delivery{Msg: m}
	d.Raw, d.Facts = ReadRaw(&d.Msg.Env, d.Msg.Body, 0)

	return d
}

// parkingBus runs a bus, standalone or a cluster of three, with the traffic and parking streams of that many replicas.
func parkingBus(t *testing.T, clustered bool) (jetstream.JetStream, jetstream.Stream) {
	t.Helper()
	ctx := t.Context()
	url := testbus.Start(t)
	if clustered {
		url = testbus.StartCluster(t, 3)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	// shortcut: a request to a cluster just formed sometimes gets no reply; both steps are idempotent, so they are retried.
	var parked jetstream.Stream
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = nil
		if clustered {
			// Ensure keeps an existing traffic stream, and gives the parking stream its replicas.
			c := busconfig.StreamConfig(busconfig.DefaultTrafficStream, prefix)
			c.Replicas = 3
			if _, err = js.CreateStream(actx, c); errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
				err = nil
			}
		}
		if err == nil {
			_, parked, err = bus.Ensure(actx, js, bus.Config{SubjectPrefix: prefix, ClaimBucket: bucket})
		}
		cancel()
		if err == nil {
			break
		}
		if !errors.Is(err, context.DeadlineExceeded) || attempt == 3 {
			t.Fatal(err)
		}
	}
	if clustered {
		testbus.AwaitStream(t, js, parked)
	}

	return js, parked
}

// stored returns the message the parking stream holds on subject, or nil.
func stored(t *testing.T, s jetstream.Stream, subject string) *jetstream.RawStreamMsg {
	t.Helper()
	m, err := s.GetLastMsgForSubject(t.Context(), subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}

	return m
}

func msgs(t *testing.T, s jetstream.Stream) uint64 {
	t.Helper()
	info, err := s.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return info.State.Msgs
}

// A version's subject names the park prefix, the tool token, the capture, the seq and the version token,
// the full hex SHA-256 of its header and digest, so two versions of one seq never share one.
func TestParkSubject(t *testing.T) {
	t.Parallel()
	capture := tracepack.UUID{1}
	a := delivered(transported(t, inline(t, capture, dataRecord(7, now), nil)))
	b := delivered(transported(t, otherVersion(t, capture, 7)))
	sa, err := parkSubject("tkpark", a.Msg.Subject, a.Raw.Version)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^tkpark\.T1\.`+capture.String()+`\.7\.[0-9a-f]{64}$`).MatchString(sa) || len(sa) >= 160 {
		t.Errorf("subject %q", sa)
	}
	if sb, _ := parkSubject("tkpark", b.Msg.Subject, b.Raw.Version); sb == sa {
		t.Error("two versions of one seq share a subject")
	}
	for _, bad := range []string{"tk", "tk.T1." + tracepack.UUID{2}.String()} {
		if _, err := parkSubject("tkpark", bad, a.Raw.Version); err == nil {
			t.Errorf("traffic subject %q, which does not end with the tool token and the version's capture, was taken", bad)
		}
	}
}

// Only a PubAck with Duplicate false, or error 10071 whose description parses to a strictly positive sequence, confirms a copy.
func TestConfirm(t *testing.T) {
	t.Parallel()
	wrong := func(code jetstream.ErrorCode, desc string) error {
		return fmt.Errorf("nats: %w", &jetstream.APIError{Code: 400, ErrorCode: code, Description: desc})
	}
	for name, tc := range map[string]struct {
		ack  *jetstream.PubAck
		err  error
		want Confirmation
	}{
		"stored":             {ack: &jetstream.PubAck{Sequence: 3}, want: Confirmed},
		"duplicate":          {ack: &jetstream.PubAck{Sequence: 3, Duplicate: true}, want: Suppressed},
		"10071 with seq":     {err: wrong(10071, "wrong last sequence: 5"), want: Confirmed},
		"10071 with zero":    {err: wrong(10071, "wrong last sequence: 0"), want: Unconfirmed},
		"10071 malformed":    {err: wrong(10071, "wrong last sequence: five"), want: Unconfirmed},
		"10071 negative":     {err: wrong(10071, "wrong last sequence: -5"), want: Unconfirmed},
		"10071 another text": {err: wrong(10071, "last sequence 5"), want: Unconfirmed},
		"10164":              {err: wrong(10164, "wrong last sequence"), want: Unconfirmed},
		"another code":       {err: wrong(10077, "wrong last sequence: 5"), want: Unconfirmed},
		"a timeout":          {err: context.DeadlineExceeded, want: Unconfirmed},
		"no responders":      {err: jetstream.ErrNoStreamResponse, want: Unconfirmed},
	} {
		if got := confirm(tc.ack, tc.err); got != tc.want {
			t.Errorf("%s: confirm = %v, want %v", name, got, tc.want)
		}
	}
}

// Against the bus, the copy's publish has three outcomes, asserted on what the parking stream stores:
// stored when its subject is empty; refused with 10071 and the stored sequence while the subject holds the copy
// (on a cluster the duplicate window answers first); a duplicate storing nothing for another version of the seq inside the window.
func TestPublishCopy(t *testing.T) {
	t.Parallel()
	for _, clustered := range []bool{false, true} {
		t.Run(map[bool]string{false: "standalone", true: "clustered"}[clustered], func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			js, parked := parkingBus(t, clustered)
			k := &Coordinator{Bus: js, ParkPrefix: busconfig.DefaultParkPrefix}
			capture := tracepack.UUID{3}
			a := delivered(transported(t, inline(t, capture, dataRecord(1, now), nil)))
			b := delivered(transported(t, otherVersion(t, capture, 1)))
			sa, _ := parkSubject(busconfig.DefaultParkPrefix, a.Msg.Subject, a.Raw.Version)
			sb, _ := parkSubject(busconfig.DefaultParkPrefix, b.Msg.Subject, b.Raw.Version)

			if c, err := k.Publish(ctx, a, catalog.VersionParking); err != nil || c != Confirmed {
				t.Fatalf("Publish into an empty subject = %v, %v; want confirmed", c, err)
			}
			cp := stored(t, parked, sa)
			if cp == nil || string(cp.Data) != string(a.Msg.Body) || cp.Header.Get(jetstream.MsgIDHeader) != a.Msg.Env.MsgID ||
				cp.Header.Get(tracepack.BusHeaderLayout) != "1" || cp.Header.Get(jetstream.ExpectedStreamHeader) != "" {
				t.Fatalf("stored copy %+v; want the body and headers unchanged, Nats-Expected-Stream removed", cp)
			}

			again := Confirmed // 10071: the subject holds a message, which can only be this version's copy
			if clustered {
				again = Suppressed // the duplicate window answers first
			}
			if c, err := k.Publish(ctx, a, catalog.VersionParked); err != nil || c != again {
				t.Errorf("Publish while the copy is stored = %v, %v; want %v", c, err, again)
			}
			if c, err := k.Publish(ctx, b, catalog.VersionParking); err != nil || c != Suppressed {
				t.Errorf("Publish of another version inside the window = %v, %v; want suppressed", c, err)
			}
			if n := msgs(t, parked); n != 1 || stored(t, parked, sb) != nil {
				t.Errorf("the parking stream holds %d messages, want only the first copy", n)
			}
			if _, err := k.Publish(ctx, a, catalog.VersionStaged); err == nil {
				t.Error("a resolved version was published")
			}
		})
	}
}

// lossy publishes and loses the PubAck of its first publish, as a timeout after the server stored the copy.
type lossy struct {
	jetstream.JetStream
	lost bool
}

func (l *lossy) PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := l.JetStream.PublishMsg(ctx, m, opts...)
	if !l.lost {
		l.lost = true
		return nil, context.DeadlineExceeded
	}

	return ack, err
}

// fetch pulls one delivery from the replica's traffic consumer.
func (r *replica) fetch(t *testing.T) (*Delivery, jetstream.Msg) {
	t.Helper()
	batch, err := r.g.Consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	m, ok := <-batch.Messages()
	if !ok {
		t.Fatal("no delivery")
	}
	msg, err := readMessage(m)
	if err != nil {
		t.Fatal(err)
	}
	msg.Incarnation = r.g.Incarnation.ID
	d := &Delivery{Msg: msg}
	d.Raw, d.Facts = ReadRaw(&d.Msg.Env, d.Msg.Body, 0)

	return d, m
}

// An original is parked whole: its version row, the copy stored, the row advanced, the original acknowledged and its copy row acked.
// A lost PubAck leaves the original unacknowledged; its redelivery is confirmed by the stored copy, which stays the only one.
func TestPark(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		parked, err := r.js.Stream(ctx, busconfig.DefaultParkedStream)
		if err != nil {
			t.Fatal(err)
		}
		k := &Coordinator{Catalog: c, Bus: &lossy{JetStream: r.js}, ParkPrefix: busconfig.DefaultParkPrefix}
		capture := tracepack.UUID{4}
		r.publish(t, inline(t, capture, dataRecord(1, now), nil), nil)

		d, m := r.fetch(t)
		p, err := k.Park(ctx, Entry{D: d, Msg: m}, ReasonUnknownCapture, 0)
		if err != nil || p.Confirmation != Unconfirmed || p.Begun.State != catalog.VersionParking {
			t.Fatalf("Park with the PubAck lost = %+v, %v; want unconfirmed", p, err)
		}
		if err := m.NakWithDelay(0); err != nil {
			t.Fatal(err)
		}
		d, m = r.fetch(t)
		if p, err = k.Park(ctx, Entry{D: d, Msg: m}, ReasonUnknownCapture, 0); err != nil || p.Confirmation != Confirmed {
			t.Fatalf("Park at the redelivery = %+v, %v; want confirmed by the stored copy", p, err)
		}
		if n := r.pending(t); n != 0 {
			t.Errorf("the traffic stream holds %d messages, want the original acknowledged", n)
		}
		if n := msgs(t, parked); n != 1 {
			t.Errorf("the parking stream holds %d copies, want 1", n)
		}
		vs, err := c.ParkedVersions(ctx, &capture)
		if err != nil || len(vs) != 1 || vs[0].State != catalog.VersionParked || len(vs[0].Copies) != 1 || vs[0].Copies[0].State != "acked" {
			t.Errorf("versions = %+v, %v; want parked, the original's copy row acked", vs, err)
		}
	})
}

// terminating commits a termination of capture before each publish, as one committed while the copy's publish was in flight.
type terminating struct {
	jetstream.JetStream
	c       *catalog.Catalog
	capture tracepack.UUID
}

func (p terminating) PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if _, err := p.c.TerminateParked(ctx, p.capture, nil, "op", "p1"); err != nil {
		return nil, err
	}

	return p.JetStream.PublishMsg(ctx, m, opts...)
}

// A termination committed between begin and advance is the version's resolution:
// the original is acknowledged under it, with its audit row.
func TestParkMeetsATerminationDuringThePublish(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		capture := tracepack.UUID{5}
		k := &Coordinator{Catalog: c, Bus: terminating{JetStream: r.js, c: c, capture: capture}, ParkPrefix: busconfig.DefaultParkPrefix}
		r.publish(t, inline(t, capture, dataRecord(1, now), nil), nil)
		d, m := r.fetch(t)
		if p, err := k.Park(ctx, Entry{D: d, Msg: m}, ReasonUnknownCapture, 0); err != nil || p.Confirmation != Confirmed {
			t.Fatalf("Park = %+v, %v", p, err)
		}
		es, err := c.AuditEntries(ctx, capture)
		if err != nil || !slices.ContainsFunc(es, func(e catalog.AuditEntry) bool { return e.Action == ReasonTerminated && e.ActionID == "p1" }) {
			t.Errorf("audit %+v, %v; want the original's acknowledgement under p1", es, err)
		}
		if got := copyStates(versions(t, c, capture), r.g.Incarnation.ID); !slices.Equal(got, []string{"acked"}) {
			t.Errorf("the original's copy row %v, want acked", got)
		}
	})
}

// A termination committed between the advance and the acknowledgement is found by the acknowledgement under the capture row,
// which is then recorded under it, with its audit row.
func TestAcknowledgementMeetsATerminationAfterTheAdvance(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		ctx := t.Context()
		r := newReplica(t, c)
		k := r.g.Coordinator
		capture := tracepack.UUID{6}
		r.publish(t, inline(t, capture, dataRecord(1, now), nil), nil)
		d, m := r.fetch(t)
		b, err := k.Begin(ctx, d, ReasonUnknownCapture, 0)
		if err != nil {
			t.Fatal(err)
		}
		if conf, err := k.Publish(ctx, d, b.State); err != nil || conf != Confirmed {
			t.Fatalf("Publish = %v, %v", conf, err)
		}
		if s, err := k.Advance(ctx, d); err != nil || s != catalog.VersionParked {
			t.Fatalf("Advance = %v, %v", s, err)
		}
		if _, err := c.TerminateParked(ctx, capture, nil, "op", "p1"); err != nil {
			t.Fatal(err)
		}
		if err := k.Acknowledge(ctx, d, m); err != nil {
			t.Fatal(err)
		}
		es, err := c.AuditEntries(ctx, capture)
		if err != nil || !slices.ContainsFunc(es, func(e catalog.AuditEntry) bool { return e.Action == ReasonTerminated && e.ActionID == "p1" }) {
			t.Errorf("audit %+v, %v; want the acknowledgement recorded under p1", es, err)
		}
	})
}
