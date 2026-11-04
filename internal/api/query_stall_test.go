package api

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// smallBuffers is a listener whose connections send through a small socket buffer, so a client that stops reading stalls the server's writes soon;
// it counts the connections it accepts.
type smallBuffers struct {
	net.Listener
	accepted atomic.Int32
}

func (l *smallBuffers) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetWriteBuffer(64 << 10)
	}
	if err == nil {
		l.accepted.Add(1)
	}

	return c, err
}

// throttled is a client connection that reads at 512 KiB a second, at most 4 KiB at a time: a client that keeps reading, slowly.
type throttled struct{ net.Conn }

func (c throttled) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p[:min(len(p), 4<<10)])
	time.Sleep(time.Duration(n) * time.Second / (512 << 10)) //nolint:forbidigo // the slow client's pace, a delay injected into the scenario

	return n, err
}

// counted is a store counting the objects opened and closed.
type counted struct {
	store.ObjectStore
	opened, closed atomic.Int32
}

func (c *counted) Open(ctx context.Context, key string) (store.Object, error) {
	obj, err := c.ObjectStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	c.opened.Add(1)

	return countedObject{obj, c}, nil
}

type countedObject struct {
	store.Object
	c *counted
}

func (o countedObject) Close() error {
	o.c.closed.Add(1)

	return o.Object.Close()
}

// stallWorld is m's API served over HTTP/2 with TLS, or HTTP/1.1, on small socket buffers at both ends and a 64 KiB client stream window,
// its QueryRecords streams' ends signalled on ended and its requests counted in sm.
type stallWorld struct {
	*masked
	cl    tracekeepv1connect.MachineServiceClient
	ln    *smallBuffers
	ended chan struct{}
	sm    *metrics.Server
	st    *counted
}

func newStallWorld(t *testing.T, c *catalog.Catalog, h2, slow bool, opts ...connect.ClientOption) *stallWorld {
	t.Helper()
	w := &stallWorld{masked: newMasked(t, c), ended: make(chan struct{}, 8), sm: metrics.NewServer()}
	am, err := metrics.NewAPI(w.sm)
	if err != nil {
		t.Fatal(err)
	}
	w.srv.Metrics, w.srv.Dev = am, true
	w.st = &counted{ObjectStore: w.masked.st}
	w.srv.Store = w.st
	h := w.srv.Handler()
	hs := httptest.NewUnstartedServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(rw, r)
		if strings.HasSuffix(r.URL.Path, "/QueryRecords") {
			w.ended <- struct{}{}
		}
	}))
	w.ln = &smallBuffers{Listener: hs.Listener}
	hs.Listener = w.ln
	if h2 {
		hs.EnableHTTP2 = true
		hs.StartTLS()
	} else {
		hs.Start()
	}
	t.Cleanup(hs.Close)
	tr, _ := hs.Client().Transport.(*http.Transport)
	tr = tr.Clone()
	tr.HTTP2 = &http.HTTP2Config{MaxReceiveBufferPerStream: 64 << 10}
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetReadBuffer(64 << 10)
		}
		if err != nil || !slow {
			return c, err
		}

		return throttled{c}, nil
	}
	t.Cleanup(tr.CloseIdleConnections)
	w.cl = tracekeepv1connect.NewMachineServiceClient(&http.Client{Transport: tr}, hs.URL, opts...)

	return w
}

// bigCapture puts n records of about size bytes each in one pack of a capture, under tracekeep dev.
func (w *stallWorld) bigCapture(t *testing.T, n, size int) tracepack.UUID {
	t.Helper()
	capture := w.admitUnmasked(t)
	cs := carriers(t)
	rs := make([]tracepack.Record, 0, n)
	for i := range n {
		r := recordAt(&cs[0], hour, uint64(i+1)) //nolint:gosec // small seqs
		r.Payload = append(bytes.Clone(r.Payload), make([]byte, size)...)
		rs = append(rs, r)
	}
	w.put(t, capture, hour, rs, nil, nil)

	return capture
}

// awaitEnds waits, bounded, for n QueryRecords streams to end.
func (w *stallWorld) awaitEnds(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-w.ended:
		case <-time.After(20 * time.Second):
			t.Fatal("a stream did not end")
		}
	}
}

// A stream whose client stops reading ends once its send makes no progress for the send timeout, with deadline_exceeded,
// its read slot freed and every object it opened closed; over HTTP/2 a stream beside it on the same connection reads to its end.
// The client resumes from the last cursor it received and reads the rest, nothing twice. A binary and a JSON client each.
func TestQueryRecordsStalledStreamEnds(t *testing.T) {
	t.Parallel()
	for _, h2 := range []bool{false, true} {
		for _, codec := range clientCodecs {
			name := map[bool]string{false: "http1", true: "http2"}[h2] + "/" + codec.name
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				stalledStreamEnds(t, h2, codec.opts)
			})
		}
	}
}

// stalledStreamEnds is TestQueryRecordsStalledStreamEnds' case over HTTP/2 when h2, by a client of opts.
func stalledStreamEnds(t *testing.T, h2 bool, opts []connect.ClientOption) {
	t.Helper()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		const n = 500
		w := newStallWorld(t, c, h2, false, opts...)
		w.srv.Records.MaxReads, w.srv.Records.SendTimeout = 2, 500*time.Millisecond
		capture := w.bigCapture(t, n, 4000)
		stalled, err := w.cl.QueryRecords(t.Context(), query(capture, nil))
		if err != nil {
			t.Fatal(err)
		}
		defer stalled.Close()
		if !stalled.Receive() {
			t.Fatalf("the first message: %v", stalled.Err())
		}
		first := seqsOf(stalled.Msg().GetItems())
		cursor := stalled.Msg().GetCursor()
		ends := 1
		if h2 {
			// The stalled stream's window is full: an active stream on the same connection reads to its end.
			msgs, err := w.streamed(t, query(capture, nil))
			if err != nil {
				t.Fatalf("the active stream: %v", err)
			}
			if its, _ := items(t, msgs); len(seqsOf(its)) != n {
				t.Errorf("the active stream read %d records, want %d", len(seqsOf(its)), n)
			}
			if a := w.ln.accepted.Load(); a != 1 {
				t.Errorf("%d connections, want both streams on one", a)
			}
			ends++
		}
		w.awaitEnds(t, ends)
		if got := scrape(t, w.sm); !strings.Contains(got, `tracekeep_api_requests_total{code="deadline_exceeded",operation="QueryRecords"} 1`) {
			t.Errorf("no stalled stream counted with deadline_exceeded:\n%s", grep(got, "QueryRecords"))
		}
		if o, cl := w.st.opened.Load(), w.st.closed.Load(); o == 0 || o != cl {
			t.Errorf("%d objects opened, %d closed", o, cl)
		}
		releases := make([]func(), 0, 2)
		for range 2 {
			release, err := w.srv.acquire()
			if err != nil {
				t.Fatalf("a read slot after the stalled stream ended: %v", err)
			}
			releases = append(releases, release)
		}
		for _, release := range releases {
			release()
		}
		_ = stalled.Close()
		rest, err := w.masked.streamed(t, query(capture, func(q *tkv1.QueryRecordsRequest) { q.Cursor = cursor }))
		if err != nil {
			t.Fatalf("resumed: %v", err)
		}
		its, _ := items(t, rest)
		all := slices.Concat(first, seqsOf(its))
		if len(all) != n || all[0] != 1 || all[n-1] != n {
			t.Errorf("read %d records, from %d to %d, before and after the resumption; want each of %d once", len(all), all[0], all[len(all)-1], n)
		}
	})
}

// A client that keeps reading, too slowly for one message to arrive within the send timeout, is never ended by it.
func TestQueryRecordsSlowClientReadsToTheEnd(t *testing.T) {
	t.Parallel()
	for _, h2 := range []bool{false, true} {
		name := map[bool]string{false: "http1", true: "http2"}[h2]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
				// Two messages of about 900 KiB, a record each, each taking the slow client about 1.75 s, past the buffers.
				const n = 2
				w := newStallWorld(t, c, h2, true)
				w.srv.Records.SendTimeout = time.Second
				capture := w.bigCapture(t, n, 900<<10)
				msgs, err := w.streamed(t, query(capture, nil))
				if err != nil {
					t.Fatalf("a slow client's stream: %v", err)
				}
				if its, _ := items(t, msgs); len(seqsOf(its)) != n {
					t.Errorf("read %d records, want %d", len(seqsOf(its)), n)
				}
			})
		})
	}
}

// streamed is a stream's messages and its error, read through w's client.
func (w *stallWorld) streamed(t testing.TB, req *connect.Request[tkv1.QueryRecordsRequest]) ([]*tkv1.QueryRecordsResponse, error) {
	t.Helper()
	s, err := w.cl.QueryRecords(t.Context(), req)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	var out []*tkv1.QueryRecordsResponse
	for s.Receive() {
		out = append(out, s.Msg())
	}

	return out, s.Err()
}

// grep are text's lines holding s.
func grep(text, s string) string {
	var out []string
	for line := range strings.Lines(text) {
		if strings.Contains(line, s) {
			out = append(out, line)
		}
	}

	return strings.Join(out, "")
}
