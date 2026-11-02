package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
)

// firstChunk is what a cutStore's object serves before it fails: ServeContent's first read.
const firstChunk = 32 << 10

// timeoutError is a network timeout, as a store's client reports one.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// cutStore fails as mode says, every error naming the key:
// open-held waits at its open for the download's context to end, open-timeout times its open out;
// read-held, read-timeout and read-eof serve the first chunk, then hold every later read until the context ends, time it out, or end it short with an end of file.
// arrived is signalled when a hold begins; opened and closed count the objects.
type cutStore struct {
	store.ObjectStore
	mode           string
	arrived        chan struct{}
	opened, closed atomic.Int32
}

func (c *cutStore) Open(ctx context.Context, key string) (store.Object, error) {
	switch c.mode {
	case "open-held":
		c.arrived <- struct{}{}
		<-ctx.Done()

		return nil, fmt.Errorf("get %s: %w", key, ctx.Err())
	case "open-timeout":
		return nil, fmt.Errorf("get %s: %w", key, timeoutError{})
	}
	obj, err := c.ObjectStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	c.opened.Add(1)

	return &cutObject{Object: obj, c: c, ctx: ctx, key: key}, nil
}

type cutObject struct {
	store.Object
	c   *cutStore
	ctx context.Context //nolint:containedctx // the download's context, which a held read waits on
	key string
}

func (o *cutObject) ReadAt(p []byte, off int64) (int, error) {
	if off < firstChunk {
		return o.Object.ReadAt(p, off)
	}
	if o.c.mode == "read-held" {
		select {
		case o.c.arrived <- struct{}{}:
		default:
		}
		<-o.ctx.Done()

		return 0, fmt.Errorf("read %s: %w", o.key, o.ctx.Err())
	}
	if o.c.mode == "read-eof" {
		// A ranged GET whose body came short: part of the range, then an end of file inside the object.
		n, _ := o.Object.ReadAt(p[:len(p)/2], off)

		return n, fmt.Errorf("read %s: %w", o.key, io.EOF)
	}

	return 0, fmt.Errorf("read %s: %w", o.key, timeoutError{})
}

func (o *cutObject) Close() error {
	o.c.closed.Add(1)

	return o.Object.Close()
}

// cutCase is a download failing as its cutStore's mode says, of its range if any;
// its client cancels when cancel is set, its budget ends in the case when budget is set, and it is counted under status, logged when logged is set.
type cutCase struct {
	name, mode, rng string
	cancel, budget  bool
	status          string
	logged          bool
}

// A download whose client goes is answered nothing more, counted under 499 and never logged, before its headers or after;
// one whose budget ends or whose store times out while its client is there answers 502 before the headers, and after them,
// of a 200 or a 206, ends the body short, counted under the status sent; either logs one keyless line. The object is closed every time.
func TestDownloadFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []cutCase{
		{"the client gone before the headers", "open-held", "", true, false, "499", false},
		{"the budget's end before the headers", "open-held", "", false, true, "502", true},
		{"the store timed out before the headers", "open-timeout", "", false, false, "502", true},
		{"the client gone after the headers of a 200", "read-held", "", true, false, "499", false},
		{"the client gone after the headers of a 206", "read-held", "bytes=10-200000", true, false, "499", false},
		{"the budget's end after the headers of a 200", "read-held", "", false, true, "200", true},
		{"the budget's end after the headers of a 206", "read-held", "bytes=10-200000", false, true, "206", true},
		{"the store timed out after the headers of a 200", "read-timeout", "", false, false, "200", true},
		{"the store timed out after the headers of a 206", "read-timeout", "bytes=10-200000", false, false, "206", true},
		{"the store's read short of the object after the headers of a 200", "read-eof", "", false, false, "200", true},
		{"the store's read short of the object after the headers of a 206", "read-eof", "bytes=10-200000", false, false, "206", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func (tc *cutCase) run(t *testing.T) {
	t.Helper()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	mem := store.NewMem()
	body := bytes.Repeat([]byte("0123456789abcdef"), 1<<14) // 256 KiB, several reads
	if err := mem.Put(t.Context(), objectKey, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	clock := func() time.Time { return now }
	st := &cutStore{ObjectStore: mem, mode: tc.mode, arrived: make(chan struct{}, 1)}
	tokens := &auth.Downloads{Keys: downloadKeys(t, c), Now: clock}
	ms := metrics.NewServer()
	m, err := metrics.NewAPI(ms)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	s := &Server{Catalog: c, Auth: testAuthenticator(nil), Store: st, Downloads: Downloads{Tokens: tokens, Now: clock}, Metrics: m,
		Log: slog.New(slog.NewTextHandler(&logs, nil))}
	done := make(chan struct{}, 1)
	h := s.Handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
		done <- struct{}{}
	}))
	t.Cleanup(srv.Close)
	// A budget that ends in the case, else one that never does.
	budget := time.Hour
	if tc.budget {
		budget = time.Second
	}
	tok, err := tokens.Sign(t.Context(), objectKey, now.Add(budget))
	if err != nil {
		t.Fatal(err)
	}
	tc.request(t, srv, st, "/download/"+tok, len(body))
	<-done
	wantStatus := fmt.Sprintf(`tracekeep_api_requests_total{code=%q,operation="Download"} 1`, tc.status)
	if metricsText := scrape(t, ms); !strings.Contains(metricsText, wantStatus) {
		t.Errorf("no %s in the metrics:\n%s", wantStatus, grep(metricsText, "Download"))
	}
	want := 0
	if tc.logged {
		want = 1
	}
	if lines := strings.Count(logs.String(), "api: download failed"); lines != want || strings.Count(logs.String(), "\n") != want || strings.Contains(logs.String(), objectKey) {
		t.Errorf("the log = %q, want %d keyless failure lines", logs.String(), want)
	}
	if st.opened.Load() != st.closed.Load() {
		t.Errorf("%d objects opened, %d closed", st.opened.Load(), st.closed.Load())
	}
}

// request downloads path, cancelling once the store holds when the case cancels, and checks the answer:
// 502 when the open fails, else a body cut short of size unless the client cancelled.
func (tc *cutCase) request(t *testing.T, srv *httptest.Server, st *cutStore, path string, size int) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+path, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	if tc.rng != "" {
		req.Header.Set("Range", tc.rng)
	}
	type answer struct {
		resp *http.Response
		err  error
	}
	answered := make(chan answer, 1)
	go func() {
		resp, err := srv.Client().Do(req)
		answered <- answer{resp, err}
	}()
	if tc.mode == "open-held" {
		<-st.arrived
		if tc.cancel {
			cancel()
		}
	}
	a := <-answered
	if a.err != nil {
		return
	}
	defer a.resp.Body.Close()
	// After the headers: read what was sent, going once the read past it is held.
	head := make([]byte, 1024)
	n, _ := io.ReadFull(a.resp.Body, head)
	if tc.cancel {
		<-st.arrived
		cancel()
	}
	rest, rerr := io.ReadAll(a.resp.Body)
	got := n + len(rest)
	if cut := strings.HasPrefix(tc.mode, "read-"); cut && !tc.cancel && (rerr == nil || got >= size) || !cut && a.resp.StatusCode != http.StatusBadGateway {
		t.Errorf("%d, the body %d bytes, %v; want 502 before the headers, a body cut short after them", a.resp.StatusCode, got, rerr)
	}
}
