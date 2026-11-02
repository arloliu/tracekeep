package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/metrics"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// Sentinels a dependency's error carries, which no MachineService answer may (api.md §10.1 *What an answer says*).
const (
	catalogSentinel = "dial tcp db-sentinel.invalid:3306: connect: connection refused"
	keySentinel     = "vault read secret/data/ref-sentinel: status 503"
	auditSentinel   = "audit-sentinel: disk I/O error"
)

// errMessage is err's Connect message, what a client reads.
func errMessage(err error) string {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce.Message()
	}
	if err == nil {
		return ""
	}

	return err.Error()
}

// held is a catalog whose call op, or every call it overrides when op is "*", fails:
// with release set, it first sends the request's context on arrived and waits for release.
// It fails with err, or, when err is nil, with the context's error as the catalog would report it.
type held struct {
	Catalog
	op      string
	arrived chan context.Context
	release chan struct{}
	err     error
}

func holding(c Catalog, op string) *held {
	return &held{Catalog: c, op: op, arrived: make(chan context.Context, 1), release: make(chan struct{})}
}

// failing is a catalog whose call op fails at once with err.
func failing(c Catalog, op string, err error) *held { return &held{Catalog: c, op: op, err: err} }

// holds tells whether op is held, after holding it.
func (h *held) holds(ctx context.Context, op string) bool {
	if h.op != op && h.op != "*" {
		return false
	}
	if h.release != nil {
		h.arrived <- ctx
		<-h.release
	}

	return true
}

func (h *held) fail(ctx context.Context) error {
	if h.err != nil {
		return h.err
	}

	return fmt.Errorf("catalog: %s: %w", catalogSentinel, ctx.Err())
}

func (h *held) ListDevices(ctx context.Context, sel catalog.Selector, q time.Duration) ([]catalog.DeviceInfo, error) {
	if h.holds(ctx, "ListDevices") {
		return nil, h.fail(ctx)
	}

	return h.Catalog.ListDevices(ctx, sel, q)
}

func (h *held) ListCaptures(ctx context.Context, sel catalog.Selector, q time.Duration) ([]catalog.CaptureInfo, error) {
	if h.holds(ctx, "ListCaptures") {
		return nil, h.fail(ctx)
	}

	return h.Catalog.ListCaptures(ctx, sel, q)
}

func (h *held) ListPacks(ctx context.Context, sel catalog.Selector, q time.Duration) (catalog.Listing, error) {
	if h.holds(ctx, "ListPacks") {
		return catalog.Listing{}, h.fail(ctx)
	}

	return h.Catalog.ListPacks(ctx, sel, q)
}

func (h *held) ListScopeChanges(ctx context.Context, after catalog.Cursor, sel catalog.Selector, limit int) (catalog.Changes, error) {
	if h.holds(ctx, "ListScopeChanges") {
		return catalog.Changes{}, h.fail(ctx)
	}

	return h.Catalog.ListScopeChanges(ctx, after, sel, limit)
}

func (h *held) PackDownload(ctx context.Context, pack tracepack.UUID, delay time.Duration) (catalog.Download, error) {
	if h.holds(ctx, "PackDownload") {
		return catalog.Download{}, h.fail(ctx)
	}

	return h.Catalog.PackDownload(ctx, pack, delay)
}

func (h *held) Audit(ctx context.Context, e catalog.AuditEntry) error {
	if h.holds(ctx, "Audit") {
		return h.fail(ctx)
	}

	return h.Catalog.Audit(ctx, e)
}

func (h *held) Capture(ctx context.Context, id tracepack.UUID) (catalog.Capture, error) {
	if h.holds(ctx, "Capture") {
		return catalog.Capture{}, h.fail(ctx)
	}

	return h.Catalog.Capture(ctx, id)
}

func (h *held) RecordCopies(ctx context.Context, capture tracepack.UUID, seq uint64, hour int64, limit int) ([]catalog.Copy, bool, error) {
	if h.holds(ctx, "RecordCopies") {
		return nil, false, h.fail(ctx)
	}

	return h.Catalog.RecordCopies(ctx, capture, seq, hour, limit)
}

func (h *held) Configs(ctx context.Context) ([]catalog.ConfigInfo, error) {
	if h.holds(ctx, "Configs") {
		return nil, h.fail(ctx)
	}

	return h.Catalog.Configs(ctx)
}

func (h *held) ExportJob(ctx context.Context, job tracepack.UUID, owner string) (catalog.ExportJob, error) {
	if h.holds(ctx, "ExportJob") {
		return catalog.ExportJob{}, h.fail(ctx)
	}

	return h.Catalog.ExportJob(ctx, job, owner)
}

func (h *held) ExportJobs(ctx context.Context, owner string, limit int) ([]catalog.ExportJob, error) {
	if h.holds(ctx, "ExportJobs") {
		return nil, h.fail(ctx)
	}

	return h.Catalog.ExportJobs(ctx, owner, limit)
}

func (h *held) CreateExport(ctx context.Context, id tracepack.UUID, owner string, request []byte, audit catalog.AuditEntry) error {
	if h.holds(ctx, "CreateExport") {
		return h.fail(ctx)
	}

	return h.Catalog.CreateExport(ctx, id, owner, request, audit)
}

func (h *held) CancelExport(ctx context.Context, job tracepack.UUID, owner string, audit catalog.AuditEntry) error {
	if h.holds(ctx, "CancelExport") {
		return h.fail(ctx)
	}

	return h.Catalog.CancelExport(ctx, job, owner, audit)
}

// finished signals each request's end once every interceptor inside it, the observing one included, has returned.
type finished chan struct{}

func (f finished) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		defer func() { f <- struct{}{} }()

		return next(ctx, req)
	}
}

func (f finished) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (f finished) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		defer func() { f <- struct{}{} }()

		return next(ctx, conn)
	}
}

// rig serves s with metrics and a log, signalling each request's end on done.
type rig struct {
	s    *Server
	ms   *metrics.Server
	logs *bytes.Buffer
	done finished
	hs   *httptest.Server
	cl   tracekeepv1connect.MachineServiceClient
}

func newRig(t *testing.T, s *Server, impl tracekeepv1connect.MachineServiceHandler) *rig {
	t.Helper()
	r := &rig{s: s, ms: metrics.NewServer(), logs: &bytes.Buffer{}, done: make(finished, 64)}
	m, err := metrics.NewAPI(r.ms)
	if err != nil {
		t.Fatal(err)
	}
	s.Metrics, s.Log = m, slog.New(slog.NewTextHandler(r.logs, nil))
	if s.Auth == nil {
		s.Auth = testAuthenticator(nil)
	}
	s.outer = append(s.outer, r.done)
	if impl == nil {
		impl = s
	}
	r.hs = httptest.NewServer(s.routes(impl))
	t.Cleanup(r.hs.Close)
	r.cl = tracekeepv1connect.NewMachineServiceClient(r.hs.Client(), r.hs.URL)

	return r
}

// counted checks that the scrape holds each line.
func (r *rig) counted(t *testing.T, lines ...string) {
	t.Helper()
	got := scrape(t, r.ms)
	for _, l := range lines {
		if !strings.Contains(got, l+"\n") {
			t.Errorf("no %s in the metrics", l)
		}
	}
}

// requests are one request of each operation the client-gone cases hold, with the catalog call each holds and the outcome it counts.
var requests = []struct {
	op, held, outcome string
	call              func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error
}{
	{auth.OpQueryRecords, "ListCaptures", "", func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
		st, err := cl.QueryRecords(ctx, query(tracepack.UUID{1}, nil))
		if err != nil {
			return err
		}
		for st.Receive() {
		}

		return st.Err()
	}},
	{auth.OpListCaptures, "ListCaptures", "", func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
		_, err := cl.ListCaptures(ctx, reader(&tkv1.ListCapturesRequest{Selector: sel(hour*nsPerHour, (hour+1)*nsPerHour)}))
		return err
	}},
	{auth.OpFindTransaction, "Capture", "", func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
		_, err := cl.FindTransaction(ctx, withToken(&tkv1.FindTransactionRequest{CaptureId: tracepack.UUID{1}.String(), Seq: 1, Hour: hourTS(hour)}, viewerToken))
		return err
	}},
	{auth.OpRestore, "RecordCopies", "restores_total", func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
		_, err := cl.Restore(ctx, restoreReq(tracepack.UUID{1}, 1, hour))
		return err
	}},
	{auth.OpDigest, "Configs", "digests_total", func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
		_, err := cl.Digest(ctx, digestReq("d-1", baselineInputs()...))
		return err
	}},
	{auth.OpPackURL, "PackDownload", "", func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
		_, err := cl.PackURL(ctx, reader(&tkv1.PackURLRequest{PackId: tracepack.UUID{1}.String()}))
		return err
	}},
}

// A client that cancels, or whose deadline passes, while its request is held at the catalog is answered by its own context's code:
// canceled or deadline_exceeded, counted under it, a restore's and a digest's outcome cancelled, and never logged as a failure.
func TestClientGoneWhileHeld(t *testing.T) {
	t.Parallel()
	for _, rq := range requests {
		for _, deadline := range []bool{false, true} {
			name, code := rq.op+"/cancelled", "canceled"
			if deadline {
				name, code = rq.op+"/deadline", "deadline_exceeded"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				h := holding(&stubCatalog{}, rq.held)
				s := &Server{Catalog: h}
				if deadline {
					// The deadline connect derives from the client's timeout, set on the server, so the client's own timer cannot close the connection first.
					s.outer = []connect.Interceptor{deadlined(300 * time.Millisecond)}
				}
				r := newRig(t, s, nil)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				ended := make(chan error, 1)
				go func() { ended <- rq.call(ctx, r.cl) }()
				srv := <-h.arrived
				if !deadline {
					cancel()
				}
				<-srv.Done()
				close(h.release)
				<-r.done
				if err := <-ended; err == nil {
					t.Error("a client gone was answered")
				}
				r.counted(t, fmt.Sprintf(`tracekeep_api_requests_total{code=%q,operation=%q} 1`, code, rq.op))
				if rq.outcome != "" {
					r.counted(t, fmt.Sprintf(`tracekeep_api_%s{outcome="cancelled"} 1`, rq.outcome))
				}
				if r.logs.Len() != 0 {
					t.Errorf("a client gone was logged: %s", r.logs)
				}
			})
		}
	}
}

// A request cut by the API's stop, its connection closed, is canceled on the server.
func TestRequestCutByTheStop(t *testing.T) {
	t.Parallel()
	h := holding(&stubCatalog{}, "ListCaptures")
	r := newRig(t, &Server{Catalog: h}, nil)
	ended := make(chan error, 1)
	go func() { ended <- requests[1].call(t.Context(), r.cl) }()
	srv := <-h.arrived
	r.hs.CloseClientConnections()
	<-srv.Done()
	close(h.release)
	<-r.done
	if err := <-ended; err == nil {
		t.Error("a request cut by the stop answered")
	}
	r.counted(t, `tracekeep_api_requests_total{code="canceled",operation="ListCaptures"} 1`)
}

// A dependency whose own deadline passes, the request's context live, answers unavailable with the catalog's fixed message, logged once with its cause.
func TestDependencyDeadlineStaysUnavailable(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("catalog: %s: %w", catalogSentinel, context.DeadlineExceeded)
	r := newRig(t, &Server{Catalog: failing(&stubCatalog{}, "ListCaptures", err)}, nil)
	got := requests[1].call(t.Context(), r.cl)
	<-r.done
	if connect.CodeOf(got) != connect.CodeUnavailable || errMessage(got) != msgCatalog || strings.Contains(got.Error(), "sentinel") {
		t.Errorf("a dependency's deadline = %v, want unavailable with the catalog's fixed message", got)
	}
	if n := strings.Count(r.logs.String(), "api: request failed"); n != 1 || !strings.Contains(r.logs.String(), "db-sentinel.invalid") {
		t.Errorf("the log = %q, want one failure naming its cause", r.logs)
	}
	r.counted(t, `tracekeep_api_requests_total{code="unavailable",operation="ListCaptures"} 1`)
}

// A cancellation racing an audit failure answers internal, and one racing a refusal keeps the refusal's code.
func TestClientGoneRacingADecidedCode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, op, code string
		err            error
		logged         bool
	}{
		{"an audit failure", "Audit", "internal", nil, true},
		{"a refusal", "PackDownload", "not_found", catalog.ErrUnknownPack, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Now()
			h := holding(&stubCatalog{download: catalog.Download{Known: true, Deadline: now.Add(time.Hour).UnixNano(), Key: "staging/k"}}, tc.op)
			h.err = tc.err
			s := &Server{Catalog: h, Downloads: Downloads{Presigner: &presigner{}, Lifetime: 15 * time.Minute, Floor: time.Minute, Now: func() time.Time { return now }}}
			r := newRig(t, s, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ended := make(chan error, 1)
			go func() { ended <- requests[5].call(ctx, r.cl) }()
			srv := <-h.arrived
			cancel()
			<-srv.Done()
			close(h.release)
			<-r.done
			if err := <-ended; err == nil {
				t.Error("a client gone was answered")
			}
			r.counted(t, fmt.Sprintf(`tracekeep_api_requests_total{code=%q,operation="PackURL"} 1`, tc.code))
			if logged := strings.Contains(r.logs.String(), "level=ERROR"); logged != tc.logged {
				t.Errorf("logged %q, want a failure logged %v", r.logs, tc.logged)
			}
		})
	}
}

// deadlined gives each request's context a deadline, as connect does from a client's timeout.
type deadlined time.Duration

func (d deadlined) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(d))
		defer cancel()

		return next(ctx, req)
	}
}

func (d deadlined) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (d deadlined) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(d))
		defer cancel()

		return next(ctx, conn)
	}
}

// cancelKey holds the cancel of a request's context the cancelling interceptor derived.
type cancelKey struct{}

// cancelling derives each request's context with a cancel, outside the observing interceptor.
type cancelling struct{}

func (cancelling) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		return next(context.WithValue(ctx, cancelKey{}, cancel), req)
	}
}

func (cancelling) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (cancelling) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// cancelAfter is a Server whose Restore and Digest cancel their request once they have returned, before the observing interceptor runs.
type cancelAfter struct{ *Server }

func (c cancelAfter) Restore(ctx context.Context, req *connect.Request[tkv1.RestoreRequest]) (*connect.Response[tkv1.RestoreResponse], error) {
	cancel, _ := ctx.Value(cancelKey{}).(context.CancelFunc)
	defer cancel()

	return c.Server.Restore(ctx, req)
}

func (c cancelAfter) Digest(ctx context.Context, req *connect.Request[tkv1.DigestRequest]) (*connect.Response[tkv1.DigestResponse], error) {
	cancel, _ := ctx.Value(cancelKey{}).(context.CancelFunc)
	defer cancel()

	return c.Server.Digest(ctx, req)
}

// A Restore or a Digest that answered unavailable while its request lived keeps that code, outcome and log line
// when its request is cancelled after it returned and before the observing interceptor decides.
func TestRestoreAndDigestDecideBeforeTheyCount(t *testing.T) {
	t.Parallel()
	for _, rq := range []int{3, 4} {
		t.Run(requests[rq].op, func(t *testing.T) {
			t.Parallel()
			s := &Server{Catalog: failing(&stubCatalog{}, requests[rq].held, errors.New("catalog: "+catalogSentinel)), outer: []connect.Interceptor{cancelling{}}}
			r := newRig(t, s, cancelAfter{s})
			err := requests[rq].call(t.Context(), r.cl)
			<-r.done
			if connect.CodeOf(err) != connect.CodeUnavailable || errMessage(err) != msgCatalog {
				t.Errorf("%s = %v, want unavailable with the catalog's fixed message", requests[rq].op, err)
			}
			r.counted(t, fmt.Sprintf(`tracekeep_api_requests_total{code="unavailable",operation=%q} 1`, requests[rq].op),
				fmt.Sprintf(`tracekeep_api_%s{outcome="unavailable"} 1`, requests[rq].outcome))
			if n := strings.Count(r.logs.String(), "api: request failed"); n != 1 {
				t.Errorf("the log = %q, want one failure", r.logs)
			}
		})
	}
}

// decide keeps every code tracekeep decides and turns an unavailable or unknown answer given once the request's context ended into that context's code.
func TestDecide(t *testing.T) {
	t.Parallel()
	live := t.Context()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, stop := context.WithDeadline(t.Context(), time.Unix(1, 0))
	defer stop()
	stalled := connect.NewError(connect.CodeDeadlineExceeded, errors.New("api: the stream's send made no progress for the send timeout"))
	for _, tc := range []struct {
		name string
		ctx  context.Context //nolint:containedctx // a case's request context
		err  error
		code connect.Code
		gone bool
	}{
		{"unavailable, the request live", live, dependency(connect.CodeUnavailable, msgStore, context.DeadlineExceeded), connect.CodeUnavailable, false},
		{"unavailable, the request cancelled", cancelled, dependency(connect.CodeUnavailable, msgStore, context.Canceled), connect.CodeCanceled, true},
		{"unavailable, the request past its deadline", expired, failed(errors.New("x")), connect.CodeDeadlineExceeded, true},
		{"unknown, the request cancelled", cancelled, context.Canceled, connect.CodeCanceled, true},
		{"a stalled send, the request cancelled", cancelled, stalled, connect.CodeDeadlineExceeded, false},
		{"an audit failure, the request cancelled", cancelled, dependency(connect.CodeInternal, msgAudit, context.Canceled), connect.CodeInternal, false},
		{"pack-superseded, the request cancelled", cancelled, superseded(tracepack.UUID{1}), connect.CodeFailedPrecondition, false},
		{"differing copies, the request past its deadline", expired, conflict(tracepack.UUID{1}, 1), connect.CodeFailedPrecondition, false},
	} {
		gone, err := decide(tc.ctx, tc.err)
		if connect.CodeOf(err) != tc.code || gone != tc.gone {
			t.Errorf("%s: decide = %v, %v; want %v, gone %v", tc.name, err, gone, tc.code, tc.gone)
		}
		var ce, was *connect.Error
		if errors.As(tc.err, &was) && !tc.gone && (!errors.As(err, &ce) || ce.Message() != was.Message() || len(ce.Details()) != len(was.Details())) {
			t.Errorf("%s: decide changed the answer %v to %v", tc.name, tc.err, err)
		}
		// A decided error is decided once: deciding it again under another context changes nothing.
		if again, err2 := decide(cancelled, err); err2 != err || again != gone { //nolint:errorlint // the same error, unchanged
			t.Errorf("%s: deciding again = %v, %v", tc.name, err2, again)
		}
	}
	if gone, err := decide(cancelled, nil); gone || err != nil {
		t.Errorf("decide(nil) = %v, %v", gone, err)
	}
}

// stubCatalog is a Catalog for requests a held or failing catalog answers before any real read:
// it answers PackDownload with download, ExportJob with job, and every other call it implements with nothing.
type stubCatalog struct {
	Catalog
	download catalog.Download
	job      catalog.ExportJob
}

func (c *stubCatalog) PackDownload(context.Context, tracepack.UUID, time.Duration) (catalog.Download, error) {
	return c.download, nil
}

func (c *stubCatalog) ListPacks(context.Context, catalog.Selector, time.Duration) (catalog.Listing, error) {
	return catalog.Listing{}, nil
}

func (c *stubCatalog) ExportJob(context.Context, tracepack.UUID, string) (catalog.ExportJob, error) {
	return c.job, nil
}

func (c *stubCatalog) Audit(context.Context, catalog.AuditEntry) error { return nil }
