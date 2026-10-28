package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/metrics"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// Static tokens of the tests' principals, each at least 32 characters.
const (
	readerToken = "a-static-token-of-a-pack-reader-0001"
	viewerToken = "a-static-token-of-a-viewer-000000001"
	scorerToken = "a-static-token-of-a-scorer-000000001"
)

func testAuthenticator(f auth.Failures) *auth.Authenticator {
	return auth.NewAuthenticator(auth.AuthenticatorConfig{StaticEnabled: true, Failures: f, Static: []auth.StaticToken{
		{Token: readerToken, Subject: "reader", Roles: []string{auth.RolePackReader}},
		{Token: viewerToken, Subject: "viewer", Roles: []string{auth.RoleViewer}},
		{Token: scorerToken, Subject: "scorer", Roles: []string{auth.RoleScorer}},
	}})
}

// recorder is a MachineService that records which procedures ran, and under whose principal.
type recorder struct {
	tracekeepv1connect.UnimplementedMachineServiceHandler
	mu      sync.Mutex
	entered map[string]string
}

func (r *recorder) enter(ctx context.Context, op string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entered == nil {
		r.entered = map[string]string{}
	}
	p, _ := PrincipalFrom(ctx)
	r.entered[op] = p.Subject
}

func (r *recorder) ran(op string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.entered[op]

	return s, ok
}

func (r *recorder) ListDevices(ctx context.Context, _ *connect.Request[tkv1.ListDevicesRequest]) (*connect.Response[tkv1.ListDevicesResponse], error) {
	r.enter(ctx, auth.OpListDevices)
	return connect.NewResponse(&tkv1.ListDevicesResponse{}), nil
}

func (r *recorder) ListCaptures(ctx context.Context, _ *connect.Request[tkv1.ListCapturesRequest]) (*connect.Response[tkv1.ListCapturesResponse], error) {
	r.enter(ctx, auth.OpListCaptures)
	return connect.NewResponse(&tkv1.ListCapturesResponse{}), nil
}

// ListPacks answers many hours, a body large enough for any compression threshold.
func (r *recorder) ListPacks(ctx context.Context, _ *connect.Request[tkv1.ListPacksRequest]) (*connect.Response[tkv1.ListPacksResponse], error) {
	r.enter(ctx, auth.OpListPacks)
	resp := &tkv1.ListPacksResponse{}
	for range 4096 {
		resp.Hours = append(resp.Hours, &tkv1.Hour{NoData: true, Evicted: []string{"00000000-0000-0000-0000-000000000000"}})
	}

	return connect.NewResponse(resp), nil
}

func (r *recorder) ListScopeChanges(ctx context.Context, _ *connect.Request[tkv1.ListScopeChangesRequest]) (*connect.Response[tkv1.ListScopeChangesResponse], error) {
	r.enter(ctx, auth.OpListScopeChanges)
	return connect.NewResponse(&tkv1.ListScopeChangesResponse{}), nil
}

func (r *recorder) PackURL(ctx context.Context, _ *connect.Request[tkv1.PackURLRequest]) (*connect.Response[tkv1.PackURLResponse], error) {
	r.enter(ctx, auth.OpPackURL)
	return connect.NewResponse(&tkv1.PackURLResponse{}), nil
}

// failures counts the reasons auth refused.
type failures struct {
	mu sync.Mutex
	n  map[string]int
}

func (f *failures) AuthFailure(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n == nil {
		f.n = map[string]int{}
	}
	f.n[reason]++
}

// fixture is a recorder's routes behind testAuthenticator, served over HTTP, with a client for them.
type fixture struct {
	rec *recorder
	f   *failures
	c   tracekeepv1connect.MachineServiceClient
	srv *httptest.Server
}

func served(t *testing.T, m *metrics.API) fixture {
	t.Helper()
	x := fixture{rec: &recorder{}, f: &failures{}}
	s := &Server{Auth: testAuthenticator(x.f), Metrics: m}
	x.srv = httptest.NewServer(s.routes(x.rec))
	t.Cleanup(x.srv.Close)
	x.c = tracekeepv1connect.NewMachineServiceClient(x.srv.Client(), x.srv.URL)

	return x
}

func withToken[T any](msg *T, token string) *connect.Request[T] {
	req := connect.NewRequest(msg)
	if token != "" {
		req.Header().Set("Authorization", "Bearer "+token)
	}

	return req
}

// calls runs every procedure with token and returns each one's Connect code, 0 for success.
func calls(c tracekeepv1connect.MachineServiceClient, token string) map[string]connect.Code {
	ctx := context.Background()
	code := func(err error) connect.Code {
		if err == nil {
			return 0
		}

		return connect.CodeOf(err)
	}
	_, e1 := c.ListDevices(ctx, withToken(&tkv1.ListDevicesRequest{}, token))
	_, e2 := c.ListCaptures(ctx, withToken(&tkv1.ListCapturesRequest{}, token))
	_, e3 := c.ListPacks(ctx, withToken(&tkv1.ListPacksRequest{}, token))
	_, e4 := c.ListScopeChanges(ctx, withToken(&tkv1.ListScopeChangesRequest{}, token))
	_, e5 := c.PackURL(ctx, withToken(&tkv1.PackURLRequest{}, token))

	return map[string]connect.Code{
		auth.OpListDevices: code(e1), auth.OpListCaptures: code(e2), auth.OpListPacks: code(e3),
		auth.OpListScopeChanges: code(e4), auth.OpPackURL: code(e5),
	}
}

// Every operation is refused before its handler runs: without a credential as unauthenticated, and without its role as permission denied;
// a principal holding the role reaches the handler, its principal in the context.
func TestEveryOperationBehindAuth(t *testing.T) {
	t.Parallel()
	x := served(t, nil)
	rec, f, c := x.rec, x.f, x.c
	for op, code := range calls(c, "") {
		if code != connect.CodeUnauthenticated {
			t.Errorf("%s without a credential = %v, want unauthenticated", op, code)
		}
	}
	for op, code := range calls(c, "not-any-static-token-at-all-0000000") {
		if code != connect.CodeUnauthenticated {
			t.Errorf("%s with an unknown token = %v, want unauthenticated", op, code)
		}
	}
	for op, code := range calls(c, scorerToken) {
		if code != connect.CodePermissionDenied {
			t.Errorf("%s as a scorer = %v, want permission denied", op, code)
		}
	}
	for _, op := range []string{auth.OpListDevices, auth.OpListCaptures, auth.OpListPacks, auth.OpListScopeChanges, auth.OpPackURL} {
		if _, ok := rec.ran(op); ok {
			t.Errorf("%s ran for a refused request", op)
		}
	}
	viewer := calls(c, viewerToken)
	for op, want := range map[string]connect.Code{
		auth.OpListDevices: 0, auth.OpListCaptures: 0, auth.OpListScopeChanges: 0,
		auth.OpListPacks: connect.CodePermissionDenied, auth.OpPackURL: connect.CodePermissionDenied,
	} {
		if viewer[op] != want {
			t.Errorf("%s as a viewer = %v, want %v", op, viewer[op], want)
		}
	}
	for op, code := range calls(c, readerToken) {
		if subject, ok := rec.ran(op); code != 0 || !ok || subject != "reader" && subject != "viewer" {
			t.Errorf("%s as a pack reader = %v, ran %v as %q", op, code, ok, subject)
		}
	}
	if s, _ := rec.ran(auth.OpListPacks); s != "reader" {
		t.Errorf("ListPacks ran as %q, want the reader's principal", s)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n[auth.ReasonMissing] != 5 || f.n[auth.ReasonStatic] != 5 || f.n[auth.ReasonForbidden] != 7 {
		t.Errorf("auth failures %v, want 5 missing, 5 static, 7 forbidden", f.n)
	}
}

// A procedure the service does not have is refused before the mux answers it.
func TestUnknownProcedureRefused(t *testing.T) {
	t.Parallel()
	srv := served(t, nil).srv
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/tracekeep.v1.MachineService/Export", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+readerToken)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("an unknown procedure = %d, want 403 permission denied", resp.StatusCode)
	}
}

// Responses are never compressed, whatever the client accepts (T-7).
func TestCompressionOff(t *testing.T) {
	t.Parallel()
	srv := served(t, nil).srv
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+tracekeepv1connect.MachineServiceListPacksProcedure, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Authorization", "Bearer "+readerToken)
	tr := srv.Client().Transport.(*http.Transport).Clone() //nolint:errcheck // httptest's client has an *http.Transport
	tr.DisableCompression = true                           // see the encoding as sent
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Encoding") != "" || !bytes.HasPrefix(body, []byte("{")) {
		t.Errorf("ListPacks with gzip accepted = %d, Content-Encoding %q, body %.20q; want an uncompressed answer",
			resp.StatusCode, resp.Header.Get("Content-Encoding"), body)
	}
}

func TestRequestMetrics(t *testing.T) {
	t.Parallel()
	s := metrics.NewServer()
	m, err := metrics.NewAPI(s)
	if err != nil {
		t.Fatal(err)
	}
	c := served(t, m).c
	calls(c, readerToken)
	calls(c, "")
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/metrics") //nolint:noctx // a test's local scrape
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, want := range []string{
		`tracekeep_api_requests_total{code="ok",operation="ListPacks"} 1`,
		`tracekeep_api_requests_total{code="unauthenticated",operation="ListPacks"} 1`,
		`tracekeep_api_request_seconds_count{operation="PackURL"} 2`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("no %s in the metrics", want)
		}
	}
}

func TestRefusalCodes(t *testing.T) {
	t.Parallel()
	for reason, want := range map[string]connect.Code{
		auth.ReasonForbidden: connect.CodePermissionDenied, auth.ReasonMissing: connect.CodeUnauthenticated,
		auth.ReasonExpired: connect.CodeUnauthenticated, auth.ReasonIDPUnavailable: connect.CodeUnauthenticated,
	} {
		if got := connect.CodeOf(refusal(&auth.Error{Reason: reason, Err: errors.New("x")})); got != want {
			t.Errorf("refusal(%s) = %v, want %v", reason, got, want)
		}
	}
}
