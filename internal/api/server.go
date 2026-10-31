// Package api is the api role's machine API (api.md §10.1): the catalog's reads and PackURL over connect-go,
// every request authenticated and authorized by internal/auth before any handler runs, and the downloads the API serves itself.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// Server is the machine API.
type Server struct {
	tracekeepv1connect.UnimplementedMachineServiceHandler

	Catalog Catalog
	// QuietPeriod is Q, against which the reads report unregistered intervals (catalog.md §7.3).
	QuietPeriod time.Duration
	// Admin runs AdminService's commands, its Actor replaced by each request's principal; nil runs none.
	Admin *admin.Admin
	// Store serves the downloads the API serves itself and the packs Restore reads; Downloads issues every download URL.
	Store     store.ObjectStore
	Downloads Downloads
	// Keys resolves the keys Restore and Digest use; it must be set before either is served.
	Keys Keys
	// DigestLimit is Digest's rate limit; nil refuses every Digest.
	DigestLimit *DigestLimit
	// Auth checks every request; Metrics records them, nil for none; Log logs failures, nil for none.
	Auth *auth.Authenticator
	// People serves people's sign-in under /auth/; nil serves none, and /auth/ is not found.
	People  *auth.People
	Metrics *metrics.API
	Log     *slog.Logger
	// Records bounds QueryRecords and FindTransaction; Dev, set by tracekeep dev, withholds no capture (T-48).
	Records RecordReads
	// Exports configures the export jobs (api.md §10.1 *Exports*).
	Exports ExportConfig
	Dev     bool

	// stall, set only by the package's tests, runs at each stage of a PackURL: read, signed, audited.
	stall func(stage string)
}

func (s *Server) stalled(stage string) {
	if s.stall != nil {
		s.stall(stage)
	}
}

var _ tracekeepv1connect.MachineServiceHandler = (*Server)(nil)

// Catalog is what the machine API reads and writes of the catalog; a *catalog.Catalog is one.
type Catalog interface {
	ListDevices(ctx context.Context, sel catalog.Selector, q time.Duration) ([]catalog.DeviceInfo, error)
	ListCaptures(ctx context.Context, sel catalog.Selector, q time.Duration) ([]catalog.CaptureInfo, error)
	ListPacks(ctx context.Context, sel catalog.Selector, q time.Duration) (catalog.Listing, error)
	ListScopeChanges(ctx context.Context, after catalog.Cursor, sel catalog.Selector, limit int) (catalog.Changes, error)
	PackDownload(ctx context.Context, pack tracepack.UUID, delay time.Duration) (catalog.Download, error)
	Audit(ctx context.Context, e catalog.AuditEntry) error
	Capture(ctx context.Context, id tracepack.UUID) (catalog.Capture, error)
	ConfigContent(ctx context.Context, id int64) ([]byte, error)
	RecordCopies(ctx context.Context, capture tracepack.UUID, seq uint64) ([]catalog.Copy, error)
	Configs(ctx context.Context) ([]catalog.ConfigInfo, error)
	Policy(ctx context.Context, id string, version int64) (catalog.StoredPolicy, bool, error)
	Completeness(ctx context.Context, capture tracepack.UUID) (catalog.Completeness, error)
	ReadSet(ctx context.Context, capture tracepack.UUID, hour int64) (catalog.ReadSet, error)
}

// Handler is the HTTP handler serving the machine API.
func (s *Server) Handler() http.Handler { return s.routes(s) }

// principalKey is the context key of the authenticated principal.
type principalKey struct{}

// PrincipalFrom returns the principal the auth middleware authenticated for the request ctx belongs to.
func PrincipalFrom(ctx context.Context) (auth.Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(auth.Principal)

	return p, ok
}

// operations are the procedures the service serves; any other path under it is opUnknown, one label, and refused.
var operations = map[string]bool{
	auth.OpListDevices: true, auth.OpListCaptures: true, auth.OpListPacks: true, auth.OpListScopeChanges: true, auth.OpPackURL: true,
	auth.OpRestore: true, auth.OpDigest: true, auth.OpAdmin: true, auth.OpQueryRecords: true, auth.OpFindTransaction: true,
}

const opUnknown = "unknown"

// routes is the HTTP handler serving impl behind the auth middleware, with response compression off (T-7),
// requests bounded and their decode errors quoting none of their bytes,
// and the downloads the API serves when it has download tokens.
// Downloads and unclean paths are answered before the mux, which would redirect an unclean path to its clean form,
// a download's token in the redirect.
func (s *Server) routes(impl tracekeepv1connect.MachineServiceHandler) http.Handler {
	mux := http.NewServeMux()
	opts := append(codecs(), connect.WithCompression("gzip", nil, nil), connect.WithInterceptors(s.observe()))
	prefix, h := tracekeepv1connect.NewMachineServiceHandler(impl, opts...)
	mux.Handle(prefix, s.authenticate(prefix, h))
	adminPrefix, ah := tracekeepv1connect.NewAdminServiceHandler(adminService{s: s}, opts...)
	mux.Handle(adminPrefix, s.authenticate(adminPrefix, ah))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch p := r.URL.Path; {
		case strings.HasPrefix(p, "/download/") && s.Downloads.Tokens != nil:
			s.download(&downloadWriter{ResponseWriter: w}, r)
		case strings.HasPrefix(p, "/auth/") && (s.People == nil || path.Clean(p) != p):
			// Not found, with the protections of every answer under /auth/; an unclean path is never redirected, which would echo its query.
			auth.Protect(w, r)
			http.NotFound(w, r)
		case strings.HasPrefix(p, "/auth/"):
			s.People.Handler().ServeHTTP(w, r)
		case strings.HasPrefix(p, "/download/") || path.Clean(p) != p:
			http.NotFound(&downloadWriter{ResponseWriter: w}, r)
		default:
			mux.ServeHTTP(w, r)
		}
	})
}

// authenticate refuses, as a Connect error, every request auth.Check refuses for the procedure's operation,
// so no handler runs for it; an admitted request carries its principal in its context.
func (s *Server) authenticate(prefix string, next http.Handler) http.Handler {
	errs := connect.NewErrorWriter()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		op := strings.TrimPrefix(r.URL.Path, prefix)
		if !operations[op] {
			op = opUnknown
		}
		p, err := s.Auth.Check(r, op)
		if err != nil {
			err = refusal(err)
			s.Metrics.Request(op, code(err), time.Since(start))
			_ = errs.Write(w, r, err)

			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}

// refusal is the Connect error of auth's refusal err: permission denied for forbidden, unauthenticated for every other reason.
// Its message is the reason alone.
func refusal(err error) error {
	var e *auth.Error
	if !errors.As(err, &e) {
		return connect.NewError(connect.CodeUnauthenticated, errors.New(auth.ReasonMalformed))
	}
	if e.Reason == auth.ReasonForbidden {
		return connect.NewError(connect.CodePermissionDenied, errors.New(e.Reason))
	}

	return connect.NewError(connect.CodeUnauthenticated, errors.New(e.Reason))
}

// observe records each handled request's operation, code and duration, a stream's at its end, and logs a failure and a refused restore.
func (s *Server) observe() connect.Interceptor { return observer{s} }

type observer struct{ s *Server }

func (o observer) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		start := time.Now()
		resp, err := next(ctx, req)
		o.s.observed(req.Spec().Procedure, start, err)

		return resp, err
	}
}

func (o observer) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (o observer) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		start := time.Now()
		err := next(ctx, conn)
		o.s.observed(conn.Spec().Procedure, start, err)

		return err
	}
}

func (s *Server) observed(procedure string, start time.Time, err error) {
	op := procedure[strings.LastIndex(procedure, "/")+1:]
	s.Metrics.Request(op, code(err), time.Since(start))
	if err == nil || s.Log == nil {
		return
	}
	switch c := connect.CodeOf(err); {
	case c == connect.CodeInternal || c == connect.CodeUnavailable:
		s.Log.Error("api: request failed", "operation", op, "error", err)
	case op == auth.OpRestore:
		// A refused restore is logged too, its error naming the request and the reason (masking-keys.md §9).
		s.Log.Warn("api: request refused", "operation", op, "error", err)
	default: // another operation's refusal is only counted
	}
}

// code is err's Connect code as a label, ok for none.
func code(err error) string {
	if err == nil {
		return "ok"
	}

	return connect.CodeOf(err).String()
}
