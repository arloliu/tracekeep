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
	// Store serves the downloads the API serves itself; Downloads issues every download URL.
	Store     store.ObjectStore
	Downloads Downloads
	// Auth checks every request; Metrics records them, nil for none; Log logs failures, nil for none.
	Auth    *auth.Authenticator
	Metrics *metrics.API
	Log     *slog.Logger

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
	auth.OpAdmin: true,
}

const opUnknown = "unknown"

// routes is the HTTP handler serving impl behind the auth middleware, with response compression off (T-7),
// and the downloads the API serves when it has download tokens.
// Downloads and unclean paths are answered before the mux, which would redirect an unclean path to its clean form,
// a download's token in the redirect.
func (s *Server) routes(impl tracekeepv1connect.MachineServiceHandler) http.Handler {
	mux := http.NewServeMux()
	prefix, h := tracekeepv1connect.NewMachineServiceHandler(impl,
		connect.WithCompression("gzip", nil, nil), connect.WithInterceptors(s.observe()))
	mux.Handle(prefix, s.authenticate(prefix, h))
	adminPrefix, ah := tracekeepv1connect.NewAdminServiceHandler(adminService{s: s},
		connect.WithCompression("gzip", nil, nil), connect.WithInterceptors(s.observe()))
	mux.Handle(adminPrefix, s.authenticate(adminPrefix, ah))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch p := r.URL.Path; {
		case strings.HasPrefix(p, "/download/") && s.Downloads.Tokens != nil:
			s.download(&downloadWriter{ResponseWriter: w}, r)
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

// observe records each handled request's operation, code and duration.
func (s *Server) observe() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			op := req.Spec().Procedure[strings.LastIndex(req.Spec().Procedure, "/")+1:]
			s.Metrics.Request(op, code(err), time.Since(start))
			if c := connect.CodeOf(err); err != nil && (c == connect.CodeInternal || c == connect.CodeUnavailable) && s.Log != nil {
				s.Log.Error("api: request failed", "operation", op, "error", err)
			}

			return resp, err
		}
	}
}

// code is err's Connect code as a label, ok for none.
func code(err error) string {
	if err == nil {
		return "ok"
	}

	return connect.CodeOf(err).String()
}
