package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/catalog"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// adminService serves AdminService (api.md §10.1 *Admin over the API*): the same commands tracekeep admin runs,
// run here with the principal as the actor every audited command names.
type adminService struct {
	tracekeepv1connect.UnimplementedAdminServiceHandler
	s *Server
}

// Admin implements tracekeepv1connect.AdminServiceHandler.
func (a adminService) Admin(ctx context.Context, req *connect.Request[tkv1.AdminRequest]) (*connect.Response[tkv1.AdminResponse], error) {
	if a.s.Admin == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this process runs no admin commands"))
	}
	p, _ := PrincipalFrom(ctx)
	op := *a.s.Admin
	op.Actor = p.Subject
	run := admin.Run
	if a.s.runAdmin != nil {
		run = a.s.runAdmin
	}
	out, err := run(ctx, &op, req.Msg.GetWords())
	// The operator reads the operator's log, so an answer keeps a command's diagnostic, every URL in it cut to its scheme and host;
	// an audit failure answers internal wherever the catalog wrote it, and only a failure that is a context's passes as unavailable,
	// which the observer decides as a client gone when the request's own context ended, so a command's refusal keeps its code
	// (api.md §10.1 *Admin over the API*, *A client gone*).
	switch {
	case errors.Is(err, admin.ErrUsage):
		return nil, connect.NewError(connect.CodeInvalidArgument, operatorSafe(err))
	case errors.Is(err, catalog.ErrAudit):
		return nil, dependency(connect.CodeInternal, msgAudit, operatorSafe(err))
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return nil, connect.NewError(connect.CodeUnavailable, operatorSafe(err))
	case err != nil:
		return nil, connect.NewError(connect.CodeFailedPrecondition, operatorSafe(err))
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, operatorSafe(err))
	}

	return connect.NewResponse(&tkv1.AdminResponse{Json: string(b)}), nil
}

// urlPattern finds a URL in a diagnostic's text.
var urlPattern = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"'<>\[\]]+`)

// operatorSafe is err's text as the operator may read it (operations.md §10.4 *Logs*): every URL cut to its scheme and host,
// without user information, path, query or fragment, and the rest of the diagnostic kept.
func operatorSafe(err error) error {
	return errors.New(urlPattern.ReplaceAllStringFunc(err.Error(), func(s string) string {
		// Punctuation the diagnostic put after the URL stays after it.
		trimmed := strings.TrimRight(s, ".,;:)")
		tail := s[len(trimmed):]
		scheme, _, _ := strings.Cut(trimmed, "://")
		u, perr := url.Parse(trimmed)
		if perr != nil {
			return scheme + "://" + tail
		}

		return u.Scheme + "://" + u.Host + tail
	}))
}
