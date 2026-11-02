package api

import (
	"context"
	"encoding/json"
	"errors"

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
	// The operator reads the operator's log, so an answer keeps a command's diagnostic, which the observer makes safe (safeAnswer);
	// an audit failure answers internal wherever the catalog wrote it, and only a failure that is a context's passes as unavailable,
	// which the observer decides as a client gone when the request's own context ended, so a command's refusal keeps its code
	// (api.md §10.1 *Admin over the API*, *A client gone*).
	switch {
	case errors.Is(err, admin.ErrUsage):
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, catalog.ErrAudit):
		return nil, dependency(connect.CodeInternal, msgAudit, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return nil, connect.NewError(connect.CodeUnavailable, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&tkv1.AdminResponse{Json: string(b)}), nil
}
