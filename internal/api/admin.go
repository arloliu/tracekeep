package api

import (
	"context"
	"encoding/json"
	"errors"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/admin"
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
	out, err := admin.Run(ctx, &op, req.Msg.GetWords())
	switch {
	case errors.Is(err, admin.ErrUsage):
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&tkv1.AdminResponse{Json: string(b)}), nil
}
