package api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// AdminService runs tracekeep admin's commands for the admin role alone, auditing them with the principal as actor.
func TestAdminOverTheAPI(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		s := &Server{Catalog: c, Auth: testAuthenticator(nil), Admin: &admin.Admin{Catalog: c, Actor: "ignored"}}
		srv := httptest.NewServer(s.Handler())
		t.Cleanup(srv.Close)
		cl := tracekeepv1connect.NewAdminServiceClient(srv.Client(), srv.URL)
		call := func(token string, words ...string) (*tkv1.AdminResponse, error) {
			resp, err := cl.Admin(t.Context(), withToken(&tkv1.AdminRequest{Words: words}, token))
			if err != nil {
				return nil, err
			}

			return resp.Msg, nil
		}
		if _, err := call(readerToken, "parked", "list"); connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("admin as a pack reader = %v, want permission denied", err)
		}
		if _, err := call("", "parked", "list"); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("admin without a credential = %v, want unauthenticated", err)
		}
		got, err := call(adminToken, "parked", "list")
		if err != nil || !json.Valid([]byte(got.GetJson())) {
			t.Fatalf("parked list = %v, %v; want its JSON", got, err)
		}
		capture := tracepack.UUID{0x42}
		if _, err := call(adminToken, "capture", "close", capture.String()); err != nil {
			t.Fatalf("capture close = %v", err)
		}
		audit, err := c.AuditEntries(t.Context(), capture)
		if err != nil || len(audit) != 1 || audit[0].Actor != "operator" {
			t.Errorf("audit = %+v, %v; want one row with the principal as actor", audit, err)
		}
		if _, err := call(adminToken, "no", "such", "command"); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("an unknown command = %v, want invalid argument", err)
		}
	})
}
