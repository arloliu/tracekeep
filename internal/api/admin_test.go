package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/keys"
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
		if rows, err := c.AuditByActor(t.Context(), "operator"); err != nil || len(rows) != 1 || rows[0].Action != "parked list" {
			t.Errorf("audit of parked list = %+v, %v; want a row with the principal as actor", rows, err)
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

// key verify through AdminService: the same answer, audited with the principal as actor.
func TestKeyVerifyOverTheAPI(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	path := filepath.Join(t.TempDir(), "d-1")
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x83}, 32))), 0o600); err != nil {
		t.Fatal(err)
	}
	k, err := keys.New(keys.Config{Declared: map[string]keys.Declaration{"d-1": {Provider: keys.ProviderFile, Ref: path, Kind: catalog.KindDigest}}}, c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.Digest(t.Context(), "d-1"); err != nil {
		t.Fatal(err)
	}
	s := &Server{Catalog: c, Auth: testAuthenticator(nil), Admin: &admin.Admin{Catalog: c, Keys: k}}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	cl := tracekeepv1connect.NewAdminServiceClient(srv.Client(), srv.URL)
	resp, err := cl.Admin(t.Context(), withToken(&tkv1.AdminRequest{Words: []string{"key", "verify", "d-1"}}, adminToken))
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal([]byte(resp.Msg.GetJson()), &v); err != nil || v["Match"] != any(true) || v["KeyID"] != "d-1" {
		t.Errorf("key verify = %s, %v; want a match", resp.Msg.GetJson(), err)
	}
	if rows, err := c.AuditByActor(t.Context(), "operator"); err != nil || len(rows) != 1 || rows[0].Action != "key verify" {
		t.Errorf("audit = %+v, %v; want key verify's row with the principal as actor", rows, err)
	}
}

// The policy and config commands answer the same through AdminService as directly, refusals included.
func TestConfigCommandsOverTheAPI(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	path := filepath.Join(t.TempDir(), "d-1")
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x84}, 32))), 0o600); err != nil {
		t.Fatal(err)
	}
	k, err := keys.New(keys.Config{Declared: map[string]keys.Declaration{
		"d-1": {Provider: keys.ProviderFile, Ref: path, Kind: catalog.KindDigest}, "m-1": {Provider: keys.ProviderFile, Ref: path, Kind: catalog.KindMaster},
	}}, c)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ingest.Builtin(0, true)
	if err != nil {
		t.Fatal(err)
	}
	a := &admin.Admin{Catalog: c, Keys: k, Classifier: b.Name, MaxFrameLen: b.MaxFrameLen}
	s := &Server{Catalog: c, Auth: testAuthenticator(nil), Admin: a}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	cl := tracekeepv1connect.NewAdminServiceClient(srv.Client(), srv.URL)
	remote := func(words ...string) (string, error) {
		resp, err := cl.Admin(t.Context(), withToken(&tkv1.AdminRequest{Words: words}, adminToken))
		if err != nil {
			return "", err
		}

		return resp.Msg.GetJson(), nil
	}
	direct := func(words ...string) string {
		out, err := admin.Run(t.Context(), a, words)
		if err != nil {
			t.Fatalf("%q directly = %v", words, err)
		}
		j, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			t.Fatal(err)
		}

		return string(j)
	}
	staged, err := remote("config", "stage", "d-1", "m-1")
	if err != nil || !strings.Contains(staged, `"ConfigID": 1`) {
		t.Fatalf("config stage over the API = %s, %v", staged, err)
	}
	for _, words := range [][]string{{"config", "list"}, {"policy", "list"}, {"policy", "show", "baseline", "1"}} {
		if got, err := remote(words...); err != nil || got != direct(words...) {
			t.Errorf("%q over the API = %s, %v; want the direct answer", words, got, err)
		}
	}
	if _, err := remote("config", "activate", "1"); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "does not mask") {
		t.Errorf("activating the masking configuration over the API = %v, want refused", err)
	}
	if _, err := remote("config", "stage"); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("config stage without a key id = %v, want invalid argument", err)
	}
}
