package api

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/keys"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// config stage refuses a key id undeclared, or declared with another kind, naming neither id, whatever text it is.
func TestConfigStageQuotesNoKeyID(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	dir := t.TempDir()
	declared := map[string]keys.Declaration{}
	for id, kind := range map[string]string{"d-1": catalog.KindDigest, "m-1": catalog.KindMaster} {
		path := filepath.Join(dir, id)
		if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x5a}, 32))), 0o600); err != nil {
			t.Fatal(err)
		}
		declared[id] = keys.Declaration{Provider: keys.ProviderFile, Ref: path, Kind: kind}
	}
	k, err := keys.New(keys.Config{Declared: declared}, c)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ingest.Builtin(1<<20, true)
	if err != nil {
		t.Fatal(err)
	}
	a := &admin.Admin{Catalog: c, Keys: k, Classifier: b.Name, MaxFrameLen: b.MaxFrameLen, Dev: true}
	r := newRig(t, &Server{Catalog: c, Admin: a}, nil)
	cl := tracekeepv1connect.NewAdminServiceClient(r.hs.Client(), r.hs.URL)
	answers := make([]string, 0, 3)
	for _, words := range [][]string{
		{"config", "stage", "session=cookie-sentinel", "m-1"},
		{"config", "stage", "d-1", "password=pw-sentinel"},
		{"config", "stage", "m-1", "token-sentinel"},
	} {
		_, err := cl.Admin(t.Context(), withToken(&tkv1.AdminRequest{Words: words}, adminToken))
		<-r.done
		answers = append(answers, errMessage(err))
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(errMessage(err), "key id not accepted") {
			t.Errorf("%q = %v, want failed_precondition naming a key id not accepted", words, err)
		}
	}
	if text := strings.Join(answers, "\n") + r.logs.String(); strings.Contains(text, "sentinel") {
		t.Errorf("an answer or the log quotes a key id asked for:\n%s", text)
	}
}

// ListScopeChanges refuses too many devices naming the bound, never the request's tool.
func TestScopeChangesRefusalQuotesNoTool(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	r := newRig(t, &Server{Catalog: c}, nil)
	answers := make([]string, 0, 3)
	for _, tool := range []string{"password=pw-sentinel", "session=cookie-sentinel", "Bearer token-sentinel"} {
		s := sel(hour*nsPerHour, (hour+1)*nsPerHour)
		s.ToolId = tool
		s.Devices = make([]string, 1001)
		_, err := r.cl.ListScopeChanges(t.Context(), reader(&tkv1.ListScopeChangesRequest{Selector: s}))
		<-r.done
		answers = append(answers, errMessage(err))
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(errMessage(err), "at most") {
			t.Errorf("tool %q = %v, want invalid_argument naming the bound", tool, err)
		}
	}
	if text := strings.Join(answers, "\n") + r.logs.String(); strings.Contains(text, "sentinel") {
		t.Errorf("an answer or the log quotes the request's tool:\n%s", text)
	}
}
