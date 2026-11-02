package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// URLs carrying what no answer or log may: a password and a query, a token as user information, and a DSN.
const (
	passwordURL = "https://user:pw-sentinel@host.invalid/a/b?sig=q-sentinel#f-sentinel"
	tokenURL    = "https://tok-sentinel@host2.invalid:8443/p-sentinel"
	dsnURL      = "mysql://tk:dsn-sentinel@db.invalid:3306/tk-sentinel?tls=t-sentinel"
	// Bracketed IPv6 hosts, and a URL that does not parse.
	ipv6URL  = "https://[::1]:8443/p-sentinel?sig=q6-sentinel"
	ipv6DSN  = "mysql://tk:pw6-sentinel@[fd00::1]:3306/x-sentinel?tls=t6-sentinel"
	badURL   = "https://u:bad-sentinel@[unclosed/x?q=bad-sentinel"
	safeTail = "; retry mysql://[fd00::1]:3306, then https://[redacted]"
)

// AdminService never quotes the words it refuses: each usage error names the command and the argument's role, and none of the words,
// a bare token, a cookie, an authorization code, a password or a URL, reaches the answer or the log.
func TestAdminUsageQuotesNoWords(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	r := newRig(t, &Server{Catalog: c, Admin: &admin.Admin{Catalog: c}}, nil)
	cl := tracekeepv1connect.NewAdminServiceClient(r.hs.Client(), r.hs.URL)
	capture := tracepack.UUID{0x42}.String()
	answers := make([]string, 0, 10)
	for _, tc := range []struct {
		words []string
		named string
	}{
		{[]string{"capture", "show", "download-token-sentinel"}, "capture show: the capture id does not parse"},
		{[]string{"parked", "list", "session=cookie-sentinel"}, "parked list: the capture id does not parse"},
		{[]string{"parked", "terminate", capture, "code=authcode-sentinel"}, "parked terminate: the seq does not parse"},
		{[]string{"conflict", "acknowledge", "password=pw-sentinel", "1"}, "conflict acknowledge: the capture id does not parse"},
		{[]string{"scope", "merge", capture, "Bearer token-sentinel"}, "scope merge: the hour is not RFC 3339"},
		{[]string{"policy", "show", "p", "password=pw2-sentinel"}, "policy show: the version is not a positive integer"},
		{[]string{"config", "activate", "session=cookie2-sentinel"}, "config activate: the configuration id is not a positive integer"},
		{[]string{"bus", "adopt-stream", "token-sentinel"}, "bus adopt-stream: the role is not traffic or parking"},
		{[]string{"status", "session=cookie3-sentinel", passwordURL, ipv6URL}, "admin status: no such command"},
		{[]string{"password=pw3-sentinel", "code=authcode2-sentinel"}, "admin: unknown command"},
	} {
		_, err := cl.Admin(t.Context(), withToken(&tkv1.AdminRequest{Words: tc.words}, adminToken))
		<-r.done
		answers = append(answers, errMessage(err))
		if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(errMessage(err), tc.named) {
			t.Errorf("%q = %v, want invalid_argument naming %q", tc.words, err, tc.named)
		}
	}
	if text := strings.Join(answers, "\n") + r.logs.String(); strings.Contains(text, "sentinel") || strings.Contains(text, "host.invalid") {
		t.Errorf("an answer or the log quotes the operator's words:\n%s", text)
	}
}

// key verify of an id no binding knows answers that the id is not bound, never the id, whatever text it is.
func TestAdminKeyVerifyQuotesNoID(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	k, err := keys.New(keys.Config{}, c)
	if err != nil {
		t.Fatal(err)
	}
	r := newRig(t, &Server{Catalog: c, Admin: &admin.Admin{Catalog: c, Keys: k}}, nil)
	cl := tracekeepv1connect.NewAdminServiceClient(r.hs.Client(), r.hs.URL)
	answers := make([]string, 0, 3)
	for _, id := range []string{"session=cookie-sentinel", "password=pw-sentinel", "token-sentinel"} {
		_, err := cl.Admin(t.Context(), withToken(&tkv1.AdminRequest{Words: []string{"key", "verify", id}}, adminToken))
		<-r.done
		answers = append(answers, errMessage(err))
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(errMessage(err), "key id not bound") {
			t.Errorf("key verify %q = %v, want failed_precondition naming an unbound key id", id, err)
		}
	}
	if text := strings.Join(answers, "\n") + r.logs.String(); strings.Contains(text, "sentinel") {
		t.Errorf("an answer or the log quotes the key id asked for:\n%s", text)
	}
}

// AdminService's answers and log lines keep a command's diagnostic, every URL in it cut to its scheme and host:
// a command's failure and an audit failure whose catalog text names a DSN, bracketed IPv6 hosts included,
// and a URL that does not parse cut to its scheme.
func TestAdminAnswersNoCredential(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	s := &Server{Catalog: c, Admin: &admin.Admin{Catalog: c}}
	r := newRig(t, s, nil)
	cl := tracekeepv1connect.NewAdminServiceClient(r.hs.Client(), r.hs.URL)
	call := func(words ...string) error {
		_, err := cl.Admin(t.Context(), withToken(&tkv1.AdminRequest{Words: words}, adminToken))
		return err
	}
	answers := make([]string, 0, 2)
	fault := fmt.Errorf("catalog: dial %s: connection refused; retry %s, then %s", dsnURL, ipv6DSN, badURL)
	for _, tc := range []struct {
		name string
		err  error
		code connect.Code
		text string
	}{
		{"a command's catalog fault", fault, connect.CodeFailedPrecondition, "catalog: dial mysql://db.invalid:3306: connection refused" + safeTail},
		{"an audit failure", fmt.Errorf("%w: %w", catalog.ErrAudit, fault), connect.CodeInternal, msgAudit},
	} {
		s.runAdmin = func(context.Context, *admin.Admin, []string) (any, error) { return nil, tc.err }
		err := call("status")
		answers = append(answers, errMessage(err))
		if connect.CodeOf(err) != tc.code || errMessage(err) != tc.text {
			t.Errorf("%s = %v, want %v %q", tc.name, err, tc.code, tc.text)
		}
	}
	for range 2 {
		<-r.done
	}
	if text := strings.Join(answers, "\n") + r.logs.String(); strings.Contains(text, "sentinel") {
		t.Errorf("an answer or the log holds a credential, a path or a query:\n%s", text)
	}
	if !strings.Contains(r.logs.String(), "mysql://db.invalid:3306") || !strings.Contains(r.logs.String(), "mysql://[fd00::1]:3306") {
		t.Errorf("the log = %s\nwant the audit failure's cause, its URL cut to its host", r.logs)
	}
}

// Only a failure that is a context's passes as unavailable: with the request's context ended, a command's own refusal keeps failed_precondition,
// a catalog read the cancellation ended answers canceled, and with the context live, a dependency's own deadline answers unavailable.
func TestAdminRefusalBeforeCancellation(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	s := &Server{Catalog: c, Admin: &admin.Admin{Catalog: c}} // no keys: config stage refuses before any context's work
	principal := context.WithValue(t.Context(), principalKey{}, auth.Principal{Subject: "operator"})
	cancelled, cancel := context.WithCancel(principal)
	cancel()
	run := func(ctx context.Context, words ...string) error {
		_, err := adminService{s: s}.Admin(ctx, connect.NewRequest(&tkv1.AdminRequest{Words: words}))
		_, err = decide(ctx, err)

		return err
	}
	if err := run(cancelled, "config", "stage", "d-1", "m-1"); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("a refusal with the request cancelled = %v, want failed_precondition", err)
	}
	if err := run(cancelled, "parked", "list"); connect.CodeOf(err) != connect.CodeCanceled {
		t.Errorf("a catalog read the cancellation ended = %v, want canceled", err)
	}
	s.runAdmin = func(context.Context, *admin.Admin, []string) (any, error) {
		return nil, fmt.Errorf("catalog: %w", context.DeadlineExceeded)
	}
	if err := run(principal, "status"); connect.CodeOf(err) != connect.CodeUnavailable || errors.Is(err, context.Canceled) {
		t.Errorf("a dependency's deadline with the request live = %v, want unavailable", err)
	}
}
