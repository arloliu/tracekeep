package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
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

// AdminService's answers and log lines keep a command's diagnostic, every URL in it cut to its scheme and host:
// a usage error echoing its words, a command's failure and an audit failure whose catalog text names a DSN, bracketed IPv6 hosts included,
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
	var answers []string
	err := call("no", "such", passwordURL, tokenURL, ipv6URL)
	answers = append(answers, errMessage(err))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(errMessage(err), "usage") ||
		!strings.Contains(errMessage(err), "https://host.invalid ") || !strings.Contains(errMessage(err), "https://host2.invalid:8443") ||
		!strings.Contains(errMessage(err), "https://[::1]:8443]") {
		t.Errorf("a usage error = %v, want invalid_argument naming the usage and each URL's scheme and host", err)
	}
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
	for range 3 {
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
