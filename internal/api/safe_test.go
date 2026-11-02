package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/catalog"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// MachineService answers and logs a dependency's cause and a refused input without a credential, a path or a query:
// a catalog's and an audit's cause holding DSNs, bracketed IPv6 host included, is logged cut to scheme and host,
// and a malformed capture_id, in Restore, in FindTransaction or in a selector, is named by its field and never echoed.
func TestMachineServiceAnswersAndLogsSafe(t *testing.T) {
	t.Parallel()
	fault := fmt.Errorf("catalog: dial %s: connection refused; retry %s", dsnURL, ipv6DSN)
	now := time.Now()
	deadlineNs := now.Add(24 * time.Hour).UnixNano()
	stub := &stubCatalog{download: catalog.Download{Known: true, Deadline: deadlineNs, Key: "staging/k"}}
	for _, tc := range []struct {
		name   string
		cat    Catalog
		call   func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error
		code   connect.Code
		logged string
	}{
		{"a catalog failure", failing(stub, "ListCaptures", fault), requests[1].call, connect.CodeUnavailable, "mysql://[fd00::1]:3306"},
		{"an audit failure", failing(stub, "AuditWithin", fmt.Errorf("%w: %w", catalog.ErrAudit, fault)), requests[5].call, connect.CodeInternal, "mysql://[fd00::1]:3306"},
		{"a malformed capture_id in Restore", stub, func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
			_, err := cl.Restore(ctx, withToken(&tkv1.RestoreRequest{CaptureId: passwordURL, Seq: 1, Hour: hourTS(hour)}, restorerToken))
			return err
		}, connect.CodeInvalidArgument, "capture_id"},
		{"a malformed capture_id in FindTransaction", stub, func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
			_, err := cl.FindTransaction(ctx, withToken(&tkv1.FindTransactionRequest{CaptureId: passwordURL, Seq: 1, Hour: hourTS(hour)}, viewerToken))
			return err
		}, connect.CodeInvalidArgument, ""},
		{"a malformed capture_id in a selector", stub, func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
			s := sel(hour*nsPerHour, (hour+1)*nsPerHour)
			id := passwordURL
			s.CaptureId = &id
			_, err := cl.ListCaptures(ctx, reader(&tkv1.ListCapturesRequest{Selector: s}))
			return err
		}, connect.CodeInvalidArgument, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &Server{Catalog: tc.cat, Downloads: Downloads{
				Presigner: &presigner{}, Lifetime: 15 * time.Minute, Margin: 2 * time.Minute, Floor: time.Minute, Now: func() time.Time { return now },
			}}
			r := newRig(t, s, nil)
			err := tc.call(t.Context(), r.cl)
			<-r.done
			if connect.CodeOf(err) != tc.code {
				t.Errorf("%s = %v, want %v", tc.name, err, tc.code)
			}
			if text := errMessage(err) + "\n" + r.logs.String(); strings.Contains(text, "sentinel") || strings.Contains(text, "host.invalid") {
				t.Errorf("the answer or the log holds a credential, a path, a query or the rejected text:\n%s", text)
			}
			if tc.code == connect.CodeInvalidArgument && !strings.Contains(errMessage(err), "capture_id") {
				t.Errorf("%s = %q, want the field named", tc.name, errMessage(err))
			}
			if !strings.Contains(r.logs.String(), tc.logged) {
				t.Errorf("the log = %s\nwant %q", r.logs, tc.logged)
			}
		})
	}
}
