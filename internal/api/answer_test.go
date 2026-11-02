package api

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// Every MachineService operation whose catalog fails answers unavailable with the catalog's fixed message:
// the driver's text, its host included, reaches the log once and never the answer.
func TestCatalogFailureAnswersNoCause(t *testing.T) {
	t.Parallel()
	r := newRig(t, &Server{Catalog: failing(&stubCatalog{}, "*", errors.New("catalog: "+catalogSentinel))}, nil)
	ctx, cl := t.Context(), r.cl
	window := sel(hour*nsPerHour, (hour+1)*nsPerHour)
	job := tracepack.UUID{7}.String()
	calls := map[string]func() error{
		"ListDevices": func() error {
			_, err := cl.ListDevices(ctx, reader(&tkv1.ListDevicesRequest{Selector: window}))
			return err
		},
		"ListPacks": func() error {
			_, err := cl.ListPacks(ctx, reader(&tkv1.ListPacksRequest{Selector: window}))
			return err
		},
		"ListScopeChanges": func() error {
			_, err := cl.ListScopeChanges(ctx, reader(&tkv1.ListScopeChangesRequest{Selector: window}))
			return err
		},
		"StartExport": func() error {
			_, err := cl.StartExport(ctx, withToken(&tkv1.StartExportRequest{Kind: "canonical", Selector: window}, exporterToken))
			return err
		},
		"GetExport": func() error {
			_, err := cl.GetExport(ctx, withToken(&tkv1.GetExportRequest{JobId: job}, exporterToken))
			return err
		},
		"ListExports": func() error {
			_, err := cl.ListExports(ctx, withToken(&tkv1.ListExportsRequest{}, exporterToken))
			return err
		},
		"CancelExport": func() error {
			_, err := cl.CancelExport(ctx, withToken(&tkv1.CancelExportRequest{JobId: job}, exporterToken))
			return err
		},
	}
	for _, rq := range requests {
		calls[rq.op] = func() error { return rq.call(ctx, cl) }
	}
	for op, call := range calls {
		err := call()
		<-r.done
		if connect.CodeOf(err) != connect.CodeUnavailable || errMessage(err) != msgCatalog || strings.Contains(err.Error(), "sentinel") {
			t.Errorf("%s over a failing catalog = %v, want unavailable with the catalog's fixed message", op, err)
		}
	}
	if n := strings.Count(r.logs.String(), "api: request failed"); n != len(calls) || strings.Count(r.logs.String(), "db-sentinel.invalid") != len(calls) {
		t.Errorf("the log = %s\nwant one failure naming its cause per request", r.logs)
	}
}

// An audit row that cannot be written answers internal with the audit's fixed message, its cause logged and never answered:
// for PackURL, an export's URL, and the export transactions, which roll back.
func TestAuditFailureAnswersNoCause(t *testing.T) {
	t.Parallel()
	now := time.Now()
	key, deadlineNs := "exports/j/1.tar", now.Add(24*time.Hour).UnixNano()
	stub := &stubCatalog{
		download: catalog.Download{Known: true, Deadline: deadlineNs, Key: key},
		job:      catalog.ExportJob{ID: tracepack.UUID{7}, State: catalog.ExportDone, ObjectKey: &key, DeleteAt: &deadlineNs},
	}
	auditErr := fmt.Errorf("%w: %s", catalog.ErrAudit, auditSentinel)
	window := sel(hour*nsPerHour, (hour+1)*nsPerHour)
	for op, tc := range map[string]struct {
		held string
		call func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error
	}{
		"PackURL": {"AuditWithin", requests[5].call},
		"GetExport": {"AuditWithin", func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
			_, err := cl.GetExport(ctx, withToken(&tkv1.GetExportRequest{JobId: stub.job.ID.String()}, exporterToken))
			return err
		}},
		"StartExport": {"CreateExport", func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
			_, err := cl.StartExport(ctx, withToken(&tkv1.StartExportRequest{Kind: "canonical", Selector: window}, exporterToken))
			return err
		}},
		"CancelExport": {"CancelExport", func(ctx context.Context, cl tracekeepv1connect.MachineServiceClient) error {
			_, err := cl.CancelExport(ctx, withToken(&tkv1.CancelExportRequest{JobId: stub.job.ID.String()}, exporterToken))
			return err
		}},
	} {
		s := &Server{Catalog: failing(stub, tc.held, auditErr), Downloads: Downloads{
			Presigner: &presigner{}, Lifetime: 15 * time.Minute, Margin: 2 * time.Minute, Floor: time.Minute, Now: func() time.Time { return now },
		}}
		r := newRig(t, s, nil)
		err := tc.call(t.Context(), r.cl)
		<-r.done
		if connect.CodeOf(err) != connect.CodeInternal || errMessage(err) != msgAudit || strings.Contains(err.Error(), "sentinel") {
			t.Errorf("%s with a failing audit = %v, want internal with the audit's fixed message", op, err)
		}
		if !strings.Contains(r.logs.String(), "audit-sentinel") || strings.Count(r.logs.String(), "\n") != 1 {
			t.Errorf("%s: the log = %q, want one line naming the audit's cause", op, r.logs)
		}
	}
}

// heldSigner is a presigner and a download token's keys, each failing with an object key and a signature in its error:
// with release set, it first sends the request's context on arrived and waits for release, then wraps the context's error;
// without, it fails at once with err.
type heldSigner struct {
	arrived chan context.Context
	release chan struct{}
	err     error
}

func (h *heldSigner) fail(ctx context.Context, key string) error {
	if h.release == nil {
		return h.err
	}
	h.arrived <- ctx
	<-h.release

	return fmt.Errorf("presign https://store.invalid/%s?X-Amz-Signature=sig-sentinel: %w", key, ctx.Err())
}

func (h *heldSigner) PresignGet(ctx context.Context, key string, _ time.Time) (string, time.Time, error) {
	return "", time.Time{}, h.fail(ctx, key)
}

func (h *heldSigner) Issuing(ctx context.Context, _ string) (string, []byte, error) {
	return "", nil, h.fail(ctx, "a download key")
}

func (h *heldSigner) Accepted(context.Context, string, string) ([]byte, error) {
	return nil, errors.New("unused")
}

// A download URL that cannot be made, presigned or carrying a download token, for PackURL and a done export alike, is the store's failure:
// held while its request is cancelled or passes its deadline it answers by that context, and failing by its own deadline with the request live,
// unavailable with the store's fixed message; neither the key nor the signature reaches the answer or the log.
func TestURLFailures(t *testing.T) {
	t.Parallel()
	for _, op := range []string{auth.OpPackURL, auth.OpGetExport} {
		for _, presigned := range []bool{true, false} {
			for mode, code := range map[string]string{"cancelled": "canceled", "deadline": "deadline_exceeded", "live": "unavailable"} {
				t.Run(fmt.Sprintf("%s/presigned=%v/%s", op, presigned, mode), func(t *testing.T) {
					t.Parallel()
					now := time.Now()
					key, deadlineNs := "exports/j/1.tar", now.Add(24*time.Hour).UnixNano()
					h := &heldSigner{err: fmt.Errorf("presign https://store.invalid/%s?X-Amz-Signature=sig-sentinel: %w", key, context.DeadlineExceeded)}
					if mode != "live" {
						h.arrived, h.release = make(chan context.Context, 1), make(chan struct{})
					}
					dl := Downloads{Lifetime: 15 * time.Minute, Margin: 2 * time.Minute, Floor: time.Minute, PublicURL: publicURL, Now: func() time.Time { return now }}
					if presigned {
						dl.Presigner = h
					} else {
						dl.Tokens = &auth.Downloads{Keys: h, Now: dl.Now}
					}
					s := &Server{Downloads: dl, Catalog: &stubCatalog{
						download: catalog.Download{Known: true, Deadline: deadlineNs, Key: key},
						job:      catalog.ExportJob{ID: tracepack.UUID{7}, State: catalog.ExportDone, ObjectKey: &key, DeleteAt: &deadlineNs},
					}}
					if mode == "deadline" {
						s.outer = []connect.Interceptor{deadlined(300 * time.Millisecond)}
					}
					r := newRig(t, s, nil)
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					ended := make(chan error, 1)
					go func() {
						if op == auth.OpPackURL {
							ended <- requests[5].call(ctx, r.cl)
						} else {
							_, err := r.cl.GetExport(ctx, withToken(&tkv1.GetExportRequest{JobId: tracepack.UUID{7}.String()}, exporterToken))
							ended <- err
						}
					}()
					if mode != "live" {
						srv := <-h.arrived
						if mode == "cancelled" {
							cancel()
						}
						<-srv.Done()
						close(h.release)
					}
					<-r.done
					err := <-ended
					r.counted(t, fmt.Sprintf(`tracekeep_api_requests_total{code=%q,operation=%q} 1`, code, op))
					if mode == "live" && (connect.CodeOf(err) != connect.CodeUnavailable || errMessage(err) != msgStore) {
						t.Errorf("a URL failing with the request live = %v, want unavailable with the store's fixed message", err)
					}
					if text := fmt.Sprint(err) + r.logs.String(); strings.Contains(text, key) || strings.Contains(text, "sentinel") {
						t.Errorf("the answer or the log names the key or the signature: %v\n%s", err, r.logs)
					}
					if logged := r.logs.Len() != 0; logged != (mode == "live") {
						t.Errorf("the log = %q, want a line only for a failure with the request live", r.logs)
					}
				})
			}
		}
	}
}

// FindTransaction answers not_found for a capture the catalog does not know, and data_loss, logged as an error, for a pack that does not open
// as one of the capture, however often asked; a pack missing or one the store fails to read, its deadline included, stays unavailable,
// with the store's fixed message and neither the key nor the store's text anywhere.
func TestFindTransactionPacksThatDoNotOpen(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		capture, other := m.admitUnmasked(t), m.admitActive(t)
		rs := []tracepack.Record{message(1, hour*nsPerHour+1, 1, true, true, 7)}
		p, q := m.put(t, capture, hour, rs, nil, nil), m.put(t, other, hour, rs, nil, nil)
		pack, others := objectBytes(t, m.st, p.Key), objectBytes(t, m.st, q.Key)
		if _, err := find(m, t, capture, 1, viewerToken); err != nil {
			t.Fatalf("the control = %v", err)
		}
		if _, err := find(m, t, newID(t), 1, viewerToken); connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("an unknown capture = %v, want not_found", err)
		}
		unsupported := bytes.Clone(pack)
		binary.LittleEndian.PutUint16(unsupported[8:], 2)
		binary.LittleEndian.PutUint32(unsupported[76:], crc32.ChecksumIEEE(unsupported[:76]))
		damaged := map[string][]byte{"a corrupt header": flipped(pack, 20), "not a pack": flipped(pack, 0), "an unsupported format": unsupported, "another capture's": others}
		m.logs.Reset()
		for name, b := range damaged {
			putObject(t, m.st, p.Key, b)
			for range 2 {
				if _, err := find(m, t, capture, 1, viewerToken); connect.CodeOf(err) != connect.CodeDataLoss {
					t.Errorf("%s = %v, want data_loss", name, err)
				}
			}
		}
		if n := strings.Count(m.logs.String(), "level=ERROR msg=\"api: request failed\""); n != 2*len(damaged) {
			t.Errorf("%d error lines for the packs that do not open, want %d:\n%s", n, 2*len(damaged), m.logs)
		}
		putObject(t, m.st, p.Key, pack)
		m.logs.Reset()
		for name, st := range map[string]store.ObjectStore{
			"a store failing its open":             &failingStore{ObjectStore: m.st, open: true, err: errors.New("get " + p.Key + ": backend-sentinel")},
			"a store's read past its own deadline": &failingStore{ObjectStore: m.st, err: fmt.Errorf("read %s: %w", p.Key, context.DeadlineExceeded)},
			"a store's read failing with its text": &failingStore{ObjectStore: m.st, err: errors.New("read " + p.Key + ": backend-sentinel")},
		} {
			m.srv.Store = st
			_, err := find(m, t, capture, 1, viewerToken)
			if connect.CodeOf(err) != connect.CodeUnavailable || errMessage(err) != msgStore {
				t.Errorf("%s = %v, want unavailable with the store's fixed message", name, err)
			}
		}
		m.srv.Store = m.st
		if err := m.st.Delete(t.Context(), p.Key); err != nil {
			t.Fatal(err)
		}
		if _, err := find(m, t, capture, 1, viewerToken); connect.CodeOf(err) != connect.CodeUnavailable || errMessage(err) != msgStore {
			t.Errorf("a missing pack = %v, want unavailable with the store's fixed message", err)
		}
		if strings.Contains(m.logs.String(), p.Key) || strings.Contains(m.logs.String(), "sentinel") || strings.Count(m.logs.String(), "\n") != 4 {
			t.Errorf("the log = %s\nwant one keyless line per failure", m.logs)
		}
	})
}

// objectBytes is key's object in st.
func objectBytes(t *testing.T, st *store.Mem, key string) []byte {
	t.Helper()
	obj, err := st.Open(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	b := make([]byte, obj.Size())
	if _, err := obj.ReadAt(b, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}

	return b
}

func putObject(t *testing.T, st *store.Mem, key string, b []byte) {
	t.Helper()
	if err := st.Put(t.Context(), key, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatal(err)
	}
}

// flipped is b with its byte at i flipped.
func flipped(b []byte, i int) []byte {
	out := bytes.Clone(b)
	out[i] ^= 0xff

	return out
}

// failingStore fails each open, when open is set, or each object's reads with err.
type failingStore struct {
	store.ObjectStore
	open bool
	err  error
}

func (f *failingStore) Open(ctx context.Context, key string) (store.Object, error) {
	if f.open {
		return nil, f.err
	}
	obj, err := f.ObjectStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}

	return failingObject{obj, f.err}, nil
}

type failingObject struct {
	store.Object
	err error
}

func (o failingObject) ReadAt([]byte, int64) (int, error) { return 0, o.err }

// AdminService answers its operator with a command's text, a catalog's cause included, never the token the request carried:
// a refusal and a read whose catalog fails keep their text as failed_precondition, a cancellation passes as the request's own code,
// and an audit row that cannot be written, under a read or inside a mutating command's transaction, answers internal with the audit's fixed message.
func TestAdminOverTheAPIFailures(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		s := &Server{Catalog: c, Admin: &admin.Admin{Catalog: c}}
		r := newRig(t, s, nil)
		cl := tracekeepv1connect.NewAdminServiceClient(r.hs.Client(), r.hs.URL)
		call := func(words ...string) error {
			_, err := cl.Admin(t.Context(), withToken(&tkv1.AdminRequest{Words: words}, adminToken))
			return err
		}
		unknown := tracepack.UUID{0x42}.String()
		if err := call("capture", "show", unknown); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(errMessage(err), "unknown capture") {
			t.Errorf("capture show of an unknown capture = %v, want failed_precondition naming it", err)
		}
		ctx, cancel := context.WithCancel(context.WithValue(t.Context(), principalKey{}, auth.Principal{Subject: "operator"}))
		cancel()
		_, err := adminService{s: s}.Admin(ctx, connect.NewRequest(&tkv1.AdminRequest{Words: []string{"parked", "list"}}))
		if gone, err := decide(ctx, err); connect.CodeOf(err) != connect.CodeCanceled || !gone {
			t.Errorf("a cancelled admin read = %v, gone %v; want canceled", err, gone)
		}
		catalogtest.ExecSQL(t, c, `ALTER TABLE parked RENAME TO parked_gone`)
		if err := call("parked", "list"); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(errMessage(err), "parked") {
			t.Errorf("parked list without its table = %v, want failed_precondition with the catalog's text", err)
		}
		catalogtest.ExecSQL(t, c, `ALTER TABLE parked_gone RENAME TO parked`)
		catalogtest.ExecSQL(t, c, `ALTER TABLE audit_log RENAME TO audit_log_gone`)
		for _, words := range [][]string{{"parked", "list"}, {"capture", "close", unknown}} {
			if err := call(words...); connect.CodeOf(err) != connect.CodeInternal || errMessage(err) != msgAudit {
				t.Errorf("%v with a failing audit = %v, want internal with the audit's fixed message", words, err)
			}
		}
		if strings.Count(r.logs.String(), "level=ERROR") != 2 || !strings.Contains(r.logs.String(), "audit_log") {
			t.Errorf("the log = %s\nwant the two audit failures with their cause", r.logs)
		}
		if strings.Contains(r.logs.String(), adminToken) {
			t.Error("the log holds the request's token")
		}
	})
}
