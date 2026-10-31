package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/metrics"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

const publicURL = "https://tracekeep.example.invalid:8443"

// downloadKeys is a download purpose of key id dl-1, a 0600 file under t's directory, bound in c.
func downloadKeys(t *testing.T, c *catalog.Catalog) *keys.Keys {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dl-1")
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))), 0o600); err != nil {
		t.Fatal(err)
	}
	k, err := keys.New(keys.Config{
		Declared: map[string]keys.Declaration{"dl-1": {Provider: keys.ProviderFile, Ref: path}},
		Download: keys.Purpose{Issuing: "dl-1", Accepted: []string{"dl-1"}},
	}, c)
	if err != nil {
		t.Fatal(err)
	}

	return k
}

// presigner records the untils it presigned for, and reports each URL surely read until 1.5 s before its until, as a store signing in whole seconds would.
type presigner struct {
	mu     sync.Mutex
	untils []time.Time
}

func (p *presigner) PresignGet(_ context.Context, key string, until time.Time) (string, time.Time, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.untils = append(p.untils, until)

	return "https://store.example.invalid/" + key + "?X-Amz-Signature=synthetic", until.Add(-1500 * time.Millisecond), nil
}

// retention is the period the tests record: hour's deadline is its end plus it.
const retention = 48 * time.Hour

// deadlineOf is the deadline of a pack of hour under retention.
func deadlineOf() time.Time { return time.Unix(0, (hour+1)*nsPerHour).Add(retention) }

// urls readies w's PackURL: API downloads under w's keys, the clock at now, retention recorded unless before is set.
func (w *world) urls(t *testing.T, now *time.Time, record bool) {
	t.Helper()
	w.srv.Downloads = Downloads{
		Tokens: &auth.Downloads{Keys: downloadKeys(t, w.c), Now: func() time.Time { return *now }}, PublicURL: publicURL,
		Lifetime: 15 * time.Minute, Margin: 2 * time.Minute, Floor: time.Minute, DeleteDelay: time.Hour, Now: func() time.Time { return *now },
	}
	if !record {
		return
	}
	l, ok, err := w.c.TakeLease(t.Context(), "compactor/2", "test", time.Hour)
	if err != nil || !ok {
		t.Fatalf("TakeLease = %v, %v", ok, err)
	}
	w.lease = l
	if _, err := w.c.RecordRetention(t.Context(), l, retention, time.Hour, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
}

func packURL(t *testing.T, w *world, pack string) (*tkv1.PackURLResponse, error) {
	t.Helper()
	resp, err := w.cl.PackURL(t.Context(), reader(&tkv1.PackURLRequest{PackId: pack}))
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

// A URL on the API carries a token for the pack's object alone, expiring at the earlier of now plus the lifetime and the deadline less the margin;
// each issued URL is audited without the URL.
func TestPackURLOnTheAPI(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		w := newWorld(t, c)
		s := metrics.NewServer()
		m, err := metrics.NewAPI(s)
		if err != nil {
			t.Fatal(err)
		}
		w.srv.Metrics = m
		now := deadlineOf().Add(-24 * time.Hour)
		w.urls(t, &now, true)
		capture := w.admit(t)
		p := w.stage(t, capture)
		got, err := packURL(t, w, p.ID.String())
		if err != nil {
			t.Fatal(err)
		}
		if want := now.Add(15 * time.Minute).Truncate(time.Second); !got.Expires.AsTime().Equal(want) || got.Presigned {
			t.Errorf("PackURL expires %s, presigned %v; want %s on the API", got.Expires.AsTime(), got.Presigned, want)
		}
		u, err := url.Parse(got.Url)
		if err != nil || !strings.HasPrefix(got.Url, publicURL+"/download/") {
			t.Fatalf("PackURL = %q, %v; want under %s/download/", got.Url, err, publicURL)
		}
		token := strings.TrimPrefix(u.Path, "/download/")
		if err := w.srv.Downloads.Tokens.Verify(t.Context(), token, p.Key); err != nil {
			t.Errorf("the URL's token for the pack's object = %v", err)
		}
		if err := w.srv.Downloads.Tokens.Verify(t.Context(), token, p.Key+"x"); err == nil {
			t.Error("the URL's token admits another object")
		}
		audit, err := c.AuditEntries(t.Context(), capture)
		if err != nil || len(audit) != 1 || audit[0].Actor != "reader" || audit[0].Action != "pack-url" ||
			!strings.Contains(audit[0].Detail, p.ID.String()) || strings.Contains(audit[0].Detail, token) {
			t.Errorf("audit = %+v, %v; want one pack-url row by the reader, naming the pack and not the token", audit, err)
		}
		// Near the deadline the margin clamps the expiry.
		now = deadlineOf().Add(-10 * time.Minute)
		if got, err := packURL(t, w, p.ID.String()); err != nil || !got.Expires.AsTime().Equal(deadlineOf().Add(-2*time.Minute)) {
			t.Errorf("PackURL 10 minutes before the deadline = %v, %v; want it expiring 2 minutes before", got, err)
		}
		if b := scrape(t, s); !strings.Contains(b, `tracekeep_api_download_urls_total{kind="api"} 2`) {
			t.Errorf("download_urls_total not counted:\n%s", b)
		}
	})
}

// Less than the floor before the deadline less the margin, a URL is refused as pack-superseded with its detail.
func TestPackURLSuperseded(t *testing.T) {
	t.Parallel()
	w := newWorld(t, catalogtest.Open(t, catalog.EngineSQLite))
	s := metrics.NewServer()
	m, err := metrics.NewAPI(s)
	if err != nil {
		t.Fatal(err)
	}
	w.srv.Metrics = m
	now := deadlineOf().Add(-2*time.Minute - 59*time.Second)
	w.urls(t, &now, true)
	p := w.stage(t, w.admit(t))
	_, err = packURL(t, w, p.ID.String())
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Code() != connect.CodeFailedPrecondition || len(ce.Details()) != 1 {
		t.Fatalf("PackURL 59 s inside the floor = %v, want pack-superseded with a detail", err)
	}
	if audit, _ := w.c.AuditEntries(t.Context(), p.Capture); len(audit) != 0 {
		t.Errorf("a URL refused before signing was audited: %+v", audit)
	}
	v, derr := ce.Details()[0].Value()
	if d, ok := v.(*tkv1.PackSuperseded); derr != nil || !ok || d.PackId != p.ID.String() {
		t.Errorf("detail = %v, %v; want PackSuperseded naming %s", v, derr, p.ID)
	}
	now = deadlineOf().Add(-3*time.Minute - time.Second)
	if _, err := packURL(t, w, p.ID.String()); err != nil {
		t.Errorf("PackURL a second outside the floor = %v", err)
	}
	if b := scrape(t, s); !strings.Contains(b, "tracekeep_api_download_urls_refused_total 1") {
		t.Errorf("download_urls_refused_total not counted:\n%s", b)
	}
}

func TestPackURLRefusals(t *testing.T) {
	t.Parallel()
	w := newWorld(t, catalogtest.Open(t, catalog.EngineSQLite))
	now := time.Now()
	w.urls(t, &now, false)
	p := w.stage(t, w.admit(t))
	for name, tc := range map[string]struct {
		pack string
		want connect.Code
	}{
		"before a retention record": {p.ID.String(), connect.CodeUnavailable},
		"an unknown pack":           {newID(t).String(), connect.CodeNotFound},
		"a bad pack id":             {"pack", connect.CodeInvalidArgument},
	} {
		if _, err := packURL(t, w, tc.pack); connect.CodeOf(err) != tc.want {
			t.Errorf("%s: PackURL = %v, want %v", name, err, tc.want)
		}
	}
}

// Where the store presigns and presigning is on, the URL is presigned never to outlive its expiry, which the store reports.
func TestPackURLPresigned(t *testing.T) {
	t.Parallel()
	w := newWorld(t, catalogtest.Open(t, catalog.EngineSQLite))
	now := deadlineOf().Add(-24*time.Hour + 300*time.Millisecond)
	w.urls(t, &now, true)
	ps := &presigner{}
	w.srv.Downloads.Presigner = ps
	p := w.stage(t, w.admit(t))
	got, err := packURL(t, w, p.ID.String())
	if err != nil || !got.Presigned || !strings.Contains(got.Url, p.Key) {
		t.Fatalf("PackURL = %v, %v; want a presigned URL of the pack's key", got, err)
	}
	// Presigned until the whole second within the lifetime, and reported with the expiry the store vouches for.
	if want := now.Add(15 * time.Minute).Truncate(time.Second); len(ps.untils) != 1 || !ps.untils[0].Equal(want) {
		t.Errorf("presigned until %v, want %s", ps.untils, want)
	}
	if len(ps.untils) == 1 && !got.Expires.AsTime().Equal(ps.untils[0].Add(-1500*time.Millisecond)) {
		t.Errorf("the response's expiry %s is not the store's", got.Expires.AsTime())
	}
}

// From a clock between seconds the expiry is still whole seconds, the token's own, so the token is valid until the expiry the response names.
func TestPackURLExpiryInWholeSeconds(t *testing.T) {
	t.Parallel()
	w := newWorld(t, catalogtest.Open(t, catalog.EngineSQLite))
	now := deadlineOf().Add(-24*time.Hour + 700*time.Millisecond)
	w.urls(t, &now, true)
	p := w.stage(t, w.admit(t))
	got, err := packURL(t, w, p.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	expiry := got.Expires.AsTime()
	if got.Expires.Nanos != 0 || expiry.After(now.Add(15*time.Minute)) {
		t.Fatalf("expiry %s from %s, want whole seconds within the lifetime", expiry, now)
	}
	u, _ := url.Parse(got.Url)
	token := strings.TrimPrefix(u.Path, "/download/")
	now = expiry.Add(-time.Nanosecond)
	if err := w.srv.Downloads.Tokens.Verify(t.Context(), token, p.Key); err != nil {
		t.Errorf("the token just before the expiry = %v", err)
	}
	now = expiry
	if err := w.srv.Downloads.Tokens.Verify(t.Context(), token, p.Key); err == nil {
		t.Error("the token at the expiry is valid")
	}
}

// failingAudit is a catalog whose audit appends fail.
type failingAudit struct{ *catalog.Catalog }

func (failingAudit) Audit(context.Context, catalog.AuditEntry) error {
	return errors.New("audit refused")
}

// A URL whose audit row cannot be written is not issued.
func TestPackURLNeedsItsAuditRow(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	w := newWorld(t, c)
	now := deadlineOf().Add(-24 * time.Hour)
	w.urls(t, &now, true)
	p := w.stage(t, w.admit(t))
	w.srv.Catalog = failingAudit{c}
	if got, err := packURL(t, w, p.ID.String()); connect.CodeOf(err) != connect.CodeInternal || got != nil {
		t.Errorf("PackURL with a failing audit = %v, %v; want no URL", got, err)
	}
}

// scrape returns s's metrics as exposed.
func scrape(t *testing.T, s *metrics.Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))

	return rec.Body.String()
}

// A request that read its deadline, then stalled while the retention was shortened and its hold passed, issues nothing:
// its URL would outlive the object the shortening lets retention evict.
func TestPackURLStalledPastAShortening(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	w := newWorld(t, c)
	now := deadlineOf().Add(-24 * time.Hour)
	w.urls(t, &now, true)
	p := w.stage(t, w.admit(t))
	l := w.lease
	const hold = time.Hour
	w.srv.stall = func(stage string) {
		if stage != "read" {
			return
		}
		recorded := now.Add(time.Second)
		if _, err := c.RecordRetention(context.Background(), l, time.Hour, hold, recorded.UnixNano()); err != nil {
			t.Error(err)
		}
		now = recorded.Add(hold + time.Second)
	}
	if got, err := packURL(t, w, p.ID.String()); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("PackURL stalled past a shortening and its hold = %v, %v; want pack-superseded", got, err)
	}
	if audit, _ := c.AuditEntries(t.Context(), p.Capture); len(audit) != 0 {
		t.Errorf("a refused URL was audited: %+v", audit)
	}
}

// noLifetime is a store that refuses every presign: until leaves no lifetime by its clock.
type noLifetime struct{}

func (noLifetime) PresignGet(context.Context, string, time.Time) (string, time.Time, error) {
	return "", time.Time{}, store.ErrNoLifetime
}

// A request that stalls after signing or after its audit row, or whose store finds no lifetime left, issues nothing living less than the floor.
func TestPackURLStalledAtEachStage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		stage     string
		presigner store.Presigner
	}{{"signed", &presigner{}}, {"signed", nil}, {"audited", &presigner{}}, {"audited", nil}, {"", noLifetime{}}} {
		w := newWorld(t, catalogtest.Open(t, catalog.EngineSQLite))
		now := deadlineOf().Add(-24 * time.Hour)
		w.urls(t, &now, true)
		w.srv.Downloads.Presigner = tc.presigner
		p := w.stage(t, w.admit(t))
		w.srv.stall = func(stage string) {
			if stage == tc.stage {
				now = now.Add(14*time.Minute + 30*time.Second) // 30 s left of a 15-minute URL
			}
		}
		if got, err := packURL(t, w, p.ID.String()); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("stalled at %q, presigned %v: PackURL = %v, %v; want pack-superseded", tc.stage, tc.presigner != nil, got, err)
		}
	}
}

// brokenSigner is a store whose presign fails with the key in its error, as a store's errors name it.
type brokenSigner struct{}

func (brokenSigner) PresignGet(_ context.Context, key string, _ time.Time) (string, time.Time, error) {
	return "", time.Time{}, fmt.Errorf("presign %s: the store broke", key)
}

// A presign that fails answers internal without the object's key.
func TestPackURLNamesNoKey(t *testing.T) {
	t.Parallel()
	w := newWorld(t, catalogtest.Open(t, catalog.EngineSQLite))
	now := deadlineOf().Add(-24 * time.Hour)
	w.urls(t, &now, true)
	w.srv.Downloads.Presigner = brokenSigner{}
	p := w.stage(t, w.admit(t))
	_, err := packURL(t, w, p.ID.String())
	if connect.CodeOf(err) != connect.CodeInternal || strings.Contains(err.Error(), p.Key) || strings.Contains(err.Error(), "broke") {
		t.Errorf("PackURL over a broken signer = %v; want internal, without the key or the store's text", err)
	}
}
