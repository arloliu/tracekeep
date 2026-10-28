package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
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

// presigner records the lifetimes it presigned for.
type presigner struct {
	mu        sync.Mutex
	lifetimes []time.Duration
}

func (p *presigner) PresignGet(_ context.Context, key string, lifetime time.Duration) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lifetimes = append(p.lifetimes, lifetime)

	return "https://store.example.invalid/" + key + "?X-Amz-Signature=synthetic", nil
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
	l, ok, err := w.c.TakeLease(t.Context(), "compactor", "test", time.Hour)
	if err != nil || !ok {
		t.Fatalf("TakeLease = %v, %v", ok, err)
	}
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

// Where the store presigns and presigning is on, the URL is presigned for its remaining lifetime in whole seconds.
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
	if len(ps.lifetimes) != 1 || now.Add(ps.lifetimes[0]).After(got.Expires.AsTime()) || ps.lifetimes[0] > 15*time.Minute {
		t.Errorf("presigned for %v from %s, the URL expiring %s; want no longer than its expiry", ps.lifetimes, now, got.Expires.AsTime())
	}
	if got.Expires.Nanos != 0 {
		t.Errorf("the expiry %s is not whole seconds", got.Expires.AsTime())
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
