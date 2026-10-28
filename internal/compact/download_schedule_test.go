package compact

import (
	"bytes"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	connectrpc "connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/api"
	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

const readerToken = "a-static-token-of-a-pack-reader-0001"

// replica is one api process over f's catalog and store: its own keys over the shared key file, its clock at *now.
type replica struct {
	cl  tracekeepv1connect.MachineServiceClient
	srv *httptest.Server
}

// keyFile writes a synthetic download key, mode 0600, and returns its path.
func keyFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dl-1")
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{5}, 32))), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func (f *fixture) replica(t *testing.T, key string, now *time.Time) *replica {
	t.Helper()
	k, err := keys.New(keys.Config{
		Declared: map[string]keys.Declaration{"dl-1": {Provider: keys.ProviderFile, Ref: key}},
		Download: keys.Purpose{Issuing: "dl-1", Accepted: []string{"dl-1"}},
	}, f.c)
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return *now }
	s := &api.Server{
		Catalog: f.c, Store: f.s, QuietPeriod: 15 * time.Minute,
		Auth: auth.NewAuthenticator(auth.AuthenticatorConfig{StaticEnabled: true, Static: []auth.StaticToken{
			{Token: readerToken, Subject: "reader", Roles: []string{auth.RolePackReader}},
		}}),
		Downloads: api.Downloads{
			Tokens: &auth.Downloads{Keys: k, Now: clock}, PublicURL: "http://placeholder.invalid",
			Lifetime: 15 * time.Minute, Margin: 2 * time.Minute, Floor: time.Minute, DeleteDelay: f.r.DeleteDelay, Now: clock,
		},
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	s.Downloads.PublicURL = srv.URL

	return &replica{cl: tracekeepv1connect.NewMachineServiceClient(srv.Client(), srv.URL), srv: srv}
}

func (r *replica) packURL(t *testing.T, pack *catalog.Pack) (*tkv1.PackURLResponse, error) {
	t.Helper()
	req := connectrpc.NewRequest(&tkv1.PackURLRequest{PackId: pack.ID.String()})
	req.Header().Set("Authorization", "Bearer "+readerToken)
	resp, err := r.cl.PackURL(t.Context(), req)
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

// fetch GETs u from r and returns its status.
func (r *replica) fetch(t *testing.T, u string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, u, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := r.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	return resp.StatusCode
}

func superseded(err error) bool {
	var ce *connectrpc.Error

	return errors.As(err, &ce) && ce.Code() == connectrpc.CodeFailedPrecondition
}

// The gate's promised download: a URL issued, then the retention shortened, and the URL's token and its object both valid
// until the URL's expiry, a second api process validating the first's token; URLs issued after the shortening are clamped to it.
func TestPromisedDownloadOutlivesAShortenedRetention(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		f.r.DeleteDelay = time.Hour
		p := f.stage(t, writeSegment(t, f.admit(t), []uint64{0, 1}, noAdjust))
		at := time.Unix(0, (hour+3)*nsPerHour) // two hours past the hour's end: a one-hour retention would evict it
		if _, err := c.RecordRetention(t.Context(), f.lease, 48*time.Hour, f.r.DeleteDelay, at.UnixNano()); err != nil {
			t.Fatal(err)
		}
		now := at
		key := keyFile(t)
		first := f.replica(t, key, &now)
		u, err := first.packURL(t, p)
		if err != nil {
			t.Fatal(err)
		}
		expiry := u.Expires.AsTime()
		if !expiry.Equal(at.Add(15 * time.Minute)) {
			t.Fatalf("expiry %s, want %s", expiry, at.Add(15*time.Minute))
		}
		if _, err := c.RecordRetention(t.Context(), f.lease, time.Hour, f.r.DeleteDelay, at.UnixNano()); err != nil {
			t.Fatal(err)
		}
		for _, when := range []time.Time{expiry.Add(-time.Second), expiry} {
			if got, err := c.EvictCandidates(t.Context(), when.UnixNano()); err != nil || len(got) != 0 {
				t.Errorf("evictable at %s = %v, %v; want nothing before the shortening's hold ends", when, got, err)
			}
		}
		if got, err := c.EvictCandidates(t.Context(), at.Add(f.r.DeleteDelay+time.Second).UnixNano()); err != nil || len(got) != 1 {
			t.Errorf("evictable once the hold ended = %v, %v; want the hour", got, err)
		}
		// A second process, keys resolved afresh against the binding, serves the first's URL until its expiry.
		second := f.replica(t, key, &now)
		swapped := second.srv.URL + u.Url[len(first.srv.URL):]
		now = expiry.Add(-time.Second)
		if code := second.fetch(t, swapped); code != http.StatusOK {
			t.Errorf("the second process's download a second before the expiry = %d, want 200", code)
		}
		now = expiry
		if code := second.fetch(t, swapped); code != http.StatusForbidden {
			t.Errorf("the second process's download at the expiry = %d, want refused", code)
		}
		now = at.Add(50 * time.Minute)
		if got, err := second.packURL(t, p); err != nil || !got.Expires.AsTime().Equal(at.Add(time.Hour-2*time.Minute)) {
			t.Errorf("a URL after the shortening = %v, %v; want it clamped to the held deadline less the margin", got, err)
		}
	})
}

// A merged input's URLs are clamped to its merge's deletion time less the margin, refused as pack-superseded inside the floor
// and once it is deleted; its archive's are issued.
func TestPackURLAroundAMergesDeletion(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		f := newFixture(t, c)
		f.merging(t)
		f.r.DeleteDelay = time.Hour
		capture := f.admit(t)
		a := f.stage(t, writeSegment(t, capture, []uint64{0, 1}, noAdjust))
		f.stage(t, writeSegment(t, capture, []uint64{2, 3}, noAdjust))
		mergedAt := f.r.now()
		// A period long enough that the hour outlives the merge's deletion time, which the catalog's clock sets.
		if _, err := c.RecordRetention(t.Context(), f.lease, 1_000_000*time.Hour, f.r.DeleteDelay, mergedAt.UnixNano()); err != nil {
			t.Fatal(err)
		}
		now := mergedAt
		r := f.replica(t, keyFile(t), &now)
		before, err := r.packURL(t, a)
		if err != nil || !before.Expires.AsTime().Equal(mergedAt.Add(15*time.Minute)) {
			t.Fatalf("PackURL before the merge = %v, %v", before, err)
		}
		if rep := f.merges(t); len(rep.Merged) != 1 {
			t.Fatalf("Merges = %+v", rep)
		}
		deleteAt := time.Unix(0, deadline(t, c, a, f.r.DeleteDelay))
		if !deleteAt.After(before.Expires.AsTime()) {
			t.Errorf("the merge deletes its input at %s, before the URL issued earlier expires at %s", deleteAt, before.Expires.AsTime())
		}
		now = deleteAt.Add(-10 * time.Minute)
		if got, err := r.packURL(t, a); err != nil || !got.Expires.AsTime().Equal(deleteAt.Add(-2*time.Minute).Truncate(time.Second)) {
			t.Errorf("PackURL 10 minutes before the deletion = %v, %v; want it clamped", got, err)
		}
		now = deleteAt.Add(-2*time.Minute - 59*time.Second)
		if _, err := r.packURL(t, a); !superseded(err) {
			t.Errorf("PackURL inside the floor = %v, want pack-superseded", err)
		}
		f.r.Now = func() time.Time { return deleteAt.Add(time.Second) }
		var rep RetentionReport
		if err := f.r.Deletions(t.Context(), f.lease, &rep); err != nil || rep.Deleted != 2 {
			t.Fatalf("Deletions = %+v, %v", rep, err)
		}
		now = deleteAt.Add(time.Second)
		if _, err := r.packURL(t, a); !superseded(err) {
			t.Errorf("PackURL of a deleted input = %v, want pack-superseded", err)
		}
		archive := f.archivePack(t)
		if got, err := r.packURL(t, archive); err != nil || got.Url == "" {
			t.Errorf("PackURL of the archive = %v, %v", got, err)
		}
	})
}

// deadline is pack's deadline under the recorded retention and the merge's deletion time.
func deadline(t *testing.T, c *catalog.Catalog, pack *catalog.Pack, delay time.Duration) int64 {
	t.Helper()
	d, ok, err := c.Deadline(t.Context(), pack.ID, delay)
	if err != nil || !ok {
		t.Fatalf("Deadline = %v, %v", ok, err)
	}

	return d
}

// archivePack is the one archive in f's store, read back as the reconciler reads a pack.
func (f *fixture) archivePack(t *testing.T) *catalog.Pack {
	t.Helper()
	keys := f.archives(t)
	if len(keys) != 1 {
		t.Fatalf("archives %v, want one", keys)
	}
	info, err := f.s.Stat(t.Context(), keys[0])
	if err != nil {
		t.Fatal(err)
	}
	p, reason, err := f.r.read(t.Context(), info)
	if err != nil || reason != "" {
		t.Fatalf("read = %q, %v", reason, err)
	}

	return p
}
