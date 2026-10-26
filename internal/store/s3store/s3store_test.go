package s3store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/s3store"
	"github.com/arloliu/tracekeep/internal/store/storetest"
)

// The suite needs a store: `make test-s3` starts each in a container and sets these;
// without them the tests skip, so `make test` stays offline.
const (
	envEndpoint = "TRACEKEEP_S3_ENDPOINT"
	envKey      = "TRACEKEEP_S3_ACCESS_KEY"
	envSecret   = "TRACEKEEP_S3_SECRET_KEY"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T) store.ObjectStore { return newStore(t) })
}

// A presigned URL reads the object, and one of an absent key is refused as not found, not for its signature:
// the check the api role makes at start (storage.md §5.2 *Downloads*).
func TestPresignGet(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	data := []byte("pack bytes")
	if err := s.Put(t.Context(), "archive/c1/p1", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if got, status := fetch(t, s, "archive/c1/p1"); status != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("presigned GET = %d %q, want 200 %q", status, got, data)
	}
	if _, status := fetch(t, s, "archive/c1/none"); status != http.StatusNotFound {
		t.Errorf("presigned GET of an absent key = %d, want 404", status)
	}
}

// newStore returns a store on a bucket of its own, emptied and removed when the test ends.
func newStore(t *testing.T) *s3store.Store {
	t.Helper()
	endpoint := os.Getenv(envEndpoint)
	if endpoint == "" {
		t.Skipf("%s not set: no S3-compatible store to test against", envEndpoint)
	}
	bucket := "tk-" + strings.ToLower(rand.Text())
	s, err := s3store.New(s3store.Config{
		Endpoint: endpoint, Bucket: bucket,
		AccessKeyID: os.Getenv(envKey), SecretAccessKey: os.Getenv(envSecret),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Client().CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = s.List(ctx, "", func(info store.ObjectInfo) error { return s.Delete(ctx, info.Key) })
		if _, err := s.Client().DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket}); err != nil {
			t.Errorf("delete bucket: %v", err)
		}
	})

	return s
}

func fetch(t *testing.T, s *s3store.Store, key string) ([]byte, int) {
	t.Helper()
	url, err := s.PresignGet(t.Context(), key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return body, resp.StatusCode
}
