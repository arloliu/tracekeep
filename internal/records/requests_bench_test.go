package records

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/arloliu/tracekeep/internal/benchscale"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/s3store"
)

// The S3-compatible store BenchmarkReadRequests' s3 rows read from, as the s3 store's tests take it:
// scripts/test-s3.sh starts each conformance store in a container and sets these; without them the rows skip.
const (
	envS3Endpoint = "TRACEKEEP_S3_ENDPOINT"
	envS3Key      = "TRACEKEEP_S3_ACCESS_KEY"
	envS3Secret   = "TRACEKEEP_S3_SECRET_KEY"
)

// requestPayload is the frame of every record of BenchmarkReadRequests' packs.
const requestPayload = 1 << 10

// tally counts a store's requests: heads, gets and the bytes the gets returned, and any other request.
type tally struct {
	heads, gets, bytes, others atomic.Int64
}

// countingStore counts an ObjectStore's calls in t: an Open or a Stat a head, a ReadAt a get with the bytes it read,
// the requests a ranged store without read-ahead would send.
type countingStore struct {
	store.ObjectStore
	t *tally
}

type countingObject struct {
	store.Object
	t *tally
}

// countingWriter counts the bytes of a response.
type countingWriter struct {
	http.ResponseWriter
	n *atomic.Int64
}

// BenchmarkReadRequests is the store requests of a read of one pack by Reader.Read, its open and its reads (P11).
// Its stores are an fs store, whose rows count the store's calls (countingStore), and, when envS3Endpoint is set, an S3-compatible store
// through a counting proxy, whose rows count the requests on the wire, each on a bucket of its own.
// Its starting state is one staged segment of the test's hour, written once outside the timer, of 16, 1,024 or 16,384 records (TRACEKEEP_BENCH_SCALE)
// of requestPayload bytes each: a small pack, below the s3 store's read-ahead, and packs of about 1 MiB and 16 MiB.
// An iteration reads the pack's whole hour, or a narrow seq range of ten seqs in its middle, into a counting sink, writes nothing,
// and asserts the records read, no incompleteness and no request but heads and gets;
// it reports the heads, gets, requests and bytes read an operation, and the pack's size.
func BenchmarkReadRequests(b *testing.B) {
	for _, n := range benchscale.Sizes(b, 16, 1<<10, 16<<10) {
		for _, st := range []string{"fs", "s3"} {
			b.Run(fmt.Sprintf("store=%s/records=%d", st, n), func(b *testing.B) {
				t := &tally{}
				w := &world{sets: map[int64][]catalog.ReadSet{}}
				if st == "fs" {
					w.st = &countingStore{ObjectStore: newWorld(b).st, t: t}
				} else {
					w.st = countingS3(b, t)
				}
				pack := benchPacks(b, w, n, 1, false, requestPayload)
				w.serve(pack)
				r := w.reader(benchReaders)
				mid := uint64(n/2 - 4) //nolint:gosec // a positive count
				to := mid + 9
				for _, shape := range []struct {
					name string
					q    *Query
					want int
				}{
					{"whole", window(), n},
					{"narrow", &Query{Capture: capture, From: window().From, To: window().To, SeqFrom: &mid, SeqTo: &to}, 10},
				} {
					b.Run(shape.name, func(b *testing.B) {
						t.reset()
						b.ReportAllocs()
						for b.Loop() {
							var s counting
							if err := r.Read(b.Context(), shape.q, &s); err != nil || s.records != shape.want || s.incomplete != 0 {
								b.Fatalf("Read = %d records, %d incomplete, %v; want %d, 0", s.records, s.incomplete, err, shape.want)
							}
						}
						if o := t.others.Load(); o != 0 {
							b.Fatalf("%d requests neither a head nor a get", o)
						}
						per := func(v int64) float64 { return float64(v) / float64(b.N) }
						b.ReportMetric(per(t.heads.Load()), "heads/op")
						b.ReportMetric(per(t.gets.Load()), "gets/op")
						b.ReportMetric(per(t.heads.Load()+t.gets.Load()), "requests/op")
						b.ReportMetric(per(t.bytes.Load()), "read-bytes/op")
						b.ReportMetric(float64(pack[0].Size), "pack-bytes")
					})
				}
			})
		}
	}
}

// countingS3 is an s3 store on a fresh bucket of envS3Endpoint's store, reached through a proxy that counts its requests in t;
// the bucket is emptied and removed when b ends. It skips b when envS3Endpoint is not set.
func countingS3(b *testing.B, t *tally) *s3store.Store {
	b.Helper()
	endpoint := os.Getenv(envS3Endpoint)
	if endpoint == "" {
		b.Skipf("%s not set: no S3-compatible store to read from", envS3Endpoint)
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		b.Fatal(err)
	}
	// The proxy keeps the request's Host, which its signature covers.
	proxy := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.Out.URL.Scheme, r.Out.URL.Host = target.Scheme, target.Host
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			t.heads.Add(1)
		case http.MethodGet:
			t.gets.Add(1)
			rw = &countingWriter{ResponseWriter: rw, n: &t.bytes}
		default:
			t.others.Add(1)
		}
		proxy.ServeHTTP(rw, r)
	}))
	b.Cleanup(srv.Close)
	bucket := "tk-" + strings.ToLower(rand.Text())
	s, err := s3store.New(s3store.Config{Endpoint: srv.URL, Bucket: bucket, AccessKeyID: os.Getenv(envS3Key), SecretAccessKey: os.Getenv(envS3Secret)})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := s.Client().CreateBucket(b.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		b.Fatalf("create bucket: %v", err)
	}
	b.Cleanup(func() {
		ctx := context.Background()
		_ = s.List(ctx, "", func(info store.ObjectInfo) error { return s.Delete(ctx, info.Key) })
		if _, err := s.Client().DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket}); err != nil {
			b.Errorf("delete bucket: %v", err)
		}
	})

	return s
}

// reset sets every count to zero.
func (t *tally) reset() {
	for _, n := range []*atomic.Int64{&t.heads, &t.gets, &t.bytes, &t.others} {
		n.Store(0)
	}
}

func (s *countingStore) Open(ctx context.Context, key string) (store.Object, error) {
	s.t.heads.Add(1)
	o, err := s.ObjectStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}

	return &countingObject{Object: o, t: s.t}, nil
}

func (s *countingStore) Stat(ctx context.Context, key string) (store.ObjectInfo, error) {
	s.t.heads.Add(1)

	return s.ObjectStore.Stat(ctx, key)
}

func (o *countingObject) ReadAt(p []byte, off int64) (int, error) {
	n, err := o.Object.ReadAt(p, off)
	o.t.gets.Add(1)
	o.t.bytes.Add(int64(n))

	return n, err
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.n.Add(int64(n))

	return n, err
}

// Unwrap lets the proxy flush the response it copies.
func (w *countingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
