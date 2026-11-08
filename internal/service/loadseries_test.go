package service

import (
	"context"
	"database/sql"
	"encoding/csv"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/s3store"
)

// The feed-convert mode's resource series, for judging when the fleet's flushes load the store, the catalog and the bus:
// TRACEKEEP_LOAD_SERIES names a directory that gets series.csv, a row a second (the packs' registration times are the catalog's own);
// TRACEKEEP_LOAD_S3 (any value) puts the packs in a new bucket of the S3 store TRACEKEEP_S3_ENDPOINT names, as the store tests do;
// TRACEKEEP_LOAD_PHASE=spread spreads the log stage's flushes over F (logfeed.Converter.Phase);
// TRACEKEEP_LOAD_HOURS=hold leaves an earlier hour's batches to the equipment's next flush (logfeed.Converter.HoldHours).
const (
	envSeries = "TRACEKEEP_LOAD_SERIES"
	envLoadS3 = "TRACEKEEP_LOAD_S3"
	envPhase  = "TRACEKEEP_LOAD_PHASE"
	envHours  = "TRACEKEEP_LOAD_HOURS"
)

// seriesStore counts the puts of log captures (their keys naming a fleet equipment) and of traffic captures apart.
type seriesStore struct {
	store.ObjectStore
	mu       sync.Mutex
	log, trf putCount
}

type putCount struct {
	n, bytes int64
	slowest  time.Duration
}

func (s *seriesStore) Put(ctx context.Context, key string, r io.ReadSeeker, size int64) error {
	began := time.Now()
	err := s.ObjectStore.Put(ctx, key, r, size)
	took := time.Since(began)
	s.mu.Lock()
	c := &s.trf
	if strings.Contains(key, "/FEED-") {
		c = &s.log
	}
	c.n++
	c.bytes += size
	c.slowest = max(c.slowest, took)
	s.mu.Unlock()

	return err
}

// take returns the counts since the last take.
func (s *seriesStore) take() (log, trf putCount) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, trf = s.log, s.trf
	s.log, s.trf = putCount{}, putCount{}

	return log, trf
}

// spreadPhase is an equipment's phase over F, from its id.
func spreadPhase(flush time.Duration) func(string) time.Duration {
	return func(id string) time.Duration {
		h := fnv.New64a()
		_, _ = h.Write([]byte(id))

		return time.Duration(h.Sum64() % uint64(flush)) //nolint:gosec // a flush interval is positive
	}
}

// installSeries sets up what the series environment asks for on dev before it runs; it returns nil when TRACEKEEP_LOAD_SERIES is unset.
func installSeries(ctx context.Context, t *testing.T, d *Dev) *seriesStore {
	t.Helper()
	dir := os.Getenv(envSeries)
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if os.Getenv(envLoadS3) != "" {
		bucket := "tk-load-" + strconv.FormatInt(time.Now().Unix(), 10)
		st, err := s3store.New(s3store.Config{Endpoint: os.Getenv("TRACEKEEP_S3_ENDPOINT"), Bucket: bucket,
			AccessKeyID: os.Getenv("TRACEKEEP_S3_ACCESS_KEY"), SecretAccessKey: os.Getenv("TRACEKEEP_S3_SECRET_KEY")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Client().CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
			t.Fatal(err)
		}
		d.Env.Store = st
	}
	if os.Getenv(envPhase) == "spread" {
		d.Env.logPhase = spreadPhase(d.Env.Config.Logfeed.Flush(d.Env.Config.Ingest.FlushInterval))
	}
	d.Env.logHoldHours = os.Getenv(envHours) == "hold"
	s := &seriesStore{ObjectStore: d.Env.Store}
	d.Env.Store = s

	return s
}

// recordSeries writes series.csv a row a second until ctx ends, and packs.csv once the run is over.
func recordSeries(ctx context.Context, t *testing.T, h *loadHarness, s *seriesStore) {
	t.Helper()
	feed, err := h.d.Env.JS.Stream(ctx, h.d.Env.Config.Logfeed.Stream)
	if err != nil {
		t.Fatal(err)
	}
	dir := os.Getenv(envSeries)
	var db *sql.DB
	if h.engine == catalog.EngineMariaDB {
		if db, err = sql.Open("mysql", h.d.Env.Config.Catalog.DSN); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	f, err := os.Create(filepath.Join(dir, "series.csv"))
	if err != nil {
		t.Fatal(err)
	}
	w := csv.NewWriter(f)
	status := []string{"Com_commit", "Innodb_data_written", "Innodb_os_log_written", "Innodb_row_lock_waits", "Innodb_row_lock_time", "Bytes_received", "Bytes_sent"}
	_ = w.Write(append([]string{"unix", "log_puts", "log_put_bytes", "log_put_slowest_ms", "traffic_puts", "traffic_put_bytes", "traffic_put_slowest_ms",
		"feed_last_seq", "feed_msgs", "traffic_last_seq", "traffic_msgs", "threads_running"}, status...))
	go func() {
		defer f.Close()
		defer w.Flush()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				lg, tr := s.take()
				row := []string{strconv.FormatInt(now.Unix(), 10),
					strconv.FormatInt(lg.n, 10), strconv.FormatInt(lg.bytes, 10), strconv.FormatInt(lg.slowest.Milliseconds(), 10),
					strconv.FormatInt(tr.n, 10), strconv.FormatInt(tr.bytes, 10), strconv.FormatInt(tr.slowest.Milliseconds(), 10)}
				for _, st := range []jetstream.Stream{feed, h.stream} {
					si, err := st.Info(ctx)
					if err != nil {
						row = append(row, "", "")
						continue
					}
					row = append(row, strconv.FormatUint(si.State.LastSeq, 10), strconv.FormatUint(si.State.Msgs, 10))
				}
				row = append(row, globalStatus(ctx, db, append([]string{"Threads_running"}, status...))...)
				_ = w.Write(row)
				w.Flush()
			}
		}
	}()
}

// globalStatus reads MariaDB's status variables names in order, empty without a database.
func globalStatus(ctx context.Context, db *sql.DB, names []string) []string {
	out := make([]string, len(names))
	if db == nil {
		return out
	}
	rows, err := db.QueryContext(ctx, "SHOW GLOBAL STATUS WHERE Variable_name IN ('"+strings.Join(names, "','")+"')")
	if err != nil {
		return out
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var k, v string
		if rows.Scan(&k, &v) == nil {
			got[k] = v
		}
	}
	if rows.Err() != nil {
		return out
	}
	for i, n := range names {
		out[i] = got[n]
	}

	return out
}
