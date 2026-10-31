package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/records"
)

func (c *ExportConfig) lease() time.Duration      { return cmp.Or(c.Lease, time.Minute) }
func (c *ExportConfig) putTimeout() time.Duration { return cmp.Or(c.PutTimeout, 10*time.Minute) }
func (c *ExportConfig) retention() time.Duration  { return cmp.Or(c.Retention, 24*time.Hour) }
func (c *ExportConfig) maxAttempts() int          { return cmp.Or(c.MaxAttempts, 3) }
func (c *ExportConfig) maxRunning() int           { return cmp.Or(c.MaxRunning, 1) }

// RunExports runs this replica's export attempts, as holder, until ctx ends, looking for a claimable job every poll
// (api.md §10.1 *Exports*); it first removes the attempt directories a crash left in the work directory.
func (s *Server) RunExports(ctx context.Context, holder string, poll time.Duration) error {
	if err := s.clearWorkDir(); err != nil {
		return fmt.Errorf("api: exports: work directory: %w", err)
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	slots := make(chan struct{}, s.Exports.maxRunning())
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case slots <- struct{}{}:
		}
		// The lease counts from before the claim by this replica's clock: the catalog's reads it later.
		claimed := time.Now()
		cl, ok, err := s.Catalog.ClaimExport(ctx, holder, s.Exports.lease(), s.Exports.maxAttempts())
		for range cl.Exhausted {
			s.Metrics.Export(catalog.ExportFailed, 0)
		}
		if err != nil || !ok {
			<-slots
			if err != nil && ctx.Err() == nil && s.Log != nil {
				s.Log.Warn("api: exports: claim", "error", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
			}

			continue
		}
		wg.Go(func() {
			defer func() { <-slots }()
			s.attempt(ctx, &cl, claimed)
		})
	}
}

// clearWorkDir creates the work directory, or removes from it the attempt directories a crash left: those named as attempts name theirs,
// a job id and a fence, so nothing else a shared directory holds is touched.
func (s *Server) clearWorkDir() error {
	if err := os.MkdirAll(s.workDir(), 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(s.workDir())
	if err != nil {
		return err
	}
	for _, e := range entries {
		// The last "-f": a UUID's hex digits may follow its own hyphens with an f.
		i := strings.LastIndex(e.Name(), "-f")
		if i < 0 {
			continue
		}
		job, fence := e.Name()[:i], e.Name()[i+2:]
		if _, err := tracepack.ParseUUID(job); !e.IsDir() || err != nil || fence == "" || strings.Trim(fence, "0123456789") != "" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(s.workDir(), e.Name())); err != nil {
			return err
		}
	}

	return nil
}

// workDir is the configured work directory, or one under the system's temporary directory.
func (s *Server) workDir() string {
	return cmp.Or(s.Exports.WorkDir, filepath.Join(os.TempDir(), "tracekeep-exports"))
}

// attempt runs one attempt of a claimed job: its bundle, its intent, its put bounded by the put deadline, and its completion;
// the lease is renewed meanwhile, and a renewal refused ends the attempt.
func (s *Server) attempt(parent context.Context, cl *catalog.Claim, claimed time.Time) {
	a := cl.Attempt
	ctx, cancel := context.WithCancel(parent)
	// The lease timer starts before any work: it fires when the claim's lease passes, unless a renewal moves it.
	expiry := time.AfterFunc(s.Exports.lease()-time.Since(claimed), cancel)
	var renewing sync.WaitGroup
	renewing.Go(func() { s.renew(ctx, cancel, a, expiry) })
	dir := filepath.Join(s.workDir(), fmt.Sprintf("%s-f%d", a.Job, a.Fence))
	outcome, err := s.runAttempt(ctx, dir, cl)
	// The attempt is over: its renewal stops, so a lease left running lapses and the next attempt claims the job.
	cancel()
	renewing.Wait()
	_ = os.RemoveAll(dir)
	switch {
	case outcome != "":
		s.Metrics.Export(outcome, 0)
	case err != nil && s.Log != nil && parent.Err() == nil:
		// The attempt ends; the lease's expiry starts the next.
		s.Log.Warn("api: exports: attempt ended", "job", a.Job.String(), "fence", a.Fence, "error", err)
	default: // done, counted where it completed
	}
}

func (s *Server) runAttempt(ctx context.Context, dir string, cl *catalog.Claim) (string, error) {
	a := cl.Attempt
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	var req exportRequest
	if err := json.Unmarshal(cl.Job.Request, &req); err != nil {
		return s.fail(ctx, a, "the request does not decode")
	}
	path, size, err := s.buildBundle(ctx, a.Job, &req, dir)
	switch {
	case errors.Is(err, errBounds):
		return s.fail(ctx, a, err.Error())
	case errors.Is(err, records.ErrWithheld):
		return s.fail(ctx, a, "the capture is withheld")
	case err != nil:
		return "", err
	}
	s.exportStalled("built")
	key := fmt.Sprintf("exports/%s/%d.tar", a.Job, a.Fence)
	// The put deadline counts from before the intent, by this replica's clock: the catalog's, read during the intent, is later
	// but for the clock skew, which the cleanup waits out twice, so no put outlives the deadline the cleanup reads.
	putCtx, stop := context.WithDeadline(ctx, time.Now().Add(s.Exports.putTimeout()))
	defer stop()
	if _, err := s.Catalog.IntendExport(ctx, a, key, s.Exports.putTimeout()); err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := s.Store.Put(putCtx, key, f, size); err != nil {
		return "", records.Keyless(err)
	}
	s.exportStalled("put")
	if _, err := s.Catalog.CompleteExport(ctx, a, key, size, s.Exports.retention()); err != nil {
		return "", err // a lost completion leaves its object to the cleanup
	}
	s.Metrics.Export(catalog.ExportDone, size)

	return "", nil
}

// fail fails the attempt's job with reason, which names no key.
func (s *Server) fail(ctx context.Context, a catalog.Attempt, reason string) (string, error) {
	if err := s.Catalog.FailExport(ctx, a, reason); err != nil {
		return "", err
	}

	return catalog.ExportFailed, nil
}

// renew renews a's lease every third of it until ctx ends, cancelling the attempt when a renewal is refused,
// and, independently of any renewal in flight, when the lease would pass by this replica's clock:
// the deadline counts from before each renewal's call, which the catalog's clock reads later.
func (s *Server) renew(ctx context.Context, cancel context.CancelFunc, a catalog.Attempt, expiry *time.Timer) {
	lease := s.Exports.lease()
	defer expiry.Stop()
	t := time.NewTicker(lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		before := time.Now()
		s.exportStalled("renew")
		rctx, stop := context.WithTimeout(ctx, lease/3)
		err := s.Catalog.RenewExport(rctx, a, lease)
		stop()
		if err != nil {
			cancel()
			return
		}
		if !expiry.Stop() {
			return // the lease passed while the renewal ran: the attempt is cancelled
		}
		expiry.Reset(lease - time.Since(before))
	}
}

func (s *Server) exportStalled(stage string) {
	if s.exportStall != nil {
		s.exportStall(stage)
	}
}
