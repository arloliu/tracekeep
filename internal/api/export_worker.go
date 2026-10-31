package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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
	if err := os.RemoveAll(s.Exports.WorkDir); err != nil {
		return fmt.Errorf("api: exports: clear the work directory: %w", err)
	}
	if err := os.MkdirAll(s.Exports.WorkDir, 0o700); err != nil {
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
		cl, ok, err := s.Catalog.ClaimExport(ctx, holder, s.Exports.lease(), s.Exports.maxAttempts())
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
			s.attempt(ctx, &cl)
		})
	}
}

// attempt runs one attempt of a claimed job: its bundle, its intent, its put bounded by the put deadline, and its completion;
// the lease is renewed meanwhile, and a renewal refused ends the attempt.
func (s *Server) attempt(parent context.Context, cl *catalog.Claim) {
	a := cl.Attempt
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var renewing sync.WaitGroup
	defer renewing.Wait()
	renewing.Go(func() { s.renew(ctx, cancel, a) })
	dir := filepath.Join(s.Exports.WorkDir, fmt.Sprintf("%s-%d", a.Job, a.Fence))
	defer os.RemoveAll(dir)
	outcome, err := s.runAttempt(ctx, dir, cl)
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
	if _, err := s.Catalog.IntendExport(ctx, a, key, s.Exports.putTimeout()); err != nil {
		return "", err
	}
	// Bounded by this replica's clock from now, within the catalog's deadline but for the clock skew the cleanup waits out twice.
	putCtx, stop := context.WithTimeout(ctx, s.Exports.putTimeout())
	defer stop()
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

// renew renews a's lease every third of it until ctx ends, cancelling the attempt when a renewal is refused or comes too late.
func (s *Server) renew(ctx context.Context, cancel context.CancelFunc, a catalog.Attempt) {
	lease := s.Exports.lease()
	t := time.NewTicker(lease / 3)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s.exportStalled("renew")
		if err := s.Catalog.RenewExport(ctx, a, lease); err != nil || time.Since(last) > lease {
			cancel()
			return
		}
		last = time.Now()
	}
}

func (s *Server) exportStalled(stage string) {
	if s.exportStall != nil {
		s.exportStall(stage)
	}
}
