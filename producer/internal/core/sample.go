package core

import "sync"

// Sampling is a capture's sampling rule with its defaults filled in (docs/specs/tracekeep/producer.md §4.6 *Sampling rule*).
type Sampling struct {
	// KeepEveryN is N, at least 1.
	KeepEveryN uint64
	// KeepInterval is T in nanoseconds.
	KeepInterval int64
}

// sampler applies a capture's rule to the marked S1F3 registered while the capture is current;
// its count and last kept time are the capture's, so each capture's first poll keeps its bodies.
type sampler struct {
	rule Sampling

	mu sync.Mutex
	// Guarded by mu.
	count    uint64
	lastKept int64
}

// thin counts one poll registered at now, mono ns, and reports whether it gives up its bodies.
func (s *sampler) thin(now int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := s.count%s.rule.KeepEveryN == 0 || now-s.lastKept >= s.rule.KeepInterval
	s.count++
	if keep {
		s.lastKept = now
	}

	return !keep
}
