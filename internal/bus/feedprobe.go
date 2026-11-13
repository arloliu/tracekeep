package bus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// probesPerGroup bounds the mapping probe at this many publishes a group:
// the partition spreads tokens evenly, so a count of G reaches every group long before, and one below G never does.
const probesPerGroup = 64

// probeWait bounds a probe, its mapped copy arriving at once when a mapping routes it.
const probeWait = 2 * time.Second

// ErrFeedMapping refuses servers whose account mapping does not partition the feed's subjects into exactly its G group streams (logfeed.md §15.4).
var ErrFeedMapping = errors.New("bus: the servers' mapping does not partition the feed's subjects into its group streams")

// ProbeFeedMapping checks that the servers' mapping routes the feed's subjects to its G group streams, every role's preflight with the feed enabled (logfeed.md §15.4):
// it publishes probe entries under tokens of its own until every group's stream has been reached, refusing a probe no mapping routes,
// one routed to a subject with no group stream, which a count above G gives, and a run past its bound with a group unreached, which a count below G gives.
// Each probe expects a last sequence no stream reaches, so the stream that takes it refuses to store it;
// its copy to a core subscription of nc tells the group the mapping chose.
func ProbeFeedMapping(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, f Feed) error {
	reached := make(map[int]bool, f.Groups)
	for i := 0; len(reached) < f.Groups; i++ {
		if i == probesPerGroup*f.Groups {
			return fmt.Errorf("%w: %d probes reached %d of %d groups, as a mapping into fewer partitions does", ErrFeedMapping, i, len(reached), f.Groups)
		}
		n, err := probeFeed(ctx, nc, js, f, "_tracekeep_probe_"+strconv.Itoa(i))
		if err != nil {
			return err
		}
		reached[n] = true
	}

	return nil
}

// probeFeed publishes one probe under token and returns the group its mapped subject names, once that group's stream refused it.
func probeFeed(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, f Feed, token string) (int, error) {
	subject := f.Prefix + "." + token
	ctx, cancel := context.WithTimeout(ctx, probeWait)
	defer cancel()
	sub, err := nc.SubscribeSync(f.Prefix + ".*." + token)
	if err != nil {
		return 0, fmt.Errorf("bus: feed probe: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushWithContext(ctx); err != nil {
		return 0, fmt.Errorf("bus: feed probe: %w", err)
	}
	// The subscription answers a publish no stream takes, never replying, so the copy is read before the publish's answer is awaited.
	published := make(chan error, 1)
	go func() {
		_, err := js.Publish(ctx, subject, nil, jetstream.WithExpectLastSequence(math.MaxInt64), jetstream.WithRetryAttempts(0))
		published <- err
	}()
	m, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return 0, fmt.Errorf("%w: no mapping routed %s within %s: %w", ErrFeedMapping, subject, probeWait, err)
	}
	n, err := strconv.Atoi(strings.Split(m.Subject, ".")[strings.Count(f.Prefix, ".")+1])
	if err != nil || n < 0 || n >= f.Groups {
		return 0, fmt.Errorf("%w: %s mapped to %s, outside groups 0 to %d", ErrFeedMapping, subject, m.Subject, f.Groups-1)
	}
	perr := <-published
	var api *jetstream.APIError
	if !errors.As(perr, &api) || api.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
		return 0, fmt.Errorf("%w: %s mapped to %s reached no group stream: %w", ErrFeedMapping, subject, m.Subject, perr)
	}

	return n, nil
}
