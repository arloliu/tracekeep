package producer

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer/busconfig"
	"github.com/arloliu/tracekeep/producer/internal/core"
)

// Problem is one thing Preflight found wrong with the bus or the capture.
type Problem struct {
	// Check names what was checked: "stream", "bucket", "max_payload", "tls", "reconnect_buffer" or "start".
	Check string
	// Detail says what is wrong, for a person to read.
	Detail string
}

// Preflight checks the bus the producer publishes on and its current capture's start,
// and returns a readable list of what is wrong (docs/specs/tracekeep/producer-sdk.md §4.7):
// the traffic stream and the claim bucket's stream against busconfig's settings,
// max_payload against tracepack's minimum, the connection's TLS and reconnect buffer,
// and the current capture's start message within the header-block and max_payload limits.
// Begin and Rotate already refuse a descriptor without tool_id or recorder_instance_name.
// Encryption at rest is a deployment assurance no client can see, so Preflight does not check it.
// It never creates or alters a stream; an empty list means it found nothing wrong.
//
// Returns:
//   - []Problem: what it found wrong, in the order above.
//   - error: ErrNoBus, or an error from reading a stream's settings; the problems found before it are returned with it.
func (p *Producer) Preflight(ctx context.Context) ([]Problem, error) {
	if p.pub == nil {
		return nil, ErrNoBus
	}
	bus := p.pub.bus
	var problems []Problem
	add := func(check string, details []string) {
		for _, d := range details {
			problems = append(problems, Problem{Check: check, Detail: d})
		}
	}

	want := busconfig.StreamConfig(p.stream, p.prefix)
	traffic, err := streamProblems(ctx, bus, p.stream, func(s *StreamInfo) []string { return trafficProblems(s, &want) })
	if err != nil {
		return problems, err
	}
	add("stream", traffic)
	claim, err := streamProblems(ctx, bus, busconfig.ClaimStreamName(p.bucket), claimProblems)
	if err != nil {
		return problems, err
	}
	add("bucket", claim)

	maxPayload := bus.MaxPayload()
	if maxPayload < tracepack.BusMinMaxPayload {
		add("max_payload", []string{fmt.Sprintf("max_payload %d is below %d", maxPayload, tracepack.BusMinMaxPayload)})
	}
	problems = append(problems, bus.CheckConn()...)

	// A max_payload below the minimum admits no start, which the check above already names.
	if cur := p.rec.Current(); cur != nil && maxPayload >= tracepack.BusMinMaxPayload {
		start := core.Entry{Cap: cur, Kind: core.EntryStart, Wall: cur.OriginWall, Mono: cur.OriginMono}
		if _, err := p.buildMessage(&start, cur.Subject, maxPayload); err != nil {
			add("start", []string{fmt.Sprintf("capture %s: %v", cur.ID, err)})
		}
	}

	return problems, nil
}

// String returns the problem as "<check>: <detail>".
func (p Problem) String() string { return p.Check + ": " + p.Detail }

// streamProblems reads the named stream and checks it; a missing stream is a problem, not an error.
func streamProblems(ctx context.Context, bus Bus, name string, check func(*StreamInfo) []string) ([]string, error) {
	info, err := bus.StreamInfo(ctx, name)
	switch {
	case errors.Is(err, ErrStreamNotFound):
		return []string{fmt.Sprintf("stream %s does not exist", name)}, nil
	case err != nil:
		return nil, fmt.Errorf("producer: preflight: stream %s: %w", name, err)
	}

	return check(&info), nil
}

// trafficProblems checks the traffic stream against busconfig.StreamConfig.
func trafficProblems(s *StreamInfo, want *jetstream.StreamConfig) []string {
	var out []string
	for _, f := range []struct{ name, got, want string }{
		{"retention", s.Retention, want.Retention.String()},
		{"storage", s.Storage, want.Storage.String()},
		{"compression", s.Compression, want.Compression.String()},
	} {
		if f.got != f.want {
			out = append(out, fmt.Sprintf("stream %s has %s %s, want %s", s.Name, f.name, f.got, f.want))
		}
	}
	if !slices.Contains(s.Subjects, want.Subjects[0]) {
		out = append(out, fmt.Sprintf("stream %s takes subjects %q, not %s", s.Name, s.Subjects, want.Subjects[0]))
	}
	if s.MaxMsgsPerSubject > 0 {
		out = append(out, fmt.Sprintf("stream %s keeps at most %d messages per subject, want no limit", s.Name, s.MaxMsgsPerSubject))
	}
	if s.Derived {
		out = append(out, fmt.Sprintf("stream %s has a mirror or sources, whose sequences are not its own", s.Name))
	}

	return append(out, sharedProblems(s)...)
}

// claimProblems checks the claim bucket's stream: file storage, no limit, no direct reads (T-28).
func claimProblems(s *StreamInfo) []string {
	var out []string
	if want := jetstream.FileStorage.String(); s.Storage != want {
		out = append(out, fmt.Sprintf("stream %s has storage %s, want %s", s.Name, s.Storage, want))
	}

	return append(out, sharedProblems(s)...)
}

// sharedProblems checks what the traffic stream and the claim bucket share:
// no age, size or count limit, and AllowDirect off, so every read is the stream leader's.
func sharedProblems(s *StreamInfo) []string {
	var out []string
	if s.MaxAge > 0 {
		out = append(out, fmt.Sprintf("stream %s expires messages after %s, want no age limit", s.Name, s.MaxAge))
	}
	if s.MaxBytes > 0 {
		out = append(out, fmt.Sprintf("stream %s holds at most %d bytes, want no size limit", s.Name, s.MaxBytes))
	}
	if s.MaxMsgs > 0 {
		out = append(out, fmt.Sprintf("stream %s holds at most %d messages, want no count limit", s.Name, s.MaxMsgs))
	}
	if s.AllowDirect {
		out = append(out, fmt.Sprintf("stream %s allows direct reads, which a lagging replica can answer", s.Name))
	}

	return out
}
