package busconfig

import (
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// DefaultTrafficStream is the traffic stream's name when a deployment names none;
// every publish expects it (Nats-Expected-Stream).
const DefaultTrafficStream = "TRACEKEEP_TRAFFIC"

// DefaultParkedStream is the parking stream's name when a deployment names none (docs/specs/tracekeep/parking.md *Parking*).
const DefaultParkedStream = "TRACEKEEP_PARKED"

// DefaultParkPrefix is the parking stream's subject prefix when a deployment names none.
// Its first token differs from every traffic prefix's: ValidatePrefixes refuses a pair that shares one.
const DefaultParkPrefix = "tkpark"

// DuplicateWindow is the traffic stream's duplicate window, the server's default of two minutes,
// within which a publish with a stored Nats-Msg-Id is answered as a duplicate (docs/specs/tracekeep/parking.md).
const DuplicateWindow = 2 * time.Minute

// StreamConfig returns the traffic stream's settings (the tracepack storage specification §4, *Stream settings*):
// work-queue retention, file storage, s2 compression and no age, size or count limit,
// so an accepted message leaves the stream only by acknowledgement;
// no mirror, no sources and AllowDirect off, so every read is the stream leader's own (tracekeep-decisions.md T-12).
//
// Parameters:
//   - name: the stream's name
//   - subjectPrefix: the deployment's subject prefix; the stream takes every subject below it
//
// Returns:
//   - jetstream.StreamConfig: the settings tracekeep migrate creates the stream with and Preflight checks
func StreamConfig(name, subjectPrefix string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              name,
		Subjects:          []string{subjectPrefix + ".>"},
		Retention:         jetstream.WorkQueuePolicy,
		Storage:           jetstream.FileStorage,
		Compression:       jetstream.S2Compression,
		MaxAge:            0,
		MaxBytes:          -1,
		MaxMsgs:           -1,
		MaxMsgsPerSubject: -1,
		Duplicates:        DuplicateWindow,
		AllowDirect:       false,
	}
}

// ObjectStoreConfig returns the claim bucket's settings: file storage and no age or size limit,
// since an object lives until retention evicts its record's hour (docs/specs/tracekeep/producer.md §4.2 *Large records*).
// nats.go creates the bucket's stream with AllowDirect on (v1.55.0 jetstream/object.go),
// so tracekeep migrate turns it off on ClaimStreamName's stream after creating the bucket,
// and Preflight refuses a bucket whose stream has it on (tracekeep-decisions.md T-28).
//
// Parameters:
//   - bucket: the claim bucket's name
//
// Returns:
//   - jetstream.ObjectStoreConfig: the settings tracekeep migrate creates the bucket with
func ObjectStoreConfig(bucket string) jetstream.ObjectStoreConfig {
	return jetstream.ObjectStoreConfig{
		Bucket:   bucket,
		Storage:  jetstream.FileStorage,
		TTL:      0,
		MaxBytes: -1,
	}
}

// ClaimStreamName returns the name of the stream that holds a claim bucket,
// "OBJ_<bucket>" as nats.go names it (v1.55.0 jetstream/object.go).
func ClaimStreamName(bucket string) string {
	return "OBJ_" + bucket
}

// ParkedStreamConfig returns the parking stream's settings (docs/specs/tracekeep/parking.md *Parking*):
// the traffic stream's settings on the park prefix's subjects, with at least the traffic stream's replicas,
// so a parked copy leaves the stream only by acknowledgement, as its original would have.
//
// Parameters:
//   - name: the stream's name
//   - parkPrefix: the deployment's park prefix; the stream takes every subject below it
//   - replicas: the traffic stream's replica count
//
// Returns:
//   - jetstream.StreamConfig: the settings tracekeep migrate creates the stream with
func ParkedStreamConfig(name, parkPrefix string, replicas int) jetstream.StreamConfig {
	c := StreamConfig(name, parkPrefix)
	c.Replicas = replicas

	return c
}

// ValidatePrefixes reports a traffic prefix and a park prefix whose first tokens are equal:
// a different stream name alone separates nothing, so the two streams' subjects must differ from the first token on.
func ValidatePrefixes(trafficPrefix, parkPrefix string) error {
	t, p := firstToken(trafficPrefix), firstToken(parkPrefix)
	if t == "" || p == "" || t == p || isWildcard(t) || isWildcard(p) {
		return fmt.Errorf("busconfig: traffic prefix %q and park prefix %q need two distinct first tokens", trafficPrefix, parkPrefix)
	}

	return nil
}

// SubjectsOverlap reports whether a subject filter of a could match a subject of b's filters, or the other way around:
// two streams whose filters overlap would take each other's messages.
func SubjectsOverlap(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if filtersOverlap(strings.Split(x, "."), strings.Split(y, ".")) {
				return true
			}
		}
	}

	return false
}

// filtersOverlap reports whether some subject matches both filters, token by token.
func filtersOverlap(x, y []string) bool {
	for i := 0; ; i++ {
		switch {
		case i < len(x) && x[i] == ">", i < len(y) && y[i] == ">":
			// ">" takes one token or more: the other filter must still have one left.
			return i < len(x) && i < len(y)
		case i == len(x) || i == len(y):
			return len(x) == len(y)
		case x[i] != y[i] && x[i] != "*" && y[i] != "*":
			return false
		}
	}
}

func firstToken(prefix string) string {
	t, _, _ := strings.Cut(prefix, ".")

	return t
}

func isWildcard(token string) bool { return token == "*" || token == ">" }
