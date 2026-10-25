package busconfig

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// DefaultTrafficStream is the traffic stream's name when a deployment names none;
// every publish expects it (Nats-Expected-Stream).
const DefaultTrafficStream = "TRACEKEEP_TRAFFIC"

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
