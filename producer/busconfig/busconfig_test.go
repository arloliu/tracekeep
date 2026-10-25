package busconfig_test

import (
	"slices"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/arloliu/tracekeep/producer/busconfig"
)

func TestStreamConfig(t *testing.T) {
	t.Parallel()

	c := busconfig.StreamConfig("TRAFFIC_A", "tk.a")
	if c.Name != "TRAFFIC_A" || !slices.Equal(c.Subjects, []string{"tk.a.>"}) {
		t.Fatalf("name %q subjects %q, want TRAFFIC_A [tk.a.>]", c.Name, c.Subjects)
	}
	if c.Retention != jetstream.WorkQueuePolicy || c.Storage != jetstream.FileStorage || c.Compression != jetstream.S2Compression {
		t.Fatalf("retention %v storage %v compression %v, want work-queue, file, s2", c.Retention, c.Storage, c.Compression)
	}
	if c.MaxAge != 0 || c.MaxBytes != -1 || c.MaxMsgs != -1 || c.MaxMsgsPerSubject != -1 {
		t.Fatalf("limits age %v bytes %d msgs %d per-subject %d, want none", c.MaxAge, c.MaxBytes, c.MaxMsgs, c.MaxMsgsPerSubject)
	}
	if c.Duplicates != busconfig.DuplicateWindow || c.AllowDirect || c.Mirror != nil || c.Sources != nil {
		t.Fatalf("duplicates %v allow-direct %v mirror %v sources %v", c.Duplicates, c.AllowDirect, c.Mirror, c.Sources)
	}
}

func TestObjectStoreConfig(t *testing.T) {
	t.Parallel()

	c := busconfig.ObjectStoreConfig("claims")
	if c.Bucket != "claims" || c.Storage != jetstream.FileStorage || c.TTL != 0 || c.MaxBytes != -1 {
		t.Fatalf("got %+v, want bucket claims, file storage, no limit", c)
	}
	if got := busconfig.ClaimStreamName("claims"); got != "OBJ_claims" {
		t.Fatalf("claim stream %q, want OBJ_claims", got)
	}
}
