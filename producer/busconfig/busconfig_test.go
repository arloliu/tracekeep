package busconfig_test

import (
	"reflect"
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

func TestParkedStreamConfig(t *testing.T) {
	t.Parallel()

	c := busconfig.ParkedStreamConfig(busconfig.DefaultParkedStream, busconfig.DefaultParkPrefix, 3)
	traffic := busconfig.StreamConfig(busconfig.DefaultParkedStream, busconfig.DefaultParkPrefix)
	traffic.Replicas = 3
	if !reflect.DeepEqual(c, traffic) {
		t.Fatalf("got %+v, want the traffic stream's settings on the park prefix with 3 replicas", c)
	}
}

func TestValidatePrefixes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		traffic, park string
		ok            bool
	}{
		{"tpk", busconfig.DefaultParkPrefix, true},
		{"tpk.site", "tkpark.site", true},
		{"tk.traffic", "tk.park", false}, // one first token: the subjects separate only below it
		{"tpk", "tpk", false},
		{"*", "tkpark", false},
		{"", "tkpark", false},
	} {
		if err := busconfig.ValidatePrefixes(tc.traffic, tc.park); (err == nil) != tc.ok {
			t.Errorf("ValidatePrefixes(%q, %q) = %v, want ok %v", tc.traffic, tc.park, err, tc.ok)
		}
	}
}

func TestSubjectsOverlap(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"tpk.>", "tkpark.>", false},
		{"tpk.>", "tpk.t.c", true},
		{">", "tkpark.>", true},
		{"*.>", "tkpark.>", true},
		{"tpk.*", "tpk.a.b", false},
		{"tpk.*", "tpk.>", true},
		{"tpk", "tpk.>", false}, // ">" needs one token more
		{"tpk.a", "tpk.b", false},
		{"tpk.*.c", "tpk.b.*", true},
	} {
		if got := busconfig.SubjectsOverlap([]string{tc.a}, []string{tc.b}); got != tc.want {
			t.Errorf("SubjectsOverlap(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
		if got := busconfig.SubjectsOverlap([]string{tc.b}, []string{tc.a}); got != tc.want {
			t.Errorf("SubjectsOverlap(%q, %q) = %v, want %v", tc.b, tc.a, got, tc.want)
		}
	}
}
