package config

import (
	"cmp"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// yaml is a configuration holding the keys no default supplies, catalog, bus and store replaced when given, and extra appended.
func yaml(catalog, bus, store, extra string) string {
	return "catalog:\n" + cmp.Or(catalog, "  dsn: /var/lib/tracekeep/catalog.db\n") +
		"bus:\n" + cmp.Or(bus, "  url: nats://bus:4222\n  subject_prefix: tk\n  claim_bucket: claims\n") +
		"store:\n" + cmp.Or(store, "  s3:\n    bucket: packs\n    access_key_id: ak\n    secret_access_key: sk\n") + extra
}

var minimal = yaml("", "", "", "")

func write(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tracekeep.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

// The defaults are the service defaults (T-12, T-25) and the plan's two.
func TestDefaults(t *testing.T) {
	t.Parallel()
	c, err := Load(write(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]time.Duration{
		"retention": c.Retention, "ingest.flush_interval": c.Ingest.FlushInterval, "ingest.ack_wait": c.Ingest.AckWait,
		"parking.redelivery_delay": c.Parking.RedeliveryDelay, "ingest.lease": c.Ingest.Lease,
		"compact.quiet_period": c.Compact.QuietPeriod, "compact.redelivery_slack": c.Compact.RedeliverySlack,
		"compact.inactivity": c.Compact.Inactivity, "compact.delete_delay": c.Compact.DeleteDelay,
		"store.download_lifetime": c.Store.DownloadLifetime,
	} {
		want := map[string]time.Duration{
			"retention": 30 * 24 * time.Hour, "ingest.flush_interval": 5 * time.Minute, "ingest.ack_wait": 7 * time.Minute,
			"parking.redelivery_delay": time.Minute, "ingest.lease": time.Minute,
			"compact.quiet_period": 15 * time.Minute, "compact.redelivery_slack": 2 * time.Minute,
			"compact.inactivity": 2 * time.Hour, "compact.delete_delay": time.Hour,
			"store.download_lifetime": 15 * time.Minute,
		}[name]
		if got != want {
			t.Errorf("%s = %s, want %s", name, got, want)
		}
	}
	if c.Catalog.Engine != "sqlite" || c.Store.Kind != "s3" || !c.Store.Presign || !c.Parking.Enabled || c.Ingest.BatchMaxBytes.Int64() != 64<<20 ||
		c.Metrics.Listen != ":9090" {
		t.Errorf("config %+v, want the defaults", c)
	}
	if host, _ := os.Hostname(); c.Replica != host {
		t.Errorf("replica %q, want the host name %q", c.Replica, host)
	}
}

// A secret is read through its reference, never written in the file.
func TestSecretByReference(t *testing.T) {
	t.Parallel()
	secret := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(secret, []byte("user:pw@tcp(db:3306)/catalog"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(write(t, yaml("  engine: mariadb\n  dsn_ref: file://"+secret+"\n", "", "", "")))
	if err != nil {
		t.Fatal(err)
	}
	if c.Catalog.DSN != "user:pw@tcp(db:3306)/catalog" {
		t.Errorf("DSN %q, want the referenced file's", c.Catalog.DSN)
	}
}

// An environment variable overrides the file.
func TestEnvironmentOverrides(t *testing.T) {
	t.Setenv("TRACEKEEP_INGEST_FLUSH_INTERVAL", "2m")
	t.Setenv("TRACEKEEP_PARKING_ENABLED", "false")
	c, err := Load(write(t, minimal+"ingest:\n  flush_interval: 4m\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Ingest.FlushInterval != 2*time.Minute || c.Parking.Enabled {
		t.Errorf("flush interval %s, parking %v; want the environment's", c.Ingest.FlushInterval, c.Parking.Enabled)
	}
}

// Every rule refuses the configuration that breaks it, naming the key.
func TestValidation(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct{ yaml, want string }{
		"no bus url":          {yaml("", "  subject_prefix: tk\n  claim_bucket: claims\n", "", ""), "URL"},
		"unknown engine":      {yaml("  engine: postgres\n  dsn: x\n", "", "", ""), "Engine"},
		"ack wait below F":    {yaml("", "", "", "ingest:\n  flush_interval: 5m\n  ack_wait: 5m\n"), "ingest.ack_wait"},
		"fs store, no dir":    {yaml("", "", "  kind: fs\n", ""), "store.dir"},
		"s3 store, no bucket": {yaml("", "", "  kind: s3\n", ""), "store.s3.bucket"},
		"two credentials":     {yaml("", "  url: nats://bus:4222\n  subject_prefix: tk\n  claim_bucket: claims\n  user: u\n  creds_file: /c\n", "", ""), "exclusive"},
		"cert, no key":        {yaml("", "  url: nats://bus:4222\n  subject_prefix: tk\n  claim_bucket: claims\n  cert_file: /c\n", "", ""), "bus.key_file"},
		"key, no cert":        {yaml("", "  url: nats://bus:4222\n  subject_prefix: tk\n  claim_bucket: claims\n  key_file: /k\n", "", ""), "bus.cert_file"},
		"s3, no access key":   {yaml("", "", "  s3:\n    bucket: packs\n    secret_access_key: sk\n", ""), "store.s3.access_key_id"},
		"s3, no secret":       {yaml("", "", "  s3:\n    bucket: packs\n    access_key_id: ak\n", ""), "store.s3.secret_access_key"},
	} {
		if _, err := Load(write(t, tc.yaml)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Load = %v, want an error naming %s", name, err, tc.want)
		}
	}
}

// Q must exceed F + ack_wait + the redelivery slack strictly (catalog.md §7.3, T-25, T-41).
func TestQuietPeriodBound(t *testing.T) {
	t.Parallel()
	timing := func(q string) string {
		return yaml("", "", "", "ingest:\n  flush_interval: 5m\n  ack_wait: 7m\ncompact:\n  redelivery_slack: 2m\n  quiet_period: "+q+"\n")
	}
	for q, ok := range map[string]bool{"13m59s": false, "14m": false, "14m1s": true, "15m": true} {
		_, err := Load(write(t, timing(q)))
		if ok && err != nil {
			t.Errorf("Q %s: Load = %v, want it accepted", q, err)
		}
		if !ok && (err == nil || !strings.Contains(err.Error(), "compact.quiet_period")) {
			t.Errorf("Q %s: Load = %v, want an error naming compact.quiet_period", q, err)
		}
	}
	// Raising F or ack_wait raises the bound.
	if _, err := Load(write(t, yaml("", "", "", "ingest:\n  flush_interval: 6m\n  ack_wait: 7m\n"))); err == nil {
		t.Error("the default Q under a 6-minute F = nil, want refused")
	}
}

// D must exceed the download URL's lifetime by more than two minutes of clock skew (compactor.md §8.3).
func TestDeleteDelayBound(t *testing.T) {
	t.Parallel()
	timing := func(d string) string {
		return yaml("", "", "  s3:\n    bucket: packs\n    access_key_id: ak\n    secret_access_key: sk\n  download_lifetime: 15m\n", "compact:\n  delete_delay: "+d+"\n")
	}
	for d, ok := range map[string]bool{"15m1s": false, "17m": false, "17m1s": true, "1h": true} {
		_, err := Load(write(t, timing(d)))
		if ok && err != nil {
			t.Errorf("D %s: Load = %v, want it accepted", d, err)
		}
		if !ok && (err == nil || !strings.Contains(err.Error(), "compact.delete_delay")) {
			t.Errorf("D %s: Load = %v, want an error naming compact.delete_delay", d, err)
		}
	}
}

// dev's configuration is valid, its stores under the given directory.
func TestDev(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c, err := Dev(dir, "nats://127.0.0.1:4222", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, flush := range []time.Duration{time.Second, 10 * time.Second, 5 * time.Minute} {
		d, err := Dev(dir, "nats://127.0.0.1:4222", flush)
		if err != nil {
			t.Errorf("Dev with flush %s = %v", flush, err)
			continue
		}
		// dev derives Q a minute above its bound.
		if want := d.Ingest.FlushInterval + d.Ingest.AckWait + d.Compact.RedeliverySlack + time.Minute; d.Compact.QuietPeriod != want {
			t.Errorf("dev's Q with flush %s = %s, want %s", flush, d.Compact.QuietPeriod, want)
		}
	}
	if c.Catalog.DSN != filepath.Join(dir, "catalog.db") || c.Store.Dir != filepath.Join(dir, "packs") || c.Ingest.FlushInterval != 2*time.Second ||
		c.Ingest.AckWait <= c.Ingest.FlushInterval || !c.Parking.Enabled || c.Retention != 30*24*time.Hour {
		t.Errorf("dev configuration %+v", c)
	}
}
