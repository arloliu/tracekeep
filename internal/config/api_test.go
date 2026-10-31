package config

import (
	"strings"
	"testing"
	"time"
)

func TestAPIKeys(t *testing.T) {
	t.Parallel()
	c, err := Load(write(t, minimal+"api:\n  tls_cert_file: /etc/tracekeep/api.crt\n  tls_key_file: /etc/tracekeep/api.key\n  public_url: https://tracekeep.example.invalid:8443\n"))
	if err != nil {
		t.Fatal(err)
	}
	if a := c.API; a.Listen != ":8443" || a.DownloadMargin != 2*time.Minute || a.DownloadFloor != time.Minute || a.Plaintext ||
		a.DigestRate != 200 || a.DigestBurst != 2000 ||
		a.Records != (Records{MaxReads: 8, MaxReaders: 16, MaxLookupPacks: 64, HeldBytes: 64 << 20}) ||
		a.PublicURL != "https://tracekeep.example.invalid:8443" {
		t.Errorf("api = %+v", a)
	}
	for name, tc := range map[string]struct{ extra, want string }{
		"a cert, no key":             {"api:\n  tls_cert_file: /c\n", "api.tls_key_file"},
		"a floor above the life":     {"api:\n  download_floor: 15m\n", "api.download_floor"},
		"a plain-text URL":           {"api:\n  public_url: http://tracekeep.example.invalid\n", "api.public_url"},
		"a retention past a century": {"retention: 876001h\n", "Retention"},
		"a delay past a century":     {"compact:\n  delete_delay: 876001h\n", "DeleteDelay"},
		"a lifetime past a day":      {"compact:\n  delete_delay: 26h\n", "DownloadLifetime"},
		"a margin past a day":        {"api:\n  download_margin: 25h\n", "DownloadMargin"},
		"no digest rate":             {"api:\n  digest_rate: 0\n", "API.DigestRate"},
		"a burst below one request":  {"api:\n  digest_burst: 999\n", "API.DigestBurst"},
		"no record reads":            {"api:\n  records:\n    max_reads: 0\n", "MaxReads"},
		"held bytes below a MiB":     {"api:\n  records:\n    held_bytes: 1024\n", "HeldBytes"},
	} {
		base := minimal
		if name == "a lifetime past a day" {
			base = yaml("", "", "  s3:\n    bucket: packs\n    access_key_id: ak\n    secret_access_key: sk\n  download_lifetime: 25h\n", "")
		}
		if _, err := Load(write(t, base+tc.extra)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Load = %v, want an error naming %s", name, err, tc.want)
		}
	}
}
