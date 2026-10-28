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
		a.PublicURL != "https://tracekeep.example.invalid:8443" {
		t.Errorf("api = %+v", a)
	}
	for name, tc := range map[string]struct{ extra, want string }{
		"a cert, no key":             {"api:\n  tls_cert_file: /c\n", "api.tls_key_file"},
		"a floor above the life":     {"api:\n  download_floor: 15m\n", "api.download_floor"},
		"a plain-text URL":           {"api:\n  public_url: http://tracekeep.example.invalid\n", "api.public_url"},
		"a retention past a century": {"retention: 876001h\n", "Retention"},
		"a delay past a century":     {"compact:\n  delete_delay: 876001h\n", "DeleteDelay"},
	} {
		if _, err := Load(write(t, minimal+tc.extra)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Load = %v, want an error naming %s", name, err, tc.want)
		}
	}
}
