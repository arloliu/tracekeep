package service

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	connectrpc "connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// A person signs in under tracekeep dev by its test IdP, as in production: the login redirects to the IdP's page,
// whose form signs the chosen subject and roles in and returns the code to the callback, which sets the session;
// the session then reads the browser's operations under the Origin check.
func TestDevSignsPeopleIn(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	d, err := NewDev(t.Context(), DevOptions{Dir: dir, Flush: time.Second, API: "127.0.0.1:0", IDP: "localhost:0", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	bound := make(chan struct{})
	d.Env.apiBound = func() { close(bound) }
	run, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- d.Run(run) }()
	t.Cleanup(func() {
		stop()
		<-done //nolint:errcheck // the run's end, its error the cancellation's
	})
	<-bound
	public := d.Env.Config.API.PublicURL
	if issuer := d.Env.Config.Auth.OIDC.Issuer; !strings.HasPrefix(issuer, "https://localhost:") || strings.HasSuffix(issuer, ":0") {
		t.Fatalf("the issuer is %q, want the IdP's bound port on the host given", issuer)
	}
	browser := trusting(t, filepath.Join(dir, DevCertFile))
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	browser.Jar = jar
	browser.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	get := func(u string) *http.Response {
		t.Helper()
		resp, err := browser.Get(u) //nolint:noctx // a test's browser
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })

		return resp
	}

	login := get(public + "/auth/login?return=/records")
	authorize := login.Header.Get("Location")
	if login.StatusCode != http.StatusFound || !strings.HasPrefix(authorize, d.Env.Config.Auth.OIDC.Issuer+"/authorize?") {
		t.Fatalf("login = %d to %q, want the IdP's authorization page", login.StatusCode, authorize)
	}
	page := get(authorize)
	body := new(strings.Builder)
	if _, err := io.Copy(body, page.Body); err != nil || page.StatusCode != http.StatusOK {
		t.Fatalf("the authorization page = %d, %v", page.StatusCode, err)
	}
	form := url.Values{"subject": {"alice"}, "role": {"viewer"}}
	for _, m := range regexp.MustCompile(`type="hidden" name="([a-z_]+)" value="([^"]*)"`).FindAllStringSubmatch(body.String(), -1) {
		form.Set(m[1], html.UnescapeString(m[2]))
	}
	posted, err := browser.PostForm(authorize, form) //nolint:noctx // a test's browser
	if err != nil {
		t.Fatal(err)
	}
	_ = posted.Body.Close()
	callback := posted.Header.Get("Location")
	if posted.StatusCode != http.StatusFound || !strings.HasPrefix(callback, public+"/auth/callback?") {
		t.Fatalf("the form = %d to %q, want the callback", posted.StatusCode, callback)
	}
	signedIn := get(callback)
	if signedIn.StatusCode != http.StatusSeeOther || signedIn.Header.Get("Location") != "/records" {
		t.Fatalf("callback = %d to %q, want back to /records", signedIn.StatusCode, signedIn.Header.Get("Location"))
	}
	var me struct {
		Subject string   `json:"subject"`
		Roles   []string `json:"roles"`
	}
	if err := json.NewDecoder(get(public + "/auth/me").Body).Decode(&me); err != nil || me.Subject != "alice" || !slices.Equal(me.Roles, []string{"viewer"}) {
		t.Fatalf("me = %+v, %v; want alice with viewer", me, err)
	}

	cl := tracekeepv1connect.NewMachineServiceClient(browser, public, connectrpc.WithProtoJSON())
	now := time.Now()
	req := connectrpc.NewRequest(&tkv1.ListDevicesRequest{Selector: &tkv1.Selector{
		ToolId: "SIM-TOOL", From: timestamppb.New(now.Add(-time.Hour)), To: timestamppb.New(now),
	}})
	req.Header().Set("Origin", public)
	if _, err := cl.ListDevices(t.Context(), req); err != nil {
		t.Errorf("ListDevices with the session = %v", err)
	}
	// The code was spent: the callback again finds its login cookie cleared and is refused.
	if again := get(callback); again.StatusCode != http.StatusBadRequest {
		t.Errorf("the callback again = %d, want refused", again.StatusCode)
	}
}

// Dev serves and names only localhost, 127.0.0.1 and ::1, the names its certificate holds:
// its test IdP signs in whoever asks.
func TestDevServesOnlyLoopback(t *testing.T) {
	t.Parallel()
	for name, o := range map[string]DevOptions{
		"the API on every address": {API: "0.0.0.0:0", IDP: "127.0.0.1:0"},
		"the IdP on another host":  {API: "127.0.0.1:0", IDP: "127.0.0.2:0"},
		"a public URL elsewhere":   {API: "127.0.0.1:0", IDP: "127.0.0.1:0", PublicURL: "https://tk.example.invalid"},
		"a plain-http public URL":  {API: "127.0.0.1:0", IDP: "127.0.0.1:0", PublicURL: "http://localhost:5173"},
	} {
		o.Dir, o.Flush, o.Log = t.TempDir(), time.Second, slog.New(slog.DiscardHandler)
		d, err := NewDev(t.Context(), o)
		if err == nil {
			_ = d.Close()
			t.Errorf("%s: NewDev started, want refused", name)
		} else if !strings.Contains(err.Error(), "localhost, 127.0.0.1 or ::1") {
			t.Errorf("%s: NewDev = %v, want refused naming the hosts", name, err)
		}
	}
}

// Dev's certificate is kept across runs while valid, and made again once it has expired.
func TestDevCertificateRenewedWhenExpired(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cert, key := filepath.Join(dir, DevCertFile), filepath.Join(dir, devCertKey)
	now := time.Now()
	if err := devCertificate(cert, key, now); err != nil {
		t.Fatal(err)
	}
	first, _ := os.ReadFile(cert)
	if fi, err := os.Stat(key); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the key file = %v, %v; want mode 0600", fi, err)
	}
	if err := devCertificate(cert, key, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if kept, _ := os.ReadFile(cert); string(kept) != string(first) {
		t.Error("a valid certificate was made again")
	}
	if err := devCertificate(cert, key, now.Add(devCertLife+time.Hour)); err != nil {
		t.Fatal(err)
	}
	if renewed, _ := os.ReadFile(cert); string(renewed) == string(first) {
		t.Error("an expired certificate was kept")
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range devHosts {
		if err := pair.Leaf.VerifyHostname(host); err != nil {
			t.Errorf("the certificate does not hold %s: %v", host, err)
		}
	}
}

// auth.oidc.ca_file that cannot be read or holds no certificate fails the start, and none takes the default client.
func TestIdPClientNeedsItsCAFile(t *testing.T) {
	t.Parallel()
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("no certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{empty, filepath.Join(t.TempDir(), "missing.pem")} {
		if _, err := idpClient(f); err == nil || !strings.Contains(err.Error(), "auth.oidc.ca_file") {
			t.Errorf("idpClient(%s) = %v, want refused naming auth.oidc.ca_file", filepath.Base(f), err)
		}
	}
	if c, err := idpClient(""); err != nil || c.Transport != nil {
		t.Errorf("idpClient without a file = %v, %v; want the default", c, err)
	}
}
