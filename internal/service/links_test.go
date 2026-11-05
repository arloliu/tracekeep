package service

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/testbus"
)

// lines is a log whose every line, one JSON record per write, is sent on its channel.
type lines chan string

func (l lines) Write(p []byte) (int, error) {
	l <- string(p)
	return len(p), nil
}

func (l lines) logger() *slog.Logger { return slog.New(slog.NewJSONHandler(l, nil)) }

// next waits for the next line holding msg, returning it and the lines before it.
func (l lines) next(t *testing.T, msg string) (string, []string) {
	t.Helper()
	var before []string
	for {
		select {
		case line := <-l:
			if strings.Contains(line, msg) {
				return line, before
			}
			before = append(before, line)
		case <-time.After(30 * time.Second): // bounds a hung run only
			t.Fatalf("no line %q; saw %q", msg, before)
		}
	}
}

// drain returns every line already written.
func (l lines) drain() []string {
	var out []string
	for {
		select {
		case line := <-l:
			out = append(out, line)
		default:
			return out
		}
	}
}

// absent fails when a line holds any of the sentinels.
func absent(t *testing.T, got []string, sentinels ...string) {
	t.Helper()
	for _, line := range got {
		for _, s := range sentinels {
			if strings.Contains(line, s) {
				t.Errorf("a log line holds %q: %s", s, line)
			}
		}
	}
}

// The bus connection logs its loss, its restoration and an asynchronous error, each naming its server by scheme and host:
// a password, a token as user information and a query in the server's URL, and the configured password, appear in no line.
func TestBusEventsLogged(t *testing.T) {
	t.Parallel()
	for name, form := range map[string]string{
		"password": "nats://tk:url-pw-sentinel@%s",
		"token":    "nats://token-sentinel@%s",
		"query":    "nats://%s?sig=query-sentinel",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := testbus.StartServer(t, testbus.Options{})
			host := strings.TrimPrefix(srv.URL(), "nats://")
			log := make(lines, 256)
			b := &config.Bus{URL: strings.Replace(form, "%s", host, 1), User: "tk", Password: "config-pw-sentinel"}
			nc, err := connect(b, log.logger())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			var seen []string
			srv.Stop()
			lost, before := log.next(t, "bus: connection lost")
			seen = append(append(seen, before...), lost)
			srv.Restart()
			restored, before := log.next(t, "bus: connection restored")
			seen = append(append(seen, before...), restored)
			for _, line := range []string{lost, restored} {
				if !strings.Contains(line, `"server":"nats://`+host+`"`) {
					t.Errorf("line %s does not name the server nats://%s", line, host)
				}
			}
			// A subscription that holds one message at most overflows: a slow consumer's asynchronous error.
			sub, err := nc.SubscribeSync("slow")
			if err != nil {
				t.Fatal(err)
			}
			if err := sub.SetPendingLimits(1, -1); err != nil {
				t.Fatal(err)
			}
			for range 8 {
				if err := nc.Publish("slow", []byte("x")); err != nil {
					t.Fatal(err)
				}
			}
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			slow, before := log.next(t, "bus: connection error")
			seen = append(append(seen, before...), slow)
			if !strings.Contains(slow, nats.ErrSlowConsumer.Error()) {
				t.Errorf("the async error line %s does not name the slow consumer", slow)
			}
			absent(t, append(seen, log.drain()...), "url-pw-sentinel", "token-sentinel", "query-sentinel", "config-pw-sentinel")
		})
	}
}

// A close with an error is logged as an error, without the configured password.
func TestBusClosedLogged(t *testing.T) {
	t.Parallel()
	srv := testbus.StartServer(t, testbus.Options{})
	log := make(lines, 256)
	b := &config.Bus{URL: srv.URL(), User: "tk", Password: "config-pw-sentinel"}
	nc, err := connect(b, log.logger())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	// The server returns requiring another account: the reconnection is refused until the client gives up, closing with that error.
	srv.Stop()
	srv.RestartWith(testbus.Options{User: "other", Password: "other-pw"})
	closed, before := log.next(t, "bus: connection closed")
	if !strings.Contains(closed, `"level":"ERROR"`) {
		t.Errorf("the closed line %s is not an error", closed)
	}
	absent(t, append(before, closed), "config-pw-sentinel", "other-pw")
}

// A bus error's cause in the loss, asynchronous-error and close lines goes through safetext.Text:
// its words and the URL's scheme and host stay, and no credential or query of a URL it quotes reaches any field.
func TestBusErrorCausesLoggedSafe(t *testing.T) {
	t.Parallel()
	log := make(lines, 8)
	b := &busEvents{log: log.logger()}
	var o nats.Options
	for _, opt := range b.options() {
		if err := opt(&o); err != nil {
			t.Fatal(err)
		}
	}
	cause := errors.New("dial nats://u:pw-sentinel@bus.invalid:4222/?token=q-sentinel: cause-words")
	o.DisconnectedErrCB(nil, cause)
	o.AsyncErrorCB(nil, nil, cause)
	b.closed(cause)
	for _, msg := range []string{"bus: connection lost", "bus: connection error", "bus: connection closed"} {
		line, before := log.next(t, msg)
		if len(before) != 0 || !strings.Contains(line, "dial nats://bus.invalid:4222: cause-words") {
			t.Errorf("%s: line %s after %q, want it next, naming the cause by its words and its URL's host", msg, line, before)
		}
		absent(t, []string{line}, "pw-sentinel", "q-sentinel")
	}
}

// A bus URL that does not parse fails the connection before any dial, its error shown by its structure:
// the operation, the scheme alone and the cause without the URL's fragments it quotes, or a fixed diagnostic for a scheme not the bus's;
// no credential or query appears, and the client's *url.Error is still found in the chain.
func TestConnectFailureShownByStructure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ url, want string }{
		{"nats://u: pw-sentinel@bus.invalid:bad?token=q-sentinel", `service: connect the bus: parse a nats URL: invalid port "…" after host`},
		{"nats://user:pw-sentinel", `service: connect the bus: parse a nats URL: invalid port "…" after host`},
		{"nats://a.invalid,TLS://u: pw-sentinel@b.invalid?q-sentinel", "service: connect the bus: parse a tls URL: net/url: invalid userinfo"},
		{"ht tp://u:pw-sentinel@a.invalid b?q-sentinel", "bus: connect: parse failed on a URL not shown"},
	} {
		_, err := connect(&config.Bus{URL: tc.url}, slog.New(slog.DiscardHandler))
		var ue *url.Error
		if err == nil || err.Error() != tc.want || !errors.As(err, &ue) {
			t.Errorf("connect(%q) = %v, want %q with the client's *url.Error kept", tc.url, err, tc.want)
		}
		if err != nil && (strings.Contains(err.Error(), "pw-sentinel") || strings.Contains(err.Error(), "q-sentinel")) {
			t.Errorf("connect(%q) = %v, holding a credential or a query", tc.url, err)
		}
	}
}

// tlsBus is an embedded bus over TLS, its certificate a self-signed one for 127.0.0.1, and that certificate's file.
func tlsBus(t *testing.T) (*testbus.Server, string) {
	t.Helper()
	dir := t.TempDir()
	cert, key := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := devCertificate(cert, key, time.Now()); err != nil {
		t.Fatal(err)
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}

	return testbus.StartServer(t, testbus.Options{TLS: &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}}), cert
}

// A role's start warns once per link without TLS, outside dev: the bus by its connection's TLS state, so a nats:// URL
// the server upgrades is not warned, and the S3 store by an http:// endpoint, an empty or an https one not;
// no credential or query of either URL, nor a configured secret, reaches the log.
func TestLinksWithoutTLSWarned(t *testing.T) {
	t.Parallel()
	plain := testbus.StartServer(t, testbus.Options{})
	secure, ca := tlsBus(t)
	host := strings.TrimPrefix(secure.URL(), "tls://")
	for _, tc := range []struct {
		name, url, ca, endpoint string
		dev                     bool
		want                    []string
	}{
		{name: "plain bus", url: "nats://tk:url-pw-sentinel@" + strings.TrimPrefix(plain.URL(), "nats://") + "?sig=query-sentinel",
			want: []string{`"server":"` + plain.URL() + `"`}},
		{name: "upgraded nats://", url: "nats://" + host, ca: ca},
		{name: "tls://", url: secure.URL(), ca: ca},
		{name: "http endpoint", url: secure.URL(), ca: ca, endpoint: "http://ak:s3-pw-sentinel@s3.example.invalid:9000/?X-Amz-Signature=sig-sentinel",
			want: []string{`"endpoint":"http://s3.example.invalid:9000"`}},
		{name: "https endpoint", url: secure.URL(), ca: ca, endpoint: "https://s3.example.invalid"},
		{name: "empty endpoint", url: secure.URL(), ca: ca},
		{name: "dev", url: plain.URL(), endpoint: "http://s3.example.invalid:9000", dev: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log := make(lines, 64)
			nc, err := connect(&config.Bus{URL: tc.url, CAFile: tc.ca, User: "tk", Password: "config-pw-sentinel"}, log.logger())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			cfg := &config.Config{Store: config.Store{Kind: "s3", S3: config.S3{Endpoint: tc.endpoint, SecretAccessKey: "secret-sentinel"}}}
			e := &Env{Config: cfg, Log: log.logger(), NC: nc, Dev: tc.dev}
			e.warnPlain()
			e.warnPlain()
			var warned []string
			for _, line := range log.drain() {
				if strings.Contains(line, "without TLS") {
					warned = append(warned, line)
				}
			}
			if len(warned) != len(tc.want) {
				t.Fatalf("warned %q, want one line for each of %q", warned, tc.want)
			}
			for i, w := range tc.want {
				if !strings.Contains(warned[i], w) || !strings.Contains(warned[i], `"level":"WARN"`) {
					t.Errorf("line %s, want a warning naming %s", warned[i], w)
				}
			}
			absent(t, warned, "url-pw-sentinel", "query-sentinel", "config-pw-sentinel", "s3-pw-sentinel", "sig-sentinel", "secret-sentinel")
		})
	}
}

// all warns once for its roles, each of which judges the links at its start.
func TestAllWarnsOnce(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		e := env(t, c)
		log := make(lines, 256)
		e.Log, e.Dev = log.logger(), false
		e.Config.Store.Kind, e.Config.Store.S3.Endpoint = "s3", "http://s3.example.invalid:9000"
		// Every role starts and ends at once, its context ended before it runs.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_ = All(ctx, e)
		var bus, st int
		for _, line := range log.drain() {
			switch {
			case strings.Contains(line, "the bus connection is without TLS"):
				bus++
			case strings.Contains(line, "the store endpoint is without TLS"):
				st++
			default: // another line
			}
		}
		if bus != 1 || st != 1 {
			t.Errorf("all warned %d times of the bus and %d of the store, want once each", bus, st)
		}
	})
}

// The dev IdP logs why its serving ended, unless it was shut down.
func TestServeIDPLogsItsEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cert, key := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := devCertificate(cert, key, time.Now()); err != nil {
		t.Fatal(err)
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	server := func() *http.Server {
		return &http.Server{TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}, ReadHeaderTimeout: time.Second}
	}
	log := make(lines, 8)
	serveIDP(server(), failingListener{}, log.logger())
	if got := log.drain(); len(got) != 1 || !strings.Contains(got[0], "accept-sentinel") || !strings.Contains(got[0], `"level":"ERROR"`) {
		t.Errorf("a failed accept logged %q, want one error line with its cause", got)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server()
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveIDP(srv, ln, log.logger())
	}()
	if err := srv.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-done
	if got := log.drain(); len(got) != 0 {
		t.Errorf("a clean stop logged %q, want nothing", got)
	}
}

// failingListener's Accept fails for good.
type failingListener struct{}

func (failingListener) Accept() (net.Conn, error) { return nil, errors.New("accept-sentinel") }
func (failingListener) Close() error              { return nil }
func (failingListener) Addr() net.Addr            { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

// failingClose is a store whose close fails with an error naming an object's key.
type failingClose struct{ store.ObjectStore }

func (failingClose) Close() error {
	return errors.New("store: close packs/T1/key-sentinel.tpk: backend-sentinel")
}

// The store's close error is made keyless before Close joins it; a clean close joins none.
func TestCloseStoreKeyless(t *testing.T) {
	t.Parallel()
	err := (&Env{Store: failingClose{store.NewMem()}}).Close()
	if err == nil || strings.Contains(err.Error(), "key-sentinel") || strings.Contains(err.Error(), "backend-sentinel") {
		t.Errorf("Close = %v, want a keyless store failure", err)
	}
	dir := t.TempDir()
	fs, err := store.NewFS(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&Env{Store: fs}).Close(); err != nil {
		t.Errorf("Close of a clean store = %v", err)
	}
}
