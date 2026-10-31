package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	connectrpc "connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// trusting is a client of HTTP/2 over TLS trusting only the certificate in certFile, as dev's own is.
func trusting(t *testing.T, certFile string) *http.Client {
	t.Helper()
	b, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		t.Fatalf("%s holds no certificate", certFile)
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true}

	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

// tracekeep dev serves the machine API behind its generated token: a client lists the simulator's packs, gets a pack's URL,
// downloads it through the API and reads it with tracepack's reader.
func TestDevServesTheMachineAPI(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	d, err := NewDev(ctx, DevOptions{Dir: dir, Flush: 200 * time.Millisecond, Rate: 50, API: "127.0.0.1:0", IDP: "127.0.0.1:0", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	bound := make(chan struct{})
	d.Env.apiBound = func() { close(bound) }
	put := &putting{ObjectStore: d.Env.Store, put: make(chan string, 16)}
	d.Env.Store = put
	seedRetention(t, d.Env)
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(run) }()
	t.Cleanup(func() {
		stop()
		<-done //nolint:errcheck // the run's end, its error the cancellation's
	})
	<-bound
	token, err := os.ReadFile(filepath.Join(dir, DevTokenFile))
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(filepath.Join(dir, DevTokenFile)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the token file = %v, %v; want mode 0600", fi, err)
	}
	base := d.Env.Config.API.PublicURL
	client := trusting(t, filepath.Join(dir, DevCertFile))
	cl := tracekeepv1connect.NewMachineServiceClient(client, base)
	authed := func(r interface{ Header() http.Header }) {
		r.Header().Set("Authorization", "Bearer "+string(token))
	}
	now := time.Now()
	sel := &tkv1.Selector{ToolId: "SIM-TOOL", From: timestamppb.New(now.Add(-2 * time.Hour)), To: timestamppb.New(now.Add(time.Hour))}
	// Flushes run one after another, put, registration, acknowledgements, and a flush holds one segment per hour,
	// so of three puts the last comes from a later flush than the first, whose segment is then registered (TestDevLoopbackCapture).
	for range 3 {
		<-put.put
	}
	req := connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel})
	authed(req)
	listed, err := cl.ListPacks(ctx, req)
	if err != nil {
		t.Fatalf("ListPacks = %v", err)
	}
	pack := anyPack(listed.Msg)
	if pack == "" {
		t.Fatalf("ListPacks after three segments = %v, want the first registered", listed.Msg)
	}
	ureq := connectrpc.NewRequest(&tkv1.PackURLRequest{PackId: pack})
	authed(ureq)
	uresp, err := cl.PackURL(ctx, ureq)
	if err != nil {
		t.Fatalf("PackURL = %v", err)
	}
	u := uresp.Msg
	if u.Presigned || !strings.HasPrefix(u.Url, base+"/download/") {
		t.Fatalf("PackURL = %v, want a URL on the API", u)
	}
	resp, err := client.Get(u.Url) //nolint:noctx // a test's download
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Errorf("dev answered over %s, want HTTP/2 over TLS", resp.Proto)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("download = %d, %v", resp.StatusCode, err)
	}
	r, err := tracepack.Open(ctx, bytes.NewReader(b), int64(len(b)), tracepack.ReaderOptions{})
	if err != nil {
		t.Fatalf("the downloaded pack does not open: %v", err)
	}
	if id := r.Header().PackID; id.String() != pack {
		t.Errorf("the downloaded pack is %s, want %s", id, pack)
	}
	unauthed := connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel})
	if _, err := cl.ListPacks(ctx, unauthed); connectrpc.CodeOf(err) != connectrpc.CodeUnauthenticated {
		t.Errorf("ListPacks without the token = %v, want unauthenticated", err)
	}
}

// seedRetention records e's retention, as its compactor's first cycle would, so a pack has a deadline before the loop's compactor runs:
// under a lease of the test's own that lapses at once, which that compactor then takes.
func seedRetention(t *testing.T, e *Env) {
	t.Helper()
	// The lease outlasts a slow catalog's write and is released after it, so the service's compactor takes it at once.
	seed, ok, err := e.Catalog.TakeLease(t.Context(), "compactor/2", "test-seed", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TakeLease = %v, %v", ok, err)
	}
	if _, err := e.Catalog.RecordRetention(t.Context(), seed, e.Config.Retention, e.Config.Compact.DeleteDelay, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := e.Catalog.ReleaseLease(t.Context(), seed); err != nil {
		t.Fatal(err)
	}
}

// anyPack is the id of a pack l lists, empty for none.
func anyPack(l *tkv1.ListPacksResponse) string {
	for _, h := range l.Hours {
		for _, s := range h.Scopes {
			for _, p := range s.Packs {
				return p.PackId
			}
		}
	}

	return ""
}

// presigning is a store whose presigned GETs reach url, each signed with a synthetic signature.
type presigning struct{ url string }

const signature = "X-Amz-Signature=0123456789abcdef"

func (p presigning) PresignGet(_ context.Context, key string, until time.Time) (string, time.Time, error) {
	return p.url + "/" + key + "?" + signature, until, nil
}

// The start-time check: a presigned GET of an absent key answered not-found passes, any other answer fails the start.
func TestPresignCheck(t *testing.T) {
	t.Parallel()
	for status, ok := range map[int]bool{http.StatusNotFound: true, http.StatusForbidden: false, http.StatusOK: false} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		err := checkPresign(t.Context(), presigning{srv.URL})
		srv.Close()
		if (err == nil) != ok {
			t.Errorf("a store answering %d: check = %v", status, err)
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	err = checkPresign(t.Context(), presigning{"http://" + addr})
	if err == nil || !strings.Contains(err.Error(), "store.presign") || strings.Contains(err.Error(), signature) {
		t.Errorf("an unreachable store: check = %v, want the advice to turn presigning off and no signed URL", err)
	}
	// A redirect to a page that is not found is the store refusing the signature, not accepting it.
	notFound := httptest.NewServer(http.NotFoundHandler())
	defer notFound.Close()
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, notFound.URL+"/login", http.StatusFound)
	}))
	defer redirecting.Close()
	if err := checkPresign(t.Context(), presigning{redirecting.URL}); err == nil || strings.Contains(err.Error(), signature) {
		t.Errorf("a store redirecting to a 404: check = %v, want the start failed without the signed URL", err)
	}
	// A Location that does not parse fails inside the client, its error quoting the header: the check says only that it failed.
	malformed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/%?"+r.URL.RawQuery)
		w.WriteHeader(http.StatusFound)
	}))
	defer malformed.Close()
	if err := checkPresign(t.Context(), presigning{malformed.URL}); err == nil || strings.Contains(err.Error(), "Signature") {
		t.Errorf("a store redirecting to a malformed location: check = %v, want the start failed without the signed URL", err)
	}
	// A reason phrase quoting the request's target is the store's text: the check reports the status code alone.
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()
	go func() {
		conn, err := ln2.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		line, _ := bufio.NewReader(conn).ReadString('\n')
		target := strings.Fields(line)[1]
		_, _ = io.WriteString(conn, "HTTP/1.1 403 "+target+"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	}()
	if err := checkPresign(t.Context(), presigning{"http://" + ln2.Addr().String()}); err == nil || strings.Contains(err.Error(), "Signature") {
		t.Errorf("a store reflecting the signed target in its reason: check = %v, want the start failed without the signed URL", err)
	}
}

// Without a presigning store the api role needs a public URL and a download key, and refuses to start without them.
func TestAPIRoleNeedsDownloadKeys(t *testing.T) {
	t.Parallel()
	e := devEnv(t)
	e.Config.API.Listen = "127.0.0.1:0"
	e.Config.API.TLSCertFile, e.Config.API.TLSKeyFile = selfSigned(t)
	e.Config.Keys.Download.Issuing = ""
	err := API(t.Context(), e)
	if err == nil || !strings.Contains(err.Error(), "api.public_url") {
		t.Errorf("API without download keys = %v, want refused at start", err)
	}
}

// devEnv is a dev environment running nothing, its api on a free port.
func devEnv(t *testing.T) *Env {
	t.Helper()
	d, err := NewDev(t.Context(), DevOptions{Dir: t.TempDir(), Flush: time.Second, API: "127.0.0.1:0", IDP: "127.0.0.1:0", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })

	return d.Env
}

// selfSigned writes a self-signed certificate for 127.0.0.1 and its key, and returns their files.
func selfSigned(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tracekeep test"}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cert, keyFile := filepath.Join(dir, "api.crt"), filepath.Join(dir, "api.key")
	if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}

	return cert, keyFile
}

// clientCA issues client certificates, and writes its own certificate to a file.
type clientCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string
}

func newClientCA(t *testing.T) clientCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "test client ca"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "client-ca.pem")
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}

	return clientCA{cert: cert, key: key, file: file}
}

// generator is the identity the client certificates of these tests name.
const generator = "spiffe://cluster.example/ns/ve/sa/generator"

// issue is a client certificate naming generator.
func (c clientCA) issue(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(generator)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(8), Subject: pkix.Name{CommonName: "client"}, URIs: []*url.URL{u},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// The api role over TLS: the generated client reads with a client certificate the configuration maps to pack-reader,
// without a credential is refused, and a certificate of another authority fails the handshake.
func TestAPIRoleOverTLSWithAClientCertificate(t *testing.T) {
	t.Parallel()
	e := devEnv(t)
	ca := newClientCA(t)
	cert, key := selfSigned(t)
	c := e.Config
	c.API.Listen, c.API.TLSCertFile, c.API.TLSKeyFile = "127.0.0.1:0", cert, key
	c.API.PublicURL = "https://127.0.0.1"
	c.Auth.MTLS.ClientCAFile, c.Auth.MTLS.Subjects = ca.file, map[string][]string{generator: {"pack-reader"}}
	bound := make(chan struct{})
	e.apiBound = func() { close(bound) }
	run, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- API(run, e) }()
	t.Cleanup(func() {
		stop()
		<-done //nolint:errcheck // the role's end, its error the cancellation's
	})
	<-bound
	pemCert, err := os.ReadFile(cert)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pemCert)
	client := func(certs ...tls.Certificate) tracekeepv1connect.MachineServiceClient {
		tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: certs, MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true}
		return tracekeepv1connect.NewMachineServiceClient(&http.Client{Transport: tr}, "https://"+e.APIAddr())
	}
	now := time.Now()
	sel := &tkv1.Selector{ToolId: "SIM-TOOL", From: timestamppb.New(now.Add(-time.Hour)), To: timestamppb.New(now)}
	withCert := client(ca.issue(t))
	if _, err := withCert.ListPacks(t.Context(), connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel})); err != nil {
		t.Fatalf("ListPacks with the client certificate = %v", err)
	}
	// The listener speaks HTTP/2 over TLS: a raw request through the same kind of transport says so.
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true}
	raw, err := (&http.Client{Transport: tr}).Get("https://" + e.APIAddr() + "/download/x") //nolint:noctx // a test's probe
	if err != nil {
		t.Fatal(err)
	}
	_ = raw.Body.Close()
	if raw.ProtoMajor != 2 {
		t.Errorf("the API answered over %s, want HTTP/2", raw.Proto)
	}
	bare := client()
	if _, err := bare.ListPacks(t.Context(), connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel})); connectrpc.CodeOf(err) != connectrpc.CodeUnauthenticated {
		t.Errorf("ListPacks with no credential = %v, want unauthenticated", err)
	}
	stranger := client(newClientCA(t).issue(t))
	if _, err := stranger.ListPacks(t.Context(), connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel})); err == nil {
		t.Error("ListPacks with a certificate of another authority succeeded")
	}
}

// blocking is a store whose object reads block until their Open's context ends, signalling the first read and every close.
type blocking struct {
	store.ObjectStore
	reading chan struct{}
	closed  atomic.Bool
}

func (b *blocking) Open(ctx context.Context, key string) (store.Object, error) {
	obj, err := b.ObjectStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}

	return &blockingObject{Object: obj, ctx: ctx, b: b}, nil
}

type blockingObject struct {
	store.Object
	ctx context.Context //nolint:containedctx // the Open's context, which an object's reads use (store.ObjectStore)
	b   *blocking
}

func (o *blockingObject) ReadAt(p []byte, off int64) (int, error) {
	select {
	case o.b.reading <- struct{}{}:
	default:
	}
	<-o.ctx.Done()

	return 0, o.ctx.Err()
}

// Close takes a while, as releasing a store's connection may: a role that did not wait for its handlers returns first.
func (o *blockingObject) Close() error {
	time.Sleep(200 * time.Millisecond)
	o.b.closed.Store(true)

	return o.Object.Close()
}

// A download still running when the role stops is cut once the shutdown bound passes,
// and the role returns only after its handler ended and closed its object, before the store and catalog close.
func TestAPIRoleStopsWithADownloadInFlight(t *testing.T) {
	t.Parallel()
	e := devEnv(t)
	bound := make(chan struct{})
	e.apiBound = func() { close(bound) }
	e.apiShutdown = 100 * time.Millisecond
	mem := store.NewMem()
	body := bytes.Repeat([]byte("x"), 1<<20)
	if err := mem.Put(t.Context(), "staging/blocked.tpk", bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	b := &blocking{ObjectStore: mem, reading: make(chan struct{}, 1)}
	e.Store = b
	c := e.Config
	declared := map[string]keys.Declaration{}
	for id, d := range c.Keys.Declared {
		declared[id] = keys.Declaration{Provider: d.Provider, Ref: d.Ref}
	}
	k, err := keys.New(keys.Config{Declared: declared, Download: keys.Purpose{Issuing: c.Keys.Download.Issuing, Accepted: c.Keys.Download.Accepted}}, e.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := (&auth.Downloads{Keys: k}).Sign(t.Context(), "staging/blocked.tpk", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	run, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- API(run, e) }()
	<-bound
	go func() {
		resp, err := trusting(t, e.Config.API.TLSCertFile).Get(e.Config.API.PublicURL + "/download/" + tok) //nolint:noctx // a test's download, cut by the server
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	<-b.reading
	stop()
	select {
	case <-done: //nolint:errcheck // the role's end, its error the cancellation's
	case <-time.After(10 * time.Second):
		t.Fatal("the role did not return after its shutdown bound")
	}
	if !b.closed.Load() {
		t.Error("the role returned while the download's handler still held its object")
	}
}

// The api role resolves every accepted session key id at start (T-48), so a started replica validates every cookie without the provider;
// one it cannot resolve fails the start.
func TestAPIRoleResolvesSessionKeysAtStart(t *testing.T) {
	t.Parallel()
	e := devEnv(t)
	dir := t.TempDir()
	for _, id := range []string{"s-0", "s-1"} {
		path := filepath.Join(dir, id)
		if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString([]byte(rand.Text()+rand.Text()))), 0o600); err != nil {
			t.Fatal(err)
		}
		e.Config.Keys.Declared[id] = config.KeyDeclaration{Provider: keys.ProviderFile, Ref: path, Kind: catalog.KindSession}
	}
	e.Config.Keys.Session = config.KeyPurpose{Issuing: "s-1", Accepted: []string{"s-0", "s-1"}}
	if err := e.sessions(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"s-0", "s-1"} {
		if b, bound, err := e.Catalog.KeyBinding(t.Context(), id); err != nil || !bound || b.Kind != catalog.KindSession {
			t.Errorf("%s after the start = %+v, %t, %v; want bound as session", id, b, bound, err)
		}
	}

	e = devEnv(t)
	e.Config.Keys.Declared["s-0"] = config.KeyDeclaration{Provider: keys.ProviderFile, Ref: filepath.Join(dir, "absent"), Kind: catalog.KindSession}
	e.Config.Keys.Declared["s-1"] = config.KeyDeclaration{Provider: keys.ProviderFile, Ref: filepath.Join(dir, "s-1"), Kind: catalog.KindSession}
	e.Config.Keys.Session = config.KeyPurpose{Issuing: "s-1", Accepted: []string{"s-0", "s-1"}}
	// The deadline only bounds a regression, which would serve instead of failing.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := API(ctx, e); err == nil || !strings.Contains(err.Error(), "s-0") {
		t.Errorf("API with an old accepted session key unresolvable = %v, want the start failed naming s-0", err)
	}
}

// With people's sign-in configured the api role serves /auth/, which answers a login with the IdP unreachable (a closed local port)
// as unavailable without hanging; without it, /auth/ is not found.
func TestAPIRoleServesSignIn(t *testing.T) {
	t.Parallel()
	for _, configured := range []bool{true, false} {
		e := devEnv(t)
		e.Config.API.Listen = "127.0.0.1:0"
		e.Config.API.TLSCertFile, e.Config.API.TLSKeyFile = selfSigned(t)
		e.Config.API.PublicURL = "https://tk.example.invalid"
		e.Config.Auth.People = config.People{} // dev's own sign-in, replaced below when configured
		if configured {
			key := filepath.Join(t.TempDir(), "s-1")
			if err := os.WriteFile(key, []byte(base64.StdEncoding.EncodeToString([]byte(rand.Text()+rand.Text()))), 0o600); err != nil {
				t.Fatal(err)
			}
			e.Config.Keys.Declared["s-1"] = config.KeyDeclaration{Provider: keys.ProviderFile, Ref: key, Kind: catalog.KindSession}
			e.Config.Keys.Session = config.KeyPurpose{Issuing: "s-1", Accepted: []string{"s-1"}}
			e.Config.Auth.OIDC = config.OIDC{Issuer: "https://127.0.0.1:1", Audience: "tk-services", JWKSTTL: time.Hour, FetchInterval: 10 * time.Second}
			e.Config.Auth.People = config.People{ClientID: "tk-web", ClientSecret: "s", RolesClaim: "groups",
				SessionLifetime: 8 * time.Hour, LoginLifetime: 10 * time.Minute, ExchangeTimeout: 10 * time.Second}
		}
		bound := make(chan struct{})
		e.apiBound = func() { close(bound) }
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- API(ctx, e) }()
		t.Cleanup(cancel)
		select {
		case <-bound:
		case err := <-done:
			t.Fatalf("configured %t: API = %v", configured, err)
		case <-time.After(30 * time.Second): // bounds a hung start only
			t.Fatalf("configured %t: the api role never bound", configured)
		}
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // the test's own certificate
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Timeout: 30 * time.Second}
		resp, err := client.Get("https://" + e.APIAddr() + "/auth/login")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		want := http.StatusNotFound
		if configured {
			want = http.StatusServiceUnavailable
		}
		if resp.StatusCode != want || resp.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("configured %t: /auth/login = %d, Cache-Control %q; want %d", configured, resp.StatusCode, resp.Header.Get("Cache-Control"), want)
		}
		cancel()
		select {
		case <-done: //nolint:errcheck // the role's end, its error the cancellation's
		case <-time.After(30 * time.Second):
			t.Fatalf("configured %t: the api role did not stop", configured)
		}
	}
}

// The api role re-reads its certificate, key and client CA when their files change, without a restart,
// and keeps serving the previous ones while a change does not load.
func TestAPIRoleReloadsItsTLSFiles(t *testing.T) {
	t.Parallel()
	e := devEnv(t)
	oldCA, newCA := newClientCA(t), newClientCA(t)
	oldCert, oldKey := selfSigned(t)
	newCert, newKey := selfSigned(t)
	dir := t.TempDir()
	certFile, keyFile, caFile := filepath.Join(dir, "api.crt"), filepath.Join(dir, "api.key"), filepath.Join(dir, "ca.pem")
	// install replaces each file by a rename, as a mounted secret's update does.
	install := func(pairs ...string) {
		t.Helper()
		for i := 0; i < len(pairs); i += 2 {
			b, err := os.ReadFile(pairs[i])
			if err != nil {
				t.Fatal(err)
			}
			tmp := pairs[i+1] + ".new"
			if err := os.WriteFile(tmp, b, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(tmp, pairs[i+1]); err != nil {
				t.Fatal(err)
			}
		}
	}
	install(oldCert, certFile, oldKey, keyFile, oldCA.file, caFile)
	c := e.Config
	c.API.Listen, c.API.TLSCertFile, c.API.TLSKeyFile = "127.0.0.1:0", certFile, keyFile
	c.API.PublicURL = "https://127.0.0.1"
	c.Auth.MTLS.ClientCAFile, c.Auth.MTLS.Subjects = caFile, map[string][]string{generator: {"pack-reader"}}
	bound := make(chan struct{})
	e.apiBound = func() { close(bound) }
	run, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- API(run, e) }()
	t.Cleanup(func() {
		stop()
		<-done //nolint:errcheck // the role's end, its error the cancellation's
	})
	<-bound
	// read lists packs over a new connection that trusts only server's certificate and presents cert.
	read := func(server string, cert tls.Certificate) error {
		pemCert, err := os.ReadFile(server)
		if err != nil {
			t.Fatal(err)
		}
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(pemCert)
		tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true}
		defer tr.CloseIdleConnections()
		client := tracekeepv1connect.NewMachineServiceClient(&http.Client{Transport: tr}, "https://"+e.APIAddr())
		now := time.Now()
		sel := &tkv1.Selector{ToolId: "SIM-TOOL", From: timestamppb.New(now.Add(-time.Hour)), To: timestamppb.New(now)}
		_, err = client.ListPacks(t.Context(), connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel}))

		return err
	}
	if err := read(oldCert, oldCA.issue(t)); err != nil {
		t.Fatalf("before the renewal: %v", err)
	}
	install(newCert, certFile, newKey, keyFile, newCA.file, caFile)
	if err := read(newCert, newCA.issue(t)); err != nil {
		t.Errorf("after the renewal, the new certificate and client CA: %v", err)
	}
	if err := read(oldCert, newCA.issue(t)); err == nil {
		t.Error("after the renewal the API still presents its old certificate")
	}
	if err := read(newCert, oldCA.issue(t)); err == nil {
		t.Error("after the renewal a certificate of the old client CA is still admitted")
	}
	// A key that does not match the certificate, as when only one of them has been replaced, keeps the previous pair.
	install(oldKey, keyFile)
	if err := read(newCert, newCA.issue(t)); err != nil {
		t.Errorf("with a mismatched key: %v, want the previous pair served", err)
	}
	install(newKey, keyFile)
	if err := read(newCert, newCA.issue(t)); err != nil {
		t.Fatalf("with the key replaced too: %v", err)
	}
	// A client CA file holding no certificate keeps the previous CA, with the certificate and key matching,
	// so only the CA's check refuses the change.
	if err := os.WriteFile(caFile+".new", []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(caFile+".new", caFile); err != nil {
		t.Fatal(err)
	}
	if err := read(newCert, newCA.issue(t)); err != nil {
		t.Errorf("with a client CA file holding no certificate: %v, want the previous CA kept", err)
	}
	// A third CA loads once its file holds it, and replaces the kept one.
	thirdCA := newClientCA(t)
	install(thirdCA.file, caFile)
	if err := read(newCert, thirdCA.issue(t)); err != nil {
		t.Errorf("after the CA file was repaired: %v", err)
	}
	if err := read(newCert, newCA.issue(t)); err == nil {
		t.Error("after the CA file was repaired the kept CA is still admitted")
	}
	// A deleted file keeps the previous material.
	if err := os.Remove(caFile); err != nil {
		t.Fatal(err)
	}
	if err := read(newCert, thirdCA.issue(t)); err != nil {
		t.Errorf("with the client CA file deleted: %v, want the previous CA kept", err)
	}
}
