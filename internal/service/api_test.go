package service

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
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
	"testing"
	"time"

	connectrpc "connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/types/known/timestamppb"

	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// h2c is a client of HTTP/2 without TLS, as dev serves it.
func h2c() *http.Client {
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)

	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

// waitFor calls f until it returns true or the deadline passes.
func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// tracekeep dev serves the machine API behind its generated token: a client lists the simulator's packs, gets a pack's URL,
// downloads it through the API and reads it with tracepack's reader.
func TestDevServesTheMachineAPI(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	d, err := NewDev(ctx, DevOptions{Dir: dir, Flush: 200 * time.Millisecond, Rate: 50, API: "127.0.0.1:0", Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(run) }()
	t.Cleanup(func() {
		stop()
		<-done //nolint:errcheck // the run's end, its error the cancellation's
	})
	waitFor(t, "the api to listen", func() bool { return d.Env.APIAddr() != "" })
	token, err := os.ReadFile(filepath.Join(dir, DevTokenFile))
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(filepath.Join(dir, DevTokenFile)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the token file = %v, %v; want mode 0600", fi, err)
	}
	base := "http://" + d.Env.APIAddr()
	cl := tracekeepv1connect.NewMachineServiceClient(h2c(), base)
	authed := func(r interface{ Header() http.Header }) {
		r.Header().Set("Authorization", "Bearer "+string(token))
	}
	now := time.Now()
	sel := &tkv1.Selector{ToolId: "SIM-TOOL", From: timestamppb.New(now.Add(-2 * time.Hour)), To: timestamppb.New(now.Add(time.Hour))}
	var pack string
	waitFor(t, "a staged pack", func() bool {
		req := connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel})
		authed(req)
		resp, err := cl.ListPacks(ctx, req)
		if err != nil {
			t.Fatalf("ListPacks = %v", err)
		}
		for _, h := range resp.Msg.Hours {
			for _, s := range h.Scopes {
				for _, p := range s.Packs {
					pack = p.PackId
				}
			}
		}

		return pack != ""
	})
	var u *tkv1.PackURLResponse
	waitFor(t, "a pack URL, once retention is recorded", func() bool {
		req := connectrpc.NewRequest(&tkv1.PackURLRequest{PackId: pack})
		authed(req)
		resp, err := cl.PackURL(ctx, req)
		if connectrpc.CodeOf(err) == connectrpc.CodeUnavailable {
			return false
		}
		if err != nil {
			t.Fatalf("PackURL = %v", err)
		}
		u = resp.Msg

		return true
	})
	if u.Presigned || !strings.HasPrefix(u.Url, base+"/download/") {
		t.Fatalf("PackURL = %v, want a URL on the API", u)
	}
	resp, err := h2c().Get(u.Url) //nolint:noctx // a test's download
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
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

// presigning is a store whose presigned GETs reach srv.
type presigning struct{ url string }

func (p presigning) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	return p.url + "/" + key, nil
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
	if err := checkPresign(t.Context(), presigning{"http://" + addr}); err == nil || !strings.Contains(err.Error(), "store.presign") {
		t.Errorf("an unreachable store: check = %v, want the advice to turn presigning off", err)
	}
}

// Without a presigning store the api role needs a public URL and a download key, and refuses to start without them.
func TestAPIRoleNeedsDownloadKeys(t *testing.T) {
	t.Parallel()
	e := devEnv(t)
	e.Config.API.Plaintext = false
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
	d, err := NewDev(t.Context(), DevOptions{Dir: t.TempDir(), Flush: time.Second, API: "127.0.0.1:0", Log: slog.New(slog.DiscardHandler)})
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

// clientCA issues client certificates naming uri, and writes its own certificate to a file.
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

func (c clientCA) issue(t *testing.T, uri string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(uri)
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
	const generator = "spiffe://cluster.example/ns/ve/sa/generator"
	ca := newClientCA(t)
	cert, key := selfSigned(t)
	c := e.Config
	c.API.Plaintext, c.API.Listen, c.API.TLSCertFile, c.API.TLSKeyFile = false, "127.0.0.1:0", cert, key
	c.API.PublicURL = "https://127.0.0.1"
	c.Auth.MTLS.ClientCAFile, c.Auth.MTLS.Subjects = ca.file, map[string][]string{generator: {"pack-reader"}}
	run, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- API(run, e) }()
	t.Cleanup(func() {
		stop()
		<-done //nolint:errcheck // the role's end, its error the cancellation's
	})
	waitFor(t, "the api to listen", func() bool { return e.APIAddr() != "" })
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
	withCert := client(ca.issue(t, generator))
	if _, err := withCert.ListPacks(t.Context(), connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel})); err != nil {
		t.Fatalf("ListPacks with the client certificate = %v", err)
	}
	bare := client()
	if _, err := bare.ListPacks(t.Context(), connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel})); connectrpc.CodeOf(err) != connectrpc.CodeUnauthenticated {
		t.Errorf("ListPacks with no credential = %v, want unauthenticated", err)
	}
	stranger := client(newClientCA(t).issue(t, generator))
	if _, err := stranger.ListPacks(t.Context(), connectrpc.NewRequest(&tkv1.ListPacksRequest{Selector: sel})); err == nil {
		t.Error("ListPacks with a certificate of another authority succeeded")
	}
}
