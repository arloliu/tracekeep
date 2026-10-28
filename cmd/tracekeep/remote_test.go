package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/service"
)

// selfSigned writes a certificate for 127.0.0.1 and its key under dir, and returns their paths.
func selfSigned(t *testing.T, dir string) (string, string) {
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
	cert, keyFile := filepath.Join(dir, "api.crt"), filepath.Join(dir, "api.key")
	for path, block := range map[string]*pem.Block{cert: {Type: "CERTIFICATE", Bytes: der}, keyFile: {Type: "PRIVATE KEY", Bytes: kb}} {
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	return cert, keyFile
}

func secret(t *testing.T, dir, name, text string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

// With a running service holding its SQLite catalog, tracekeep admin and status go through its AdminService:
// the configuration's admin token authenticates, the service's certificate is trusted from api.tls_cert_file,
// and the audited actor is the token's subject, not --actor.
func TestAdminThroughTheRunningService(t *testing.T) {
	t.Parallel()
	path := setup(t)
	dir := filepath.Dir(path)
	cert, key := selfSigned(t, dir)
	token := secret(t, dir, "admin-token", "a-static-token-of-an-operator-000001")
	dl := secret(t, dir, "dl-1", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))))
	base, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	extra := func(publicURL string) string {
		return string(base) + "api:\n  listen: 127.0.0.1:0\n  tls_cert_file: " + cert + "\n  tls_key_file: " + key + "\n  public_url: " + publicURL +
			"\nauth:\n  static:\n    enabled: true\n    tokens:\n      - {subject: operator, token_ref: \"file://" + token + "\", roles: [admin]}\n" +
			"keys:\n  declared:\n    dl-1: {provider: file, ref: " + dl + "}\n  download:\n    issuing: dl-1\n    accepted: [dl-1]\n"
	}
	serviceConfig := secret(t, dir, "service.yaml", extra("https://127.0.0.1:1"))
	if code, _, stderr := tracekeep(t, "migrate", "--config", serviceConfig); code != 0 {
		t.Fatalf("migrate = %d, %s", code, stderr)
	}
	cfg, err := config.Load(serviceConfig)
	if err != nil {
		t.Fatal(err)
	}
	e, err := service.Open(t.Context(), cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	run, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- service.API(run, e) }()
	t.Cleanup(func() {
		stop()
		<-done //nolint:errcheck // the role's end, its error the cancellation's
		_ = e.Close()
	})
	select {
	case <-e.APIListening():
	case err := <-done:
		t.Fatalf("the api role ended at start: %v", err)
	}
	cli := secret(t, dir, "cli.yaml", extra("https://"+e.APIAddr()))
	capture := tracepack.UUID{0x51}
	code, stdout, stderr := tracekeep(t, "admin", "--config", cli, "--actor", "someone-else", "capture", "close", capture.String())
	if code != 0 || !json.Valid([]byte(stdout)) {
		t.Fatalf("admin through the service = %d, %q, %s", code, stdout, stderr)
	}
	audit, err := e.Catalog.AuditEntries(t.Context(), capture)
	if err != nil || len(audit) != 1 || audit[0].Actor != "operator" {
		t.Errorf("audit = %+v, %v; want the token's subject as actor", audit, err)
	}
	code, stdout, stderr = tracekeep(t, "status", "--config", cli)
	var st struct {
		Tools []any `json:"tools"`
	}
	if code != 0 || json.Unmarshal([]byte(stdout), &st) != nil || st.Tools == nil {
		t.Errorf("status through the service = %d, %q, %s", code, stdout, stderr)
	}
	if code, _, _ := tracekeep(t, "admin", "--config", cli, "no", "such"); code != 2 {
		t.Errorf("an unknown command through the service = %d, want usage", code)
	}
}
