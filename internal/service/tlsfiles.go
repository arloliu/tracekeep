package service

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/arloliu/tracekeep/internal/config"
)

// tlsFiles is the API's TLS material, read from its files at every handshake and loaded again when one has changed,
// so a renewed certificate, key or client CA is served without a restart.
// A change that does not load, such as a certificate whose key is not yet replaced, keeps the previous material,
// and is tried again once the files change again.
type tlsFiles struct {
	paths [3]string // the certificate, its key and the client CA, empty when none is configured
	log   *slog.Logger

	mu     sync.Mutex
	tried  [3][]byte // the files' contents last loaded or tried
	failed string    // the last read failure warned of, empty once the files read again
	config *tls.Config
}

// serverTLS is the listener's TLS: a's certificate, and client certificates verified against m's CA when given,
// each re-read when its file changes.
func serverTLS(a *config.API, m *config.MTLS, log *slog.Logger) (*tls.Config, error) {
	if a.TLSCertFile == "" {
		return nil, errors.New("service: api: api.tls_cert_file and api.tls_key_file are required")
	}
	f := &tlsFiles{paths: [3]string{a.TLSCertFile, a.TLSKeyFile, m.ClientCAFile}, log: log}
	b, err := f.read()
	if err != nil {
		return nil, err
	}
	if f.config, err = f.load(b); err != nil {
		return nil, err
	}
	f.tried = b

	return &tls.Config{MinVersion: tls.VersionTLS12, GetConfigForClient: f.forClient}, nil
}

// forClient is the handshake's TLS: the material last loaded, loaded again first when a file has changed.
// The files are read under the lock, so an older read never replaces a newer load.
func (f *tlsFiles) forClient(*tls.ClientHelloInfo) (*tls.Config, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := f.read()
	if err != nil {
		// Warned once per distinct failure, not at every handshake.
		if msg := err.Error(); msg != f.failed {
			f.failed = msg
			f.log.Warn("api: kept the previous TLS material", "error", err)
		}
		return f.config, nil
	}
	f.failed = ""
	if same(b, f.tried) {
		return f.config, nil
	}
	f.tried = b
	cfg, err := f.load(b)
	if err != nil {
		f.log.Warn("api: kept the previous TLS material", "error", err)
		return f.config, nil
	}
	f.config = cfg
	f.log.Info("api: loaded new TLS material")

	return cfg, nil
}

func (f *tlsFiles) read() ([3][]byte, error) {
	var b [3][]byte
	for i, p := range f.paths {
		if p == "" {
			continue
		}
		var err error
		if b[i], err = os.ReadFile(p); err != nil {
			return b, fmt.Errorf("service: api: %w", err)
		}
	}

	return b, nil
}

// load is the TLS of the files' contents b.
// Its protocols are the server's, HTTP/2 and HTTP/1.1, since it replaces the listener's configuration for the handshake.
func (f *tlsFiles) load(b [3][]byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(b[0], b[1])
	if err != nil {
		return nil, fmt.Errorf("service: api: %s and %s: %w", f.paths[0], f.paths[1], err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2", "http/1.1"}}
	if f.paths[2] != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b[2]) {
			return nil, fmt.Errorf("service: api: %s holds no certificate", f.paths[2])
		}
		cfg.ClientCAs, cfg.ClientAuth = pool, tls.VerifyClientCertIfGiven
	}

	return cfg, nil
}

func same(a, b [3][]byte) bool {
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}

	return true
}
