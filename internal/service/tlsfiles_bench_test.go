package service

import (
	"bytes"
	"crypto/tls"
	"io"
	"log/slog"
	"os"
	"runtime"
	"testing"

	"github.com/arloliu/tracekeep/internal/config"
)

// BenchmarkTLSFiles reads the API's TLS files for a handshake, as GetConfigForClient does at every one (P18).
// Its starting state is the listener's TLS loaded from a self-signed certificate, its key and a client CA, three files.
// Unchanged: an iteration reads the files, finds them as loaded and returns the loaded material, asserting it is that material;
// with 1 handshake under b.Loop, and with 8 at once under b.RunParallel, GOMAXPROCS set to 8 for it, so 8 goroutines contend for the lock.
// Reload: outside the timer the certificate and key files are rewritten with the other of two pairs,
// and an iteration reads them, loads the new material and asserts it serves the pair written.
// It has no sizes: the files are a few hundred bytes each.
func BenchmarkTLSFiles(b *testing.B) {
	cert, key := selfSigned(b)
	cfg, err := serverTLS(&config.API{TLSCertFile: cert, TLSKeyFile: key}, &config.MTLS{ClientCAFile: newClientCA(b).file}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		b.Fatal(err)
	}
	hello := &tls.ClientHelloInfo{}
	loaded, err := cfg.GetConfigForClient(hello)
	if err != nil || loaded == nil {
		b.Fatalf("GetConfigForClient = %v, %v", loaded, err)
	}
	b.Run("unchanged/handshakes=1", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := cfg.GetConfigForClient(hello); err != nil || got != loaded {
				b.Fatalf("GetConfigForClient = %p, %v; want the loaded %p", got, err, loaded)
			}
		}
	})
	b.Run("unchanged/handshakes=8", func(b *testing.B) {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(8))
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if got, err := cfg.GetConfigForClient(&tls.ClientHelloInfo{}); err != nil || got != loaded {
					b.Errorf("GetConfigForClient = %p, %v; want the loaded %p", got, err, loaded)
					return
				}
			}
		})
	})
	b.Run("reload", func(b *testing.B) {
		otherCert, otherKey := selfSigned(b)
		pairs := [2][2][]byte{readPair(b, cert, key), readPair(b, otherCert, otherKey)}
		var leaves [2][]byte
		for i, p := range pairs {
			c, err := tls.X509KeyPair(p[0], p[1])
			if err != nil {
				b.Fatal(err)
			}
			leaves[i] = c.Certificate[0]
		}
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			b.StopTimer()
			i = 1 - i
			writePair(b, cert, key, pairs[i])
			b.StartTimer()
			got, err := cfg.GetConfigForClient(hello)
			if err != nil || len(got.Certificates) != 1 || !bytes.Equal(got.Certificates[0].Certificate[0], leaves[i]) {
				b.Fatalf("GetConfigForClient after a change = %v; want pair %d", err, i)
			}
		}
	})
}

// readPair is the contents of a certificate file and its key's.
func readPair(b *testing.B, cert, key string) [2][]byte {
	b.Helper()
	var out [2][]byte
	for i, p := range []string{cert, key} {
		var err error
		if out[i], err = os.ReadFile(p); err != nil {
			b.Fatal(err)
		}
	}

	return out
}

// writePair writes a pair's contents to the certificate and key files.
func writePair(b *testing.B, cert, key string, pair [2][]byte) {
	b.Helper()
	for i, p := range []string{cert, key} {
		if err := os.WriteFile(p, pair[i], 0o600); err != nil {
			b.Fatal(err)
		}
	}
}
