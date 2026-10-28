package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// downloadVersion is the download token's form version, and downloadDomain the MAC's domain separation (api.md §10.1 *Downloads*).
const (
	downloadVersion = 1
	downloadDomain  = "tracekeep/download-token/v1\x00"
	// maxDownloadToken bounds a token's length before it is decoded.
	maxDownloadToken = 4096
)

var b64 = base64.RawURLEncoding

// DownloadKeys is the download purpose's keys; a *keys.Keys is one.
type DownloadKeys interface {
	Issuing(ctx context.Context, kind string) (string, []byte, error)
	Accepted(ctx context.Context, kind, keyID string) ([]byte, error)
}

// Downloads signs and verifies the download tokens a URL on the API carries (api.md §10.1 *Downloads*):
// each names one object key and an expiry and carries the key id it was signed with, all under one HMAC-SHA-256.
type Downloads struct {
	Keys DownloadKeys
	// Now is the clock; nil is time.Now.
	Now func() time.Time
	// Failures counts each refused token as download; nil counts none.
	Failures Failures
}

// Sign returns a token for objectKey expiring at expiry, under the issuing key id.
// The caller has checked the issuing operation's role and clamped expiry to the object's deadline.
func (d *Downloads) Sign(ctx context.Context, objectKey string, expiry time.Time) (string, error) {
	keyID, key, err := d.Keys.Issuing(ctx, catalog.KindDownload)
	if err != nil {
		return "", fmt.Errorf("auth: download token: %w", err)
	}
	payload := binary.AppendUvarint([]byte{downloadVersion}, uint64(len(keyID)))
	payload = append(payload, keyID...)
	payload = binary.BigEndian.AppendUint64(payload, uint64(expiry.Unix())) //nolint:gosec // the bits of a signed time, read back as signed
	payload = append(payload, objectKey...)

	return b64.EncodeToString(payload) + "." + b64.EncodeToString(mac(key, payload)), nil
}

// Verify admits token for objectKey: its key id accepted, its MAC equal in constant time, its object objectKey and its expiry not reached.
// A refusal is an *Error of reason download, counted.
func (d *Downloads) Verify(ctx context.Context, token, objectKey string) error {
	err := d.check(ctx, token, objectKey)
	if err == nil {
		return nil
	}
	if d.Failures != nil {
		d.Failures.AuthFailure(ReasonDownload)
	}

	return refuse(ReasonDownload, err)
}

// check is Verify's refusal, nil when it admits the token.
func (d *Downloads) check(ctx context.Context, token, objectKey string) error {
	if len(token) > maxDownloadToken {
		return errors.New("a token too long")
	}
	p, m, ok := strings.Cut(token, ".")
	payload, err1 := b64.DecodeString(p)
	sum, err2 := b64.DecodeString(m)
	if !ok || err1 != nil || err2 != nil || len(payload) == 0 || payload[0] != downloadVersion {
		return errors.New("not a download token")
	}
	n, w := binary.Uvarint(payload[1:])
	rest := payload[1:]
	if w <= 0 || n > uint64(len(rest)-w) || uint64(len(rest)-w)-n < 8 {
		return errors.New("not a download token")
	}
	end := w + int(n) //nolint:gosec // n is bounded by the payload's length just above
	keyID := string(rest[w:end])
	rest = rest[end:]
	expiry := time.Unix(int64(binary.BigEndian.Uint64(rest[:8])), 0) //nolint:gosec // the bits of a signed time
	// The key id is checked against the accepted ones before its key is resolved.
	key, err := d.Keys.Accepted(ctx, catalog.KindDownload, keyID)
	if err != nil {
		return err
	}
	if !hmac.Equal(sum, mac(key, payload)) {
		return fmt.Errorf("key id %s: the MAC does not verify", keyID)
	}
	if string(rest[8:]) != objectKey {
		return errors.New("a token for another object")
	}
	now := time.Now
	if d.Now != nil {
		now = d.Now
	}
	if !now().Before(expiry) {
		return fmt.Errorf("expired at %s", expiry.UTC().Format(time.RFC3339))
	}

	return nil
}

// mac is the token's HMAC-SHA-256 under key over the domain and the payload.
func mac(key, payload []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(downloadDomain))
	h.Write(payload)

	return h.Sum(nil)
}
