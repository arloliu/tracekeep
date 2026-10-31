package auth

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// The cookie form (api.md §10.2 *People's sign-in*): a version, the purpose, the key id, a nonce and the AES-256-GCM sealing of the payload,
// the version, purpose and key id authenticated as the additional data.
const (
	cookieVersion = 1
	// MaxCookie bounds a cookie's value, refused longer before it is decoded, and never issued longer.
	MaxCookie   = 4096
	cookieNonce = 12
	cookieKey   = 32
)

// A cookie's purpose, which its key and its additional data bind.
const (
	purposeLogin   byte = 1
	purposeSession byte = 2
)

// ErrCookieTooLong is a cookie that would pass MaxCookie, which too many roles cause.
var ErrCookieTooLong = errors.New("auth: a cookie longer than its bound")

// Session is a session cookie's payload: who signed in, with what roles, until when.
type Session struct {
	Subject string    `json:"sub"`
	Roles   []string  `json:"roles"`
	Issued  time.Time `json:"iat"`
	Expiry  time.Time `json:"exp"`
}

// Login is a login cookie's payload: a started login's PKCE verifier, state, nonce and return path, until when.
type Login struct {
	Verifier string    `json:"verifier"`
	State    string    `json:"state"`
	Nonce    string    `json:"nonce"`
	Return   string    `json:"return"`
	Expiry   time.Time `json:"exp"`
}

// Cookies seals and opens the login and session cookies under the session purpose's keys, which any replica validates without shared state.
type Cookies struct {
	Keys DownloadKeys
	// Leeway is the clock tolerance an expiry is allowed.
	Leeway time.Duration
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// SealSession returns the session cookie's value of s, under the issuing session key id.
func (c *Cookies) SealSession(ctx context.Context, s Session) (string, error) {
	return c.seal(ctx, purposeSession, s)
}

// OpenSession returns the session a session cookie's value holds, refusing one that does not decode, names a key id not accepted,
// does not authenticate, or has expired.
func (c *Cookies) OpenSession(ctx context.Context, value string) (Session, error) {
	var s Session
	if err := c.open(ctx, purposeSession, value, &s); err != nil {
		return Session{}, err
	}

	return s, c.live(s.Expiry)
}

// SealLogin returns the login cookie's value of l, under the issuing session key id.
func (c *Cookies) SealLogin(ctx context.Context, l Login) (string, error) {
	return c.seal(ctx, purposeLogin, l)
}

// OpenLogin returns the login a login cookie's value holds, refused as OpenSession refuses.
func (c *Cookies) OpenLogin(ctx context.Context, value string) (Login, error) {
	var l Login
	if err := c.open(ctx, purposeLogin, value, &l); err != nil {
		return Login{}, err
	}

	return l, c.live(l.Expiry)
}

func (c *Cookies) seal(ctx context.Context, purpose byte, payload any) (string, error) {
	keyID, material, err := c.Keys.Issuing(ctx, catalog.KindSession)
	if err != nil {
		return "", fmt.Errorf("auth: cookie: %w", err)
	}
	plain, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("auth: cookie: %w", err)
	}
	aead, err := cookieAEAD(material, purpose)
	if err != nil {
		return "", err
	}
	if len(keyID) == 0 || len(keyID) > 255 {
		return "", fmt.Errorf("auth: cookie: key id of %d bytes", len(keyID))
	}
	head := append([]byte{cookieVersion, purpose, byte(len(keyID))}, keyID...)
	nonce := make([]byte, cookieNonce)
	_, _ = rand.Read(nonce) // never fails (crypto/rand)
	out := append(slices.Clone(head), nonce...)
	out = aead.Seal(out, nonce, plain, head)
	if b64.EncodedLen(len(out)) > MaxCookie {
		return "", ErrCookieTooLong
	}

	return b64.EncodeToString(out), nil
}

// open checks value's length, form version and purpose, then its key id against the accepted ones before the key is resolved,
// then opens the sealing into payload.
func (c *Cookies) open(ctx context.Context, purpose byte, value string, payload any) error {
	if len(value) > MaxCookie {
		return errors.New("a cookie too long")
	}
	raw, err := b64.DecodeString(value)
	if err != nil || len(raw) < 3 {
		return errors.New("not a cookie")
	}
	if raw[0] != cookieVersion {
		return fmt.Errorf("cookie form version %d", raw[0])
	}
	if raw[1] != purpose {
		return errors.New("a cookie of another purpose")
	}
	n := 3 + int(raw[2])
	if raw[2] == 0 || len(raw) < n+cookieNonce {
		return errors.New("not a cookie")
	}
	keyID := string(raw[3:n])
	material, err := c.Keys.Accepted(ctx, catalog.KindSession, keyID)
	if err != nil {
		return err
	}
	aead, err := cookieAEAD(material, purpose)
	if err != nil {
		return err
	}
	plain, err := aead.Open(nil, raw[n:n+cookieNonce], raw[n+cookieNonce:], raw[:n])
	if err != nil {
		return fmt.Errorf("key id %s: the cookie does not authenticate", keyID)
	}
	if err := json.Unmarshal(plain, payload); err != nil {
		return errors.New("a cookie's payload does not decode")
	}

	return nil
}

// live refuses an expiry passed by more than the leeway.
func (c *Cookies) live(expiry time.Time) error {
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	if now().After(expiry.Add(c.Leeway)) {
		return fmt.Errorf("expired at %s", expiry.UTC().Format(time.RFC3339))
	}

	return nil
}

// cookieAEAD is AES-256-GCM under the key HKDF-SHA-256 derives from material for purpose, one key per purpose.
func cookieAEAD(material []byte, purpose byte) (cipher.AEAD, error) {
	key, err := hkdf.Key(sha256.New, material, nil, fmt.Sprintf("tracekeep/cookie/v1/%d", purpose), cookieKey)
	if err != nil {
		return nil, fmt.Errorf("auth: cookie key: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("auth: cookie key: %w", err)
	}

	return cipher.NewGCM(block)
}
