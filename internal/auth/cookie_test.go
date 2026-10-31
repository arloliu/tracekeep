package auth

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// sessionKeys is a session purpose over fixed material: issuing one key id, accepting a list,
// recording each key id whose material was asked for.
type sessionKeys struct {
	issuing  string
	accepted []string
	material map[string][]byte
	mu       sync.Mutex
	asked    []string
}

func newSessionKeys(issuing string, accepted ...string) *sessionKeys {
	k := &sessionKeys{issuing: issuing, accepted: accepted, material: map[string][]byte{}}
	for i, id := range accepted {
		k.material[id] = bytes.Repeat([]byte{byte(20 + i)}, 32)
	}

	return k
}

func (k *sessionKeys) Issuing(_ context.Context, kind string) (string, []byte, error) {
	if kind != catalog.KindSession {
		return "", nil, errors.New("no such purpose")
	}

	return k.issuing, k.material[k.issuing], nil
}

func (k *sessionKeys) Accepted(_ context.Context, kind, keyID string) ([]byte, error) {
	if kind != catalog.KindSession || !slices.Contains(k.accepted, keyID) {
		return nil, errors.New("not accepted")
	}
	k.mu.Lock()
	k.asked = append(k.asked, keyID)
	k.mu.Unlock()

	return k.material[keyID], nil
}

func testCookies(k *sessionKeys) *Cookies {
	return &Cookies{Keys: k, Leeway: time.Minute, Now: newClock().Now}
}

func TestSessionCookieRoundTrip(t *testing.T) {
	t.Parallel()
	c := testCookies(newSessionKeys("s-1", "s-1"))
	want := Session{Subject: "person-1", Roles: []string{RoleViewer}, Issued: c.Now().Truncate(time.Second), Expiry: c.Now().Add(8 * time.Hour).Truncate(time.Second)}
	v, err := c.SealSession(t.Context(), want)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(v, "person-1") || strings.ContainsAny(v, "=+/;, ") {
		t.Errorf("cookie %q shows its subject or leaves base64url", v)
	}
	got, err := c.OpenSession(t.Context(), v)
	if err != nil || got.Subject != want.Subject || !slices.Equal(got.Roles, want.Roles) || !got.Expiry.Equal(want.Expiry) || !got.Issued.Equal(want.Issued) {
		t.Errorf("OpenSession = %+v, %v; want %+v", got, err, want)
	}
	l := Login{Verifier: "v", State: "s", Nonce: "n", Return: "/x", Expiry: c.Now().Add(10 * time.Minute)}
	lv, err := c.SealLogin(t.Context(), l)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.OpenLogin(t.Context(), lv); err != nil || got.State != "s" || got.Verifier != "v" || got.Nonce != "n" || got.Return != "/x" {
		t.Errorf("OpenLogin = %+v, %v", got, err)
	}
}

// A login cookie never passes as a session cookie, nor the reverse, even with the purpose byte rewritten:
// the purpose is in the key and the additional data.
func TestCookiePurposesStayDistinct(t *testing.T) {
	t.Parallel()
	c := testCookies(newSessionKeys("s-1", "s-1"))
	lv, err := c.SealLogin(t.Context(), Login{State: "s", Expiry: c.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.OpenSession(t.Context(), lv); err == nil {
		t.Error("a login cookie opened as a session")
	}
	raw, _ := base64.RawURLEncoding.DecodeString(lv)
	raw[1] = purposeSession
	if _, err := c.OpenSession(t.Context(), base64.RawURLEncoding.EncodeToString(raw)); err == nil {
		t.Error("a login cookie with its purpose rewritten opened as a session")
	}
	sv, err := c.SealSession(t.Context(), Session{Subject: "p", Expiry: c.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.OpenLogin(t.Context(), sv); err == nil {
		t.Error("a session cookie opened as a login")
	}
}

// Rotation: a cookie issued under an old key id opens while it is accepted, and is refused once dropped;
// a key id not accepted is refused before any material is asked for.
func TestCookieRotation(t *testing.T) {
	t.Parallel()
	old := newSessionKeys("s-0", "s-0", "s-1")
	v, err := testCookies(old).SealSession(t.Context(), Session{Subject: "p", Expiry: newClock().Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	switched := newSessionKeys("s-1", "s-0", "s-1")
	switched.material = old.material
	if _, err := testCookies(switched).OpenSession(t.Context(), v); err != nil {
		t.Errorf("a cookie of the old key id while it is accepted: %v", err)
	}
	dropped := newSessionKeys("s-1", "s-1")
	dropped.material = old.material
	if _, err := testCookies(dropped).OpenSession(t.Context(), v); err == nil {
		t.Error("a cookie of a dropped key id opened")
	}
	if len(dropped.asked) != 0 {
		t.Errorf("material asked for %v, want none for a key id not accepted", dropped.asked)
	}
	// The key id is authenticated: renamed to another accepted id with the same material, the cookie still fails to open.
	shared := newSessionKeys("s-1", "s-0", "s-1")
	shared.material = map[string][]byte{"s-0": old.material["s-0"], "s-1": old.material["s-0"]}
	raw, _ := base64.RawURLEncoding.DecodeString(v)
	renamed := bytes.Replace(raw, []byte("s-0"), []byte("s-1"), 1)
	if _, err := testCookies(shared).OpenSession(t.Context(), v); err != nil {
		t.Fatalf("the cookie under its own key id: %v", err)
	}
	if _, err := testCookies(shared).OpenSession(t.Context(), base64.RawURLEncoding.EncodeToString(renamed)); err == nil {
		t.Error("a cookie with its key id renamed opened under the same material")
	}
}

func TestCookieRefusals(t *testing.T) {
	t.Parallel()
	k := newSessionKeys("s-1", "s-1")
	c := testCookies(k)
	v, err := c.SealSession(t.Context(), Session{Subject: "p", Expiry: c.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.RawURLEncoding.DecodeString(v)
	flipped := slices.Clone(raw)
	flipped[len(flipped)-1] ^= 1
	versioned := slices.Clone(raw)
	versioned[0] = 2
	for name, value := range map[string]string{
		"empty":            "",
		"not base64url":    "a+b/c=",
		"padded":           v + "==",
		"too short":        "AQI",
		"no key id":        base64.RawURLEncoding.EncodeToString([]byte{1, 2, 0}),
		"a key id past it": base64.RawURLEncoding.EncodeToString([]byte{1, 2, 200, 's'}),
		"tampered":         base64.RawURLEncoding.EncodeToString(flipped),
		"another version":  base64.RawURLEncoding.EncodeToString(versioned),
		"too long":         strings.Repeat("A", MaxCookie+1),
	} {
		if _, err := c.OpenSession(t.Context(), value); err == nil {
			t.Errorf("%s: OpenSession succeeded", name)
		}
	}
	// Past its expiry by more than the leeway it is refused; within the leeway it opens.
	late := testCookies(k)
	late.Now = func() time.Time { return c.Now().Add(time.Hour + 30*time.Second) }
	if _, err := late.OpenSession(t.Context(), v); err != nil {
		t.Errorf("within the leeway: %v", err)
	}
	late.Now = func() time.Time { return c.Now().Add(time.Hour + 2*time.Minute) }
	if _, err := late.OpenSession(t.Context(), v); err == nil {
		t.Error("an expired cookie opened")
	}
	// A session too large to fit a cookie is refused at sealing.
	big := Session{Subject: "p", Roles: slices.Repeat([]string{strings.Repeat("r", 100)}, 40), Expiry: c.Now().Add(time.Hour)}
	if _, err := c.SealSession(t.Context(), big); !errors.Is(err, ErrCookieTooLong) {
		t.Errorf("sealing a session past the bound = %v, want ErrCookieTooLong", err)
	}
}

// FuzzSessionCookie: no input panics the parser, and only a sealed cookie opens, to the session sealed.
// Each fuzzing process seals its own seed under a fresh nonce, so the check is on what opens, not on the value.
func FuzzSessionCookie(f *testing.F) {
	c := testCookies(newSessionKeys("s-1", "s-1"))
	valid, err := c.SealSession(context.Background(), Session{Subject: "p", Expiry: c.Now().Add(time.Hour)})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add("")
	f.Add("AQIDcy0x")
	f.Fuzz(func(t *testing.T, value string) {
		if s, err := c.OpenSession(context.Background(), value); err == nil && s.Subject != "p" {
			t.Errorf("OpenSession admitted %q as %+v, not a session sealed", value, s)
		}
	})
}

// Every key id the configuration admits, 128 characters of up to four bytes, seals and opens.
func TestCookieKeyIDLengths(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"s", strings.Repeat("\u20ac", 86), strings.Repeat("\U0001F511", 128)} {
		k := newSessionKeys(id, id)
		c := testCookies(k)
		v, err := c.SealSession(t.Context(), Session{Subject: "p", Expiry: c.Now().Add(time.Hour)})
		if err != nil {
			t.Errorf("a key id of %d bytes: %v", len(id), err)
			continue
		}
		if _, err := c.OpenSession(t.Context(), v); err != nil {
			t.Errorf("a key id of %d bytes does not open: %v", len(id), err)
		}
	}
}
