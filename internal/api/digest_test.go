package api

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack/redact"
	"github.com/arloliu/go-secs/v2/secs2"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/policy"
	"github.com/arloliu/tracekeep/internal/procconfig"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

// digestMaterial is d-1's material, as maskingKeys writes it.
var digestMaterial = bytes.Repeat([]byte("d"), 32)

func digestReq(keyID string, ins ...*tkv1.DigestInput) *connect.Request[tkv1.DigestRequest] {
	return withToken(&tkv1.DigestRequest{DigestKeyId: keyID, Inputs: ins}, scorerToken)
}

// baselineInputs are one input of each kind under the baseline's rules.
func baselineInputs() []*tkv1.DigestInput {
	b := policy.Baseline()

	return []*tkv1.DigestInput{
		{Domain: b.ItemRules[0].Domain, Kind: kindItem, Original: maskingtest.List(maskingtest.ASCII("a recipe body"), maskingtest.Item(0o10, 1, 2))},
		{Domain: redact.DomainMessageText, Kind: kindText, Original: []byte("a whole message text")},
		{Domain: b.AnnotationRules[0].Domain, Kind: kindAnnotation, Original: []byte("an annotation value")},
	}
}

// Each input's digest is redact.Digest under d-1's material, in order, with one audit row of the allowlisted fields.
func TestDigest(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.DigestLimit = &DigestLimit{Rate: 1000, Burst: 2000}
		ins := baselineInputs()
		resp, err := m.cl.Digest(t.Context(), digestReq("d-1", ins...))
		if err != nil {
			t.Fatal(err)
		}
		// A second request answers the same: the first left the cached key as it was.
		again, err := m.cl.Digest(t.Context(), digestReq("d-1", ins...))
		if err != nil || !slices.EqualFunc(again.Msg.GetDigests(), resp.Msg.GetDigests(), bytes.Equal) {
			t.Fatalf("a second Digest = %v, %v; want the first's digests", again, err)
		}
		if resp.Msg.GetDigestKeyId() != "d-1" || len(resp.Msg.GetDigests()) != len(ins) {
			t.Fatalf("Digest = %v", resp.Msg)
		}
		for i, in := range ins {
			want, err := redact.Digest(digestMaterial, in.GetDomain(), in.GetOriginal())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(resp.Msg.GetDigests()[i], want) {
				t.Errorf("input %d (%s): digest %x, want %x", i, in.GetKind(), resp.Msg.GetDigests()[i], want)
			}
		}
		rows, err := c.AuditByActor(t.Context(), "scorer")
		if err != nil || len(rows) != 2 {
			t.Fatalf("audit rows %v, %v; want one a request", rows, err)
		}
		allow := regexp.MustCompile(`^digest key d-1, 3 inputs, domains [^,]+(, [^,]+){2}$`)
		if r := rows[0]; r.Action != "digest" || r.Capture != nil || !allow.MatchString(r.Detail) {
			t.Errorf("audit row %+v holds more than the allowlist", r)
		}
		for i, in := range ins {
			if strings.Contains(rows[0].Detail, string(in.GetOriginal())) || strings.Contains(rows[0].Detail, hex.EncodeToString(resp.Msg.GetDigests()[i])) {
				t.Errorf("audit row %q holds input %d's original or digest", rows[0].Detail, i)
			}
		}
	})
}

// Every refusal refuses the whole request and digests nothing: no row is written.
func TestDigestRefusals(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	m := newMasked(t, c)
	m.srv.DigestLimit = &DigestLimit{Rate: 1e9, Burst: 1 << 30}
	good := baselineInputs()
	with := func(f func(in *tkv1.DigestInput)) []*tkv1.DigestInput {
		ins := []*tkv1.DigestInput{good[1], {Domain: good[0].GetDomain(), Kind: good[0].GetKind(), Original: good[0].GetOriginal()}}
		f(ins[1])

		return ins
	}
	many := make([]*tkv1.DigestInput, maxDigestInputs+1)
	for i := range many {
		many[i] = good[1]
	}
	huge := []*tkv1.DigestInput{{Domain: redact.DomainMessageText, Kind: kindText, Original: make([]byte, maxDigestBytes+1)}}
	for name, tc := range map[string]struct {
		keyID string
		ins   []*tkv1.DigestInput
		code  connect.Code
	}{
		"no input":                  {"d-1", nil, connect.CodeInvalidArgument},
		"too many inputs":           {"d-1", many, connect.CodeInvalidArgument},
		"too many bytes":            {"d-1", huge, connect.CodeInvalidArgument},
		"an undeclared key id":      {"d-9", good, connect.CodeInvalidArgument},
		"a master key id":           {"m-1", good, connect.CodeInvalidArgument},
		"an unknown kind":           {"d-1", with(func(in *tkv1.DigestInput) { in.Kind = "record" }), connect.CodeInvalidArgument},
		"an item domain of no rule": {"d-1", with(func(in *tkv1.DigestInput) { in.Domain = "nobody's" }), connect.CodeInvalidArgument},
		"an item with a trailer":    {"d-1", with(func(in *tkv1.DigestInput) { in.Original = append(bytes.Clone(in.Original), 0) }), connect.CodeInvalidArgument},
		"a truncated item":          {"d-1", with(func(in *tkv1.DigestInput) { in.Original = in.Original[:len(in.Original)-1] }), connect.CodeInvalidArgument},
		"a text's other domain":     {"d-1", with(func(in *tkv1.DigestInput) { in.Kind = kindText }), connect.CodeInvalidArgument},
		"an annotation of no rule":  {"d-1", with(func(in *tkv1.DigestInput) { in.Kind = kindAnnotation }), connect.CodeInvalidArgument},
	} {
		_, err := m.cl.Digest(t.Context(), digestReq(tc.keyID, tc.ins...))
		if connect.CodeOf(err) != tc.code {
			t.Errorf("%s: Digest = %v, want %v", name, err, tc.code)
		}
		if err != nil && strings.Contains(err.Error(), "a recipe body") {
			t.Errorf("%s: the error holds an original: %v", name, err)
		}
	}
	// A key id only a staged configuration names is none an active or bound one names.
	staged := maskingContent(t, m.keys)
	staged.DigestKey = &procconfig.KeyRef{ID: "d-2", Fingerprint: "unbound"}
	if _, err := c.StageConfig(t.Context(), catalog.Staging{Content: staged.Encode()}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.cl.Digest(t.Context(), digestReq("d-2", good...)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("Digest under a staged configuration's key id = %v, want invalid", err)
	}
	_, err := m.cl.Digest(t.Context(), withToken(&tkv1.DigestRequest{DigestKeyId: "d-1", Inputs: good}, restorerToken))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("Digest as a restorer = %v, want permission denied", err)
	}
	m.srv.Keys = failingKeys{fmt.Errorf("%w: d-1", keys.ErrUnknownKey)}
	if _, err := m.cl.Digest(t.Context(), digestReq("d-1", good...)); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("Digest with its key refused = %v, want failed precondition", err)
	}
	m.srv.Keys = failingKeys{errors.New("provider down")}
	if _, err := m.cl.Digest(t.Context(), digestReq("d-1", good...)); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("Digest with its key unavailable = %v, want unavailable", err)
	}
	m.srv.Keys = m.keys
	m.srv.Catalog = failingAudit{c}
	if resp, err := m.cl.Digest(t.Context(), digestReq("d-1", good...)); connect.CodeOf(err) != connect.CodeInternal || resp != nil {
		t.Errorf("Digest with a failing audit = %v, %v; want nothing", resp, err)
	}
	m.srv.Catalog = c
	if rows, err := c.AuditByActor(t.Context(), "scorer"); err != nil || len(rows) != 0 {
		t.Errorf("refusals wrote audit rows %v, %v", rows, err)
	}
}

// The limit counts inputs per principal: a request beyond the bucket is refused whole, one within it passes,
// and a nil limit refuses every request.
func TestDigestRateLimit(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	m := newMasked(t, c)
	text := baselineInputs()[1]
	if _, err := m.cl.Digest(t.Context(), digestReq("d-1", text)); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("Digest without a limit = %v, want refused", err)
	}
	m.srv.DigestLimit = &DigestLimit{Rate: 1e-9, Burst: 3}
	if _, err := m.cl.Digest(t.Context(), digestReq("d-1", text, text, text, text)); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("4 inputs on a burst of 3 = %v, want refused", err)
	}
	if _, err := m.cl.Digest(t.Context(), digestReq("d-1", text, text, text)); err != nil {
		t.Errorf("3 inputs on a burst of 3: %v", err)
	}
	if _, err := m.cl.Digest(t.Context(), digestReq("d-1", text)); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("an input past the burst = %v, want refused", err)
	}
	if !m.srv.DigestLimit.allow("another", 3) {
		t.Error("another principal's bucket was taken")
	}
}

func TestOneItem(t *testing.T) {
	t.Parallel()
	nested := maskingtest.ASCII("x")
	for range maxItemDepth {
		nested = maskingtest.List(nested)
	}
	for name, tc := range map[string]struct {
		b    []byte
		want bool
	}{
		"an ascii":               {maskingtest.ASCII("abc"), true},
		"an empty list":          {maskingtest.List(), true},
		"a nested list":          {maskingtest.List(maskingtest.List(maskingtest.ASCII("a")), maskingtest.Item(0o10, 1)), true},
		"nested to the limit":    {nested, true},
		"nested past the limit":  {maskingtest.List(nested), false},
		"empty":                  {nil, false},
		"no length bytes":        {[]byte{0o20 << 2}, false},
		"a short length":         {[]byte{0o20<<2 | 2, 0}, false},
		"a short body":           {[]byte{0o20<<2 | 1, 3, 'a'}, false},
		"two items":              {append(maskingtest.ASCII("a"), maskingtest.ASCII("b")...), false},
		"a list missing an item": {[]byte{0o00<<2 | 1, 2, 0o20<<2 | 1, 0}, false},
		"an undefined format":    {[]byte{0xFD, 0}, false},
		"a short U4":             {[]byte{0xB1, 1, 0}, false},
		"a U4":                   {[]byte{0xB1, 4, 0, 0, 0, 1}, true},
		"a localized header cut": {[]byte{0x49, 1, 0}, false},
		"a localized string":     {[]byte{0x49, 3, 0, 1, 'a'}, true},
		"a huge list count":      {[]byte{0o00<<2 | 3, 0xFF, 0xFF, 0xFF}, false},
	} {
		if got := oneItem(tc.b); got != tc.want {
			t.Errorf("%s: oneItem(%x) = %v, want %v", name, tc.b, got, tc.want)
		}
	}
}

// oneItem never panics, and what it accepts go-secs decodes as one item.
func FuzzOneItem(f *testing.F) {
	for _, seed := range [][]byte{
		maskingtest.ASCII("abc"), maskingtest.List(maskingtest.ASCII("a"), maskingtest.Item(0o10, 1)), {0xB1, 4, 0, 0, 0, 1}, {0x49, 1, 0}, {0xFD, 0},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if oneItem(b) {
			if _, err := secs2.Decode(b); err != nil {
				t.Errorf("oneItem accepted %x, which go-secs refuses: %v", b, err)
			}
		}
	})
}

// A request that does not decode is refused without quoting it: a bad base64 original, in either JSON content type,
// and a truncated binary message, its error naming none of its bytes, and nothing logged that holds them.
func TestUndecodableRequestQuotesNothing(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	m := newMasked(t, c)
	m.srv.DigestLimit = &DigestLimit{Rate: 1e9, Burst: 1 << 30}
	hs := httptest.NewServer(m.srv.Handler())
	t.Cleanup(hs.Close)
	const sentinel = "SYNTHETIC RECIPE!"
	for _, tc := range []struct{ contentType, body string }{
		{"application/json", `{"digestKeyId":"d-1","inputs":[{"domain":"message-text","kind":"text","original":"` + sentinel + `"}]}`},
		{"application/json; charset=utf-8", `{"digestKeyId":"d-1","inputs":[{"domain":"` + sentinel + `","kind":"text","original":"` + sentinel + `"}]}`},
		{"application/json", `{"digestKeyId":"` + sentinel + `","inputs":[{"original":` + sentinel + `}]}`},
		{"application/proto", "\x12\x20\x0a\x02" + sentinel},
	} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, hs.URL+tracekeepv1connect.MachineServiceDigestProcedure, strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", tc.contentType)
		req.Header.Set("Authorization", "Bearer "+scorerToken)
		resp, err := hs.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusBadRequest || bytes.Contains(body, []byte(sentinel)) || bytes.Contains(body, []byte("SYNTHETIC")) {
			t.Errorf("%s %q = %d %s; want refused as invalid, quoting nothing", tc.contentType, tc.body, resp.StatusCode, body)
		}
	}
	if strings.Contains(m.logs.String(), "SYNTHETIC") {
		t.Errorf("the log holds a request's bytes: %s", m.logs.String())
	}
}

// A request past the transport's bound is refused before it is decoded, before the byte check or a key is resolved.
func TestOversizedRequestRefusedBeforeDecoding(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	m := newMasked(t, c)
	m.srv.DigestLimit = &DigestLimit{Rate: 1e9, Burst: 1 << 30}
	m.srv.Keys = failingKeys{errors.New("no key may be resolved")}
	in := &tkv1.DigestInput{Domain: redact.DomainMessageText, Kind: kindText, Original: make([]byte, maxRequestBytes)}
	if _, err := m.cl.Digest(t.Context(), digestReq("d-1", in)); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("a request past the bound = %v, want refused before decoding", err)
	}
	if rows, err := c.AuditByActor(t.Context(), "scorer"); err != nil || len(rows) != 0 {
		t.Errorf("audit rows %v, %v", rows, err)
	}
}
