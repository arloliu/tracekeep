package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack/redact"
	"golang.org/x/time/rate"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/policy"
	"github.com/arloliu/tracekeep/internal/procconfig"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// Bounds of one Digest request (masking-keys.md §9, T-44).
const (
	maxDigestInputs = 1000
	maxDigestBytes  = 8 << 20
)

// Input kinds of Digest, the three forms redact.Digest defines.
const (
	kindItem       = "item"
	kindText       = "text"
	kindAnnotation = "annotation"
)

// Outcomes of a digest request, the label of digests_total.
const (
	digestOK          = "ok"
	digestInvalid     = "invalid"
	digestLimited     = "rate-limited"
	digestKeyRefused  = "key-refused"
	digestUnavailable = "unavailable"
	digestAuditFailed = "audit-failed"
)

// DigestLimit is the rate limit of Digest: Rate inputs a second and a burst of Burst, per principal subject, on this replica.
type DigestLimit struct {
	Rate  float64
	Burst int

	mu       sync.Mutex
	limiters map[string]*rate.Limiter
}

// allow takes n inputs of subject's bucket, or none when they are not all there; a nil limit allows nothing.
func (l *DigestLimit) allow(subject string, n int) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limiters == nil {
		l.limiters = map[string]*rate.Limiter{}
	}
	lim, ok := l.limiters[subject]
	if !ok {
		// shortcut: one bucket per subject kept for the process's life; fine for the few service principals that score.
		lim = rate.NewLimiter(rate.Limit(l.Rate), l.Burst)
		l.limiters[subject] = lim
	}

	return lim.AllowN(time.Now(), n)
}

// Digest implements tracekeepv1connect.MachineServiceHandler (masking-keys.md §9 *Digest*):
// every input checked against a rule of the key id's configurations, the request counted against its principal's limit,
// the key resolved, every input digested, the audit row written, and only then the answer.
func (s *Server) Digest(ctx context.Context, req *connect.Request[tkv1.DigestRequest]) (*connect.Response[tkv1.DigestResponse], error) {
	resp, outcome, err := s.digestInputs(ctx, req.Msg)
	s.Metrics.Digest(outcome, len(req.Msg.GetInputs()))

	return resp, err
}

func (s *Server) digestInputs(ctx context.Context, m *tkv1.DigestRequest) (*connect.Response[tkv1.DigestResponse], string, error) {
	keyID, inputs := m.GetDigestKeyId(), m.GetInputs()
	if err := checkBounds(inputs); err != nil {
		return nil, digestInvalid, err
	}
	rules, err := s.digestRules(ctx, keyID)
	if err != nil {
		return nil, digestUnavailable, connect.NewError(connect.CodeUnavailable, fmt.Errorf("digest: %w", err))
	}
	if rules == nil {
		return nil, digestInvalid, invalid("digest_key_id %q is no active or bound configuration's digest key", keyID)
	}
	for i, in := range inputs {
		if err := rules.check(in); err != nil {
			return nil, digestInvalid, invalid("input %d: %v", i, err)
		}
	}
	p, _ := PrincipalFrom(ctx)
	if !s.DigestLimit.allow(p.Subject, len(inputs)) {
		return nil, digestLimited, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf("digest: %d inputs exceed the rate limit", len(inputs)))
	}
	key, err := s.Keys.Digest(ctx, keyID)
	if keys.Refused(err) {
		return nil, digestKeyRefused, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("digest: %w", err))
	} else if err != nil {
		return nil, digestUnavailable, connect.NewError(connect.CodeUnavailable, fmt.Errorf("digest: %w", err))
	}
	// key is the keys module's cached material, shared by every use: never cleared or changed here.
	out := &tkv1.DigestResponse{DigestKeyId: keyID, Digests: make([][]byte, len(inputs))}
	domains := make([]string, 0, len(inputs))
	for i, in := range inputs {
		// The domain was checked against a rule, which Compile validated; a refusal here is a defect.
		if out.Digests[i], err = redact.Digest(key, in.GetDomain(), in.GetOriginal()); err != nil {
			return nil, digestInvalid, invalid("input %d: %v", i, err)
		}
		domains = append(domains, in.GetDomain())
	}
	slices.Sort(domains)
	entry := catalog.AuditEntry{
		Actor: p.Subject, Action: "digest",
		Detail: fmt.Sprintf("digest key %s, %d inputs, domains %s", keyID, len(inputs), strings.Join(slices.Compact(domains), ", ")),
	}
	if err := s.Catalog.Audit(ctx, entry); err != nil {
		return nil, digestAuditFailed, connect.NewError(connect.CodeInternal, fmt.Errorf("digest: audit: %w", err))
	}

	return connect.NewResponse(out), digestOK, nil
}

// checkBounds refuses an empty request and one beyond the request bounds.
func checkBounds(inputs []*tkv1.DigestInput) error {
	if len(inputs) == 0 || len(inputs) > maxDigestInputs {
		return invalid("%d inputs, want 1 to %d", len(inputs), maxDigestInputs)
	}
	total := 0
	for _, in := range inputs {
		total += len(in.GetOriginal())
	}
	if total > maxDigestBytes {
		return invalid("%d bytes of originals, want at most %d", total, maxDigestBytes)
	}

	return nil
}

// digestRules are the domains a digest key id's configurations name, by kind.
type digestRules struct {
	item, annotation map[string]bool
}

// digestRules gathers the rules of every active or bound configuration whose digest key id is keyID; nil when there is none.
func (s *Server) digestRules(ctx context.Context, keyID string) (*digestRules, error) {
	infos, err := s.Catalog.Configs(ctx)
	if err != nil {
		return nil, err
	}
	var out *digestRules
	for _, ci := range infos {
		if active := ci.ActivatedAt.Valid && !ci.RetiredAt.Valid; !active && !ci.Bound {
			continue
		}
		c, err := procconfig.Decode(ci.Content)
		if err != nil || c.DigestKey == nil || c.DigestKey.ID != keyID || c.Policy == nil {
			continue // a policy-less configuration, or one this binary cannot read, names no digest key
		}
		sp, ok, err := s.Catalog.Policy(ctx, c.Policy.ID, c.Policy.Version)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("configuration %d names the unstored policy %s version %d", ci.ID, c.Policy.ID, c.Policy.Version)
		}
		p, err := policy.FromCanonical(sp.Body)
		if err != nil {
			return nil, fmt.Errorf("configuration %d: %w", ci.ID, err)
		}
		if out == nil {
			out = &digestRules{item: map[string]bool{}, annotation: map[string]bool{}}
		}
		for _, r := range p.ItemRules {
			out.item[r.Domain] = true
		}
		for _, r := range p.AnnotationRules {
			out.annotation[r.Domain] = true
		}
	}

	return out, nil
}

var errInput = errors.New("refused")

// check refuses an input whose kind and domain are no rule's, or an item that is not exactly one encoded SECS-II item.
func (r *digestRules) check(in *tkv1.DigestInput) error {
	d := in.GetDomain()
	switch in.GetKind() {
	case kindItem:
		if !r.item[d] {
			return fmt.Errorf("%w: domain %q is no item rule's", errInput, d)
		}
		if !oneItem(in.GetOriginal()) {
			return fmt.Errorf("%w: the original is not exactly one encoded SECS-II item", errInput)
		}
	case kindText:
		if d != redact.DomainMessageText {
			return fmt.Errorf("%w: a text's domain is %q", errInput, redact.DomainMessageText)
		}
	case kindAnnotation:
		if !r.annotation[d] {
			return fmt.Errorf("%w: domain %q is no annotation rule's", errInput, d)
		}
	default:
		return fmt.Errorf("%w: kind %q, want item, text or annotation", errInput, in.GetKind())
	}

	return nil
}

// maxItemDepth bounds the lists oneItem descends, as the masking walker's depth limit does.
const maxItemDepth = 64

// elementSize is the element size of every format code SEMI E5 §9.2 defines but the list (0o00);
// 0o22 is a localized string, whose non-empty body starts with a 2-byte header.
var elementSize = map[byte]int{
	0o10: 1, 0o11: 1, 0o20: 1, 0o21: 1, 0o22: 1,
	0o30: 8, 0o31: 1, 0o32: 2, 0o34: 4,
	0o40: 8, 0o44: 4,
	0o50: 8, 0o51: 1, 0o52: 2, 0o54: 4,
}

// oneItem reports whether b is exactly one valid SECS-II item's encoding (E5 §9.2), as the masking walker validates one:
// a format byte of a defined format code with one to three length bytes, then a list's elements, as many as its length,
// or a body of its length, a whole number of elements, a localized string's never one byte alone; nothing after it.
func oneItem(b []byte) bool {
	n, ok := itemLen(b, 0)

	return ok && n == len(b)
}

// itemLen is the length of the valid item b starts with, at list depth depth.
func itemLen(b []byte, depth int) (int, bool) {
	if len(b) < 1 || depth > maxItemDepth {
		return 0, false
	}
	nlb := int(b[0] & 0x03)
	if nlb == 0 || len(b) < 1+nlb {
		return 0, false
	}
	length := 0
	for _, c := range b[1 : 1+nlb] {
		length = length<<8 | int(c)
	}
	off, code := 1+nlb, b[0]>>2
	if code != 0 {
		size, known := elementSize[code]
		if !known || length%size != 0 || code == 0o22 && length == 1 || len(b)-off < length {
			return 0, false
		}

		return off + length, true
	}
	// Every element takes at least its format byte and one length byte.
	if length > (len(b)-off)/2 {
		return 0, false
	}
	for range length {
		n, ok := itemLen(b[off:], depth+1)
		if !ok {
			return 0, false
		}
		off += n
	}

	return off, true
}
