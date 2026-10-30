package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/redact"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/procconfig"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// Keys is what the API resolves of the keys module; a *keys.Keys is one.
type Keys interface {
	Encryption(ctx context.Context, capture tracepack.UUID, keyID string) ([]byte, error)
	Digest(ctx context.Context, keyID string) ([]byte, error)
}

// Outcomes of a restore, the label of restores_total.
const (
	restoreOK             = "ok"
	restoreNotFound       = "not-found"
	restoreConflict       = "conflict"
	restoreNotRestorable  = "not-restorable"
	restoreKeyRefused     = "key-refused"
	restoreUnavailable    = "unavailable"
	restoreAuditFailed    = "audit-failed"
	restoreInvalidRequest = "invalid"
)

// closeSentinel is the close record's seq, never a record a restore can name.
const closeSentinel = uint64(1)<<63 - 1

// Restore implements tracekeepv1connect.MachineServiceHandler (masking-keys.md §9 *Restore*):
// the record's copies read from one catalog snapshot, every one of them compared whatever hour is named,
// the original restored in memory, the audit row written, and only then the answer.
// An error names the request and the reason, never a byte of the record.
func (s *Server) Restore(ctx context.Context, req *connect.Request[tkv1.RestoreRequest]) (*connect.Response[tkv1.RestoreResponse], error) {
	resp, outcome, err := s.restoreRecord(ctx, req.Msg)
	s.Metrics.Restore(outcome)

	return resp, err
}

func (s *Server) restoreRecord(ctx context.Context, m *tkv1.RestoreRequest) (*connect.Response[tkv1.RestoreResponse], string, error) {
	capture, err := tracepack.ParseUUID(m.GetCaptureId())
	if err != nil {
		return nil, restoreInvalidRequest, invalid("capture_id %q: %v", m.GetCaptureId(), err)
	}
	seq, h := m.GetSeq(), m.GetHour()
	if seq >= closeSentinel {
		return nil, restoreInvalidRequest, invalid("seq %d is no record's", seq)
	}
	if h == nil || h.CheckValid() != nil || h.GetNanos() != 0 || h.GetSeconds()%int64(time.Hour/time.Second) != 0 {
		return nil, restoreInvalidRequest, invalid("hour is not an hour start")
	}
	hour := h.GetSeconds() / int64(time.Hour/time.Second)
	at := fmt.Sprintf("%s seq %d", capture, seq)

	// Whether the named hour holds a copy is answered first, whatever the capture's configuration.
	copies, err := s.Catalog.RecordCopies(ctx, capture, seq)
	if err != nil {
		return nil, restoreUnavailable, connect.NewError(connect.CodeUnavailable, fmt.Errorf("restore %s: %w", at, err))
	}
	named := slices.IndexFunc(copies, func(c catalog.Copy) bool { return c.Hour == hour })
	if named < 0 {
		return nil, restoreNotFound, connect.NewError(connect.CodeNotFound, fmt.Errorf("restore %s: no copy in hour %d", at, hour))
	}
	s.stalled("copies")
	// Every copy is read and compared before anything about the capture's configuration is answered.
	v, err := s.version(ctx, capture, seq, copies)
	switch {
	case errors.Is(err, errConflict):
		return nil, restoreConflict, conflict(capture, seq)
	case err != nil:
		return nil, restoreUnavailable, connect.NewError(connect.CodeUnavailable, fmt.Errorf("restore %s: %w", at, err))
	}
	keyID, err := s.encryptionKeyID(ctx, capture)
	switch {
	case errors.Is(err, errNotRestorable):
		return nil, restoreNotRestorable, notRestorable(at, err)
	case err != nil:
		return nil, restoreUnavailable, connect.NewError(connect.CodeUnavailable, fmt.Errorf("restore %s: %w", at, err))
	}
	if err := v.restorable(keyID); err != nil {
		return nil, restoreNotRestorable, notRestorable(at, err)
	}
	key, err := s.Keys.Encryption(ctx, capture, keyID)
	if keys.Refused(err) {
		return nil, restoreKeyRefused, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("restore %s: %w", at, err))
	} else if err != nil {
		return nil, restoreUnavailable, connect.NewError(connect.CodeUnavailable, fmt.Errorf("restore %s: %w", at, err))
	}
	original, err := redact.Restore(capture, &v.record, &v.marking, v.policy, key)
	clear(key)
	if err != nil {
		return nil, restoreNotRestorable, notRestorable(at, err)
	}
	p, _ := PrincipalFrom(ctx)
	entry := catalog.AuditEntry{
		Actor: p.Subject, Action: "restore", Capture: &capture,
		Detail: fmt.Sprintf("seq %d, hour %d, pack %s, encryption key %s", seq, hour, copies[named].Pack, keyID),
	}
	if err := s.Catalog.Audit(ctx, entry); err != nil {
		clear(original)
		return nil, restoreAuditFailed, connect.NewError(connect.CodeInternal, fmt.Errorf("restore %s: audit: %w", at, err))
	}

	return connect.NewResponse(&tkv1.RestoreResponse{Payload: original, Time: ts(v.record.TSUTCNs), Kind: v.record.Kind.String()}), restoreOK, nil
}

var (
	errNotRestorable = errors.New("not restorable")
	errConflict      = errors.New("copies differ")
)

// encryptionKeyID is the encryption key id of capture's bound configuration, which holds a copy of a record;
// one bound to a configuration without encryption is not restorable.
func (s *Server) encryptionKeyID(ctx context.Context, capture tracepack.UUID) (string, error) {
	c, err := s.Catalog.Capture(ctx, capture)
	if err != nil {
		return "", err
	}
	if c.ConfigID == 0 {
		return "", fmt.Errorf("%w: the capture is bound to no configuration", errNotRestorable)
	}
	b, err := s.Catalog.ConfigContent(ctx, c.ConfigID)
	if err != nil {
		return "", err
	}
	content, err := procconfig.Decode(b)
	if err != nil {
		return "", err
	}
	if content.EncryptionKey == nil {
		return "", fmt.Errorf("%w: the capture's configuration does not encrypt", errNotRestorable)
	}

	return content.EncryptionKey.ID, nil
}

// copyOf is one pack's copy of a record: the record as stored, its Marking, the seq's entries and the pack's policy.
type copyOf struct {
	record     tracepack.Record
	extra      []byte
	marking    tracepack.RecordRedaction
	entries    []tracepack.RedactionEntry
	policy     *tracepack.RedactionPolicy
	unobserved bool
}

// version reads every copy and returns the one version they hold, or errConflict when two differ.
// A copy that cannot be opened or read, or that does not hold the seq, fails the read.
// The stored records are compared by tracepack's MergeIterate, which compares every byte of the record header as stored and the payload,
// record_flags whole and the header extension included; the seq's redaction entries and the packs' policies are compared here.
func (s *Server) version(ctx context.Context, capture tracepack.UUID, seq uint64, copies []catalog.Copy) (*copyOf, error) {
	readers := make([]*tracepack.Reader, 0, len(copies))
	objs := make([]store.Object, 0, len(copies))
	defer func() {
		for _, o := range objs {
			_ = o.Close()
		}
	}()
	var first *copyOf
	from, to := int64(math.MaxInt64), int64(math.MinInt64)
	for _, c := range copies {
		obj, r, err := s.openCopy(ctx, capture, c)
		if err != nil {
			return nil, err
		}
		objs, readers = append(objs, obj), append(readers, r)
		v, err := readCopy(ctx, r, seq, c)
		if err != nil {
			return nil, err
		}
		from, to = min(from, v.record.TSUTCNs), max(to, v.record.TSUTCNs)
		if first == nil {
			first = v
		} else if !first.equal(v) {
			return nil, errConflict
		}
	}
	if len(readers) < 2 {
		return first, nil
	}
	q := tracepack.Query{Filter: tracepack.Filter{TimeFrom: &from}}
	if to < math.MaxInt64 {
		to++
		q.Filter.TimeTo = &to // a record at the last instant leaves the range unbounded above
	}
	res, err := tracepack.MergeIterate(ctx, readers, q, tracepack.MergeIterateOptions{Order: tracepack.OrderCapture},
		func(*tracepack.Item) error { return nil })
	if err != nil {
		return nil, fmt.Errorf("compare the copies: %w", err)
	}
	for _, d := range res.Incomplete {
		if d.Reason != tracepack.ReasonCoverage && d.Reason != tracepack.ReasonCrashGap {
			return nil, fmt.Errorf("compare the copies: incomplete: %v", d.Reason)
		}
	}
	for _, c := range res.Conflicts {
		if c.CaptureID == capture && c.Seq == seq {
			return nil, errConflict
		}
	}

	return first, nil
}

// equal compares two copies' decoded record, header extension, seq's entries and pack policy;
// MergeIterate compares what decoding drops.
func (v *copyOf) equal(o *copyOf) bool {
	return reflect.DeepEqual(v.record, o.record) && slices.Equal(v.extra, o.extra) && v.unobserved == o.unobserved &&
		reflect.DeepEqual(v.entries, o.entries) && reflect.DeepEqual(v.policy, o.policy)
}

// openCopy opens c's pack, refusing one of another capture; the caller closes the object.
func (s *Server) openCopy(ctx context.Context, capture tracepack.UUID, c catalog.Copy) (store.Object, *tracepack.Reader, error) {
	obj, err := s.Store.Open(ctx, c.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("open pack %s: %w", c.Pack, err)
	}
	r, err := tracepack.Open(ctx, obj, obj.Size(), tracepack.ReaderOptions{})
	if err == nil && r.Header().CaptureID != capture {
		err = errors.New("of another capture")
	}
	if err != nil {
		_ = obj.Close()
		return nil, nil, fmt.Errorf("open pack %s: %w", c.Pack, err)
	}

	return obj, r, nil
}

// readCopy reads seq's record from r, c's pack, iterating only the blocks whose times meet the block indexed as holding it.
func readCopy(ctx context.Context, r *tracepack.Reader, seq uint64, c catalog.Copy) (*copyOf, error) {
	var q tracepack.Query
	q.Payloads = true
	for _, b := range r.Blocks() {
		if b.Indexed && b.FirstSeq <= seq && seq <= b.LastSeq && b.TSMax < math.MaxInt64 {
			to := b.TSMax + 1
			q.Filter.TimeFrom, q.Filter.TimeTo = &b.TSMin, &to
			break
		}
	}
	var found []*copyOf
	res, err := r.Iterate(ctx, q, func(it *tracepack.Item) error {
		if it.Record.Seq != seq {
			return nil
		}
		v := &copyOf{record: it.Record, extra: slices.Clone(it.HeaderExtra), unobserved: r.Unobserved(&it.Record)}
		v.record.Payload = slices.Clone(it.Record.Payload)
		if it.Redaction != nil {
			v.marking = *it.Redaction
			v.marking.Entries = slices.Clone(it.Redaction.Entries)
		}
		found = append(found, v)

		return nil
	})
	if err == nil && len(res.Incomplete) > 0 && len(found) == 0 {
		err = fmt.Errorf("incomplete: %v", res.Incomplete[0].Reason)
	}
	switch {
	case err != nil:
		return nil, fmt.Errorf("read pack %s: %w", c.Pack, err)
	case len(found) == 0:
		return nil, fmt.Errorf("pack %s does not hold the seq", c.Pack)
	case len(found) > 1:
		return nil, errConflict
	}
	v := found[0]
	if meta := r.Header().Meta; meta != nil {
		v.policy = meta.RedactionPolicy
		for _, e := range meta.Redaction {
			if e.Seq == seq {
				v.entries = append(v.entries, e)
			}
		}
	}

	return v, nil
}

// restorable refuses what holds no original to restore, and a pack naming another encryption key id than the capture's configuration:
// the service trusts no value a pack names.
func (v *copyOf) restorable(keyID string) error {
	switch {
	case v.record.Kind == tracepack.KindAnnotation:
		return fmt.Errorf("%w: an annotation's masks keep no original", errNotRestorable)
	case v.record.Kind != tracepack.KindData:
		return fmt.Errorf("%w: a %v record is never masked", errNotRestorable, v.record.Kind)
	case v.unobserved:
		return fmt.Errorf("%w: the record is unobserved", errNotRestorable)
	case !v.record.Quality.Has(tracepack.QualityRedacted) && len(v.marking.Entries) == 0:
		return fmt.Errorf("%w: the record is not masked", errNotRestorable)
	case v.policy == nil || v.policy.EncryptionKeyID == nil:
		return fmt.Errorf("%w: the pack carries no encryption", errNotRestorable)
	case *v.policy.EncryptionKeyID != keyID:
		return fmt.Errorf("%w: the pack names another encryption key id than the capture's configuration", errNotRestorable)
	}

	return nil
}

// notRestorable is the refusal of a record holding no original to restore, or one that does not authenticate.
func notRestorable(at string, err error) error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("restore %s: not restorable: %w", at, err))
}

// conflict is the refusal of differing copies: failed precondition with the RecordConflict detail.
func conflict(capture tracepack.UUID, seq uint64) error {
	err := connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("restore %s seq %d: copies differ", capture, seq))
	if detail, derr := connect.NewErrorDetail(&tkv1.RecordConflict{CaptureId: capture.String(), Seq: seq}); derr == nil {
		err.AddDetail(detail)
	}

	return err
}
