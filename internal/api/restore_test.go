package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/redact"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/policy"
	"github.com/arloliu/tracekeep/internal/procconfig"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// masked is a world whose captures bind the baseline's masking configuration under d-1 and m-1, its packs in a store.
type masked struct {
	*world
	st       *store.Mem
	keys     *keys.Keys
	compiled *redact.Compiled
	cfg      int64
	logs     *bytes.Buffer
	// patch, when set, rewrites the next pack put writes, which it writes uncompressed.
	patch func([]byte) []byte
}

// maskingKeys declares d-1 (digest) and m-1 (master) as 0600 files under t's directory, resolved through c's bindings.
func maskingKeys(t *testing.T, c *catalog.Catalog) *keys.Keys {
	t.Helper()
	dir := t.TempDir()
	declared := map[string]keys.Declaration{}
	for id, kind := range map[string]string{"d-1": catalog.KindDigest, "m-1": catalog.KindMaster} {
		path := filepath.Join(dir, id)
		if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte(id[:1]), 32))), 0o600); err != nil {
			t.Fatal(err)
		}
		declared[id] = keys.Declaration{Provider: keys.ProviderFile, Ref: path, Kind: kind}
	}
	k, err := keys.New(keys.Config{Declared: declared}, c)
	if err != nil {
		t.Fatal(err)
	}

	return k
}

// maskingContent is the baseline's masking configuration under d-1 and m-1, as ingest stages it.
func maskingContent(t *testing.T, k *keys.Keys) procconfig.Content {
	t.Helper()
	b := policy.Baseline()
	_, sum := b.Canonical()
	content := procconfig.Content{
		Classifier: "test", MaxFrameLen: 1 << 20, Policy: &procconfig.PolicyRef{ID: b.ID, Version: int64(b.Version), SHA256: hex.EncodeToString(sum[:])}, //nolint:gosec // the baseline's version
		FormatMinor: procconfig.MaskingFormatMinor, Masker: procconfig.Masker,
	}
	for _, ref := range []**procconfig.KeyRef{&content.DigestKey, &content.EncryptionKey} {
		*ref = &procconfig.KeyRef{}
	}
	content.DigestKey.ID, content.EncryptionKey.ID = "d-1", "m-1"
	for _, kr := range []*procconfig.KeyRef{content.DigestKey, content.EncryptionKey} {
		kind := catalog.KindDigest
		if kr == content.EncryptionKey {
			kind = catalog.KindMaster
		}
		fp, err := k.Fingerprint(t.Context(), kr.ID, kind)
		if err != nil {
			t.Fatal(err)
		}
		kr.Fingerprint = fp
	}

	return content
}

func newMasked(t *testing.T, c *catalog.Catalog) *masked {
	t.Helper()
	k := maskingKeys(t, c)
	b := policy.Baseline()
	compiled, err := b.Compile("d-1", "m-1")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := b.Canonical()
	content := maskingContent(t, k)
	cfg, err := c.StageConfig(t.Context(), catalog.Staging{
		Content: content.Encode(), Policy: &catalog.StoredPolicy{ID: b.ID, Version: int64(b.Version), Body: body}, //nolint:gosec // the baseline's version
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ActivateStaged(t.Context(), cfg, nil, nil); err != nil {
		t.Fatal(err)
	}
	m := &masked{world: newWorld(t, c), st: store.NewMem(), keys: k, compiled: compiled, cfg: cfg, logs: &bytes.Buffer{}}
	m.srv.Store, m.srv.Keys = m.st, k
	m.srv.Log = slog.New(slog.NewTextHandler(m.logs, nil))

	return m
}

// policy is the redaction_policy the configuration's packs name.
func (m *masked) policy() *tracepack.RedactionPolicy {
	p := m.compiled.PackPolicy()

	return &p
}

// lastHour is the hour of the last representable instant.
const lastHour = int64(math.MaxInt64) / nsPerHour

// recordAt is c's record at seq, its time hour's start plus the seq.
func recordAt(c *maskingtest.Carrier, h int64, seq uint64) tracepack.Record {
	r := tracepack.Record{Seq: seq, TSUTCNs: h*nsPerHour + int64(seq), Kind: c.Kind, Payload: c.Payload} //nolint:gosec // small seqs
	if c.Kind == tracepack.KindData {
		r.Dir, r.Fidelity = tracepack.DirHostToEquipment, tracepack.FidelityWireExact
	} else {
		r.Dir, r.Fidelity = tracepack.DirLocal, tracepack.FidelityNotApplicable
	}

	return r
}

// mask masks rs as ingest does, returning the masked records and their entries in seq order.
func (m *masked) mask(t *testing.T, capture tracepack.UUID, rs []tracepack.Record) ([]tracepack.Record, []tracepack.RedactionEntry) {
	t.Helper()
	out := make([]tracepack.Record, 0, len(rs))
	var entries []tracepack.RedactionEntry
	for i := range rs {
		r, es, err := m.compiled.Mask(capture, &rs[i], false, m.keys.ForUse(t.Context()))
		if err != nil {
			t.Fatal(err)
		}
		out, entries = append(out, r), append(entries, es...)
	}

	return out, entries
}

// put writes rs, masked or not, as a segment of capture's hour h under policy, puts it and registers it.
func (m *masked) put(t *testing.T, capture tracepack.UUID, h int64, rs []tracepack.Record, entries []tracepack.RedactionEntry, rp *tracepack.RedactionPolicy) *catalog.Pack {
	t.Helper()
	cp, err := m.c.Capture(t.Context(), capture)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := tracepack.DecodeDescriptor(cp.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	interval := uint64(time.Second)
	meta.Writer, meta.PackRole, meta.SeqStart = "tracekeep-test", tracepack.PackRoleSegment, rs[0].Seq
	meta.PeriodStart, meta.PeriodEnd = h*nsPerHour, (h+1)*nsPerHour
	if h == lastHour {
		meta.PeriodEnd = math.MaxInt64 // the last hour ends at the last instant
	}
	meta.FlushIntervalNs, meta.ScopeGeneration = &interval, new(uint64)
	meta.Classifiers, meta.MaxFrameLens = []string{"test"}, []uint64{1 << 20}
	meta.RedactionPolicy, meta.Redaction = rp, entries
	id := newID(t)
	var buf bytes.Buffer
	facts := tracepack.PackFacts{DurableRecorder: true, AnyRedacted: len(entries) > 0}
	tw, err := tracepack.NewWriter(&buf, tracepack.WriterOptions{Meta: meta, Facts: facts, PackID: id, CaptureID: capture, Uncompressed: m.patch != nil})
	if err != nil {
		t.Fatal(err)
	}
	for i := range rs {
		if err := tw.Append(&rs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	p := &catalog.Pack{
		ID: id, Capture: capture, ToolID: tool, Hour: h, Role: catalog.RoleSegment, Key: "staging/" + id.String(), Size: int64(buf.Len()),
		SeqStart: rs[0].Seq, FormatMinor: 1,
		Ranges: []catalog.Range{{SeqFrom: rs[0].Seq, SeqTo: rs[len(rs)-1].Seq, TSFrom: rs[0].TSUTCNs, TSTo: rs[len(rs)-1].TSUTCNs}},
	}
	if m.patch != nil {
		b := m.patch(buf.Bytes())
		buf.Reset()
		buf.Write(b)
		m.patch = nil
	}
	if err := m.st.Put(t.Context(), p.Key, bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
		t.Fatal(err)
	}
	if out, err := m.c.RegisterPack(t.Context(), p, catalog.Registration{Evidence: &catalog.Evidence{}}); err != nil || out != catalog.Registered {
		t.Fatalf("RegisterPack = %v, %v", out, err)
	}

	return p
}

// carriers are the baseline's carriers.
func carriers(t *testing.T) []maskingtest.Carrier {
	t.Helper()
	b := policy.Baseline()
	cs, err := maskingtest.Carriers(&b)
	if err != nil {
		t.Fatal(err)
	}

	return append(cs, maskingtest.WholeText())
}

// recordsOf are cs as records of hour h from seq 1.
func recordsOf(cs []maskingtest.Carrier, h int64) []tracepack.Record {
	rs := make([]tracepack.Record, len(cs))
	for i := range cs {
		rs[i] = recordAt(&cs[i], h, uint64(i+1)) //nolint:gosec // small seqs
	}

	return rs
}

func restoreReq(capture tracepack.UUID, seq uint64, h int64) *connect.Request[tkv1.RestoreRequest] {
	return withToken(&tkv1.RestoreRequest{CaptureId: capture.String(), Seq: seq, Hour: hourTS(h)}, restorerToken)
}

func (m *masked) restore(t *testing.T, capture tracepack.UUID, seq uint64, h int64) (*tkv1.RestoreResponse, error) {
	t.Helper()
	resp, err := m.cl.Restore(t.Context(), restoreReq(capture, seq, h))
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

// auditDetails are the restorer's audit rows, oldest first, each its action and detail.
func auditDetails(t *testing.T, c *catalog.Catalog) []string {
	t.Helper()
	rows, err := c.AuditByActor(t.Context(), "restorer")
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Action + ": " + r.Detail
	}

	return out
}

// Every masked data carrier restores to the record as received, with one audit row of the allowlisted fields each;
// annotations are refused as not restorable and write no row.
func TestRestoreRoundTrip(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		cs := carriers(t)
		rs := recordsOf(cs, hour)
		ms, entries := m.mask(t, capture, rs)
		p := m.put(t, capture, hour, ms, entries, m.policy())
		restored := 0
		for i := range rs {
			got, err := m.restore(t, capture, rs[i].Seq, hour)
			if cs[i].Kind == tracepack.KindAnnotation {
				if connect.CodeOf(err) != connect.CodeFailedPrecondition || got != nil {
					t.Errorf("%s: Restore of an annotation = %v, %v; want not restorable", cs[i].Name, got, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s: Restore: %v", cs[i].Name, err)
			}
			if !bytes.Equal(got.GetPayload(), rs[i].Payload) || got.GetTime().AsTime().UnixNano() != rs[i].TSUTCNs || got.GetKind() != "data" {
				t.Errorf("%s: restored %x at %v, want the record as received", cs[i].Name, got.GetPayload(), got.GetTime().AsTime())
			}
			restored++
		}
		details := auditDetails(t, c)
		if len(details) != restored || restored == 0 {
			t.Fatalf("%d restore rows for %d restores", len(details), restored)
		}
		allow := regexp.MustCompile(`^restore: seq \d+, hour ` + fmt.Sprint(hour) + `, pack ` + p.ID.String() + `, encryption key m-1$`)
		for _, d := range details {
			if !allow.MatchString(d) {
				t.Errorf("audit row %q holds more than the allowlist", d)
			}
		}
	})
}

// The hour names where to look: a copy in another hour answers not found for this hour;
// equal copies, as a redelivery writes them, restore once; a version in another hour, its time differing, refuses either hour as a conflict.
func TestRestoreAcrossHours(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		cs := carriers(t)[:1]
		rs := recordsOf(cs, hour)
		ms, entries := m.mask(t, capture, rs)
		m.put(t, capture, hour, ms, entries, m.policy())
		if _, err := m.restore(t, capture, 1, hour+1); connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("Restore naming an hour without a copy = %v, want not found", err)
		}
		if _, err := m.restore(t, capture, 2, hour); connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("Restore of a seq no pack holds = %v, want not found", err)
		}
		if _, err := m.restore(t, newID(t), 1, hour); connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("Restore of an unknown capture = %v, want not found", err)
		}
		m.put(t, capture, hour, ms, entries, m.policy())
		if got, err := m.restore(t, capture, 1, hour); err != nil || !bytes.Equal(got.GetPayload(), rs[0].Payload) {
			t.Errorf("Restore of equal copies = %v, %v", got, err)
		}
		vs, oentries := m.mask(t, capture, recordsOf(cs, hour+2))
		m.put(t, capture, hour+2, vs, oentries, m.policy())
		for _, h := range []int64{hour, hour + 2} {
			_, err := m.restore(t, capture, 1, h)
			var ce *connect.Error
			if !errors.As(err, &ce) || ce.Code() != connect.CodeFailedPrecondition || len(ce.Details()) != 1 {
				t.Fatalf("Restore naming hour %d of differing copies = %v, want a conflict", h, err)
			}
			v, derr := ce.Details()[0].Value()
			if rc, ok := v.(*tkv1.RecordConflict); derr != nil || !ok || rc.GetSeq() != 1 || rc.GetCaptureId() != capture.String() {
				t.Errorf("conflict detail = %v, %v", v, derr)
			}
		}
		// An acknowledged conflict refuses it still.
		if _, err := c.AcknowledgeConflict(t.Context(), capture, 1, "operator", "a1"); err != nil {
			t.Fatal(err)
		}
		if _, err := m.restore(t, capture, 1, hour); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("Restore of an acknowledged conflict = %v, want refused", err)
		}
	})
}

// failingKeys refuses or fails every resolution with err.
type failingKeys struct{ err error }

func (k failingKeys) Encryption(context.Context, tracepack.UUID, string) ([]byte, error) {
	return nil, k.err
}
func (k failingKeys) Digest(context.Context, string) ([]byte, error) { return nil, k.err }

// Each refusal answers its code, writes no audit row, and holds no byte of the original in its error or the log.
func TestRestoreRefusals(t *testing.T) {
	t.Parallel()
	c := catalogtest.Open(t, catalog.EngineSQLite)
	m := newMasked(t, c)
	capture := m.admitActive(t)
	cs := carriers(t)[:4]
	rs := recordsOf(cs, hour)
	ms, entries := m.mask(t, capture, rs)
	m.put(t, capture, hour, ms[:2], entries[:countEntries(entries, 2)], m.policy())
	// Seq 3 under a policy naming another encryption key id; seq 4 with a ciphertext byte flipped.
	other := m.policy()
	other.EncryptionKeyID = new("m-2")
	m.put(t, capture, hour, ms[2:3], seqEntries(entries, 3), other)
	tampered := ms[3]
	tampered.Payload = bytes.Clone(tampered.Payload)
	e4 := seqEntries(entries, 4)
	tampered.Payload[e4[0].MaskedRanges[0].Offset] ^= 1
	m.put(t, capture, hour, []tracepack.Record{tampered}, e4, m.policy())
	// A capture bound to a configuration without encryption.
	plainContent := procconfig.Content{Classifier: "test", MaxFrameLen: 1 << 20}
	if _, err := c.ActivateConfig(t.Context(), plainContent.Encode()); err != nil {
		t.Fatal(err)
	}
	plain := m.admitActive(t)
	if cp, err := c.Capture(t.Context(), plain); err != nil || cp.ConfigID == m.cfg {
		t.Fatalf("the plain capture = %+v, %v; want it bound to the policy-less configuration", cp, err)
	}
	m.put(t, plain, hour, rs[:1], nil, nil)
	// Its seq 3 in two differing versions, the second in the next hour; its seq 4 in a copy whose object is gone.
	m.put(t, plain, hour, rs[2:3], nil, nil)
	m.put(t, plain, hour+1, recordsOf(cs, hour+1)[2:3], nil, nil)
	m.put(t, plain, hour, rs[3:4], nil, nil)
	lost := m.put(t, plain, hour, rs[3:4], nil, nil)
	if err := m.st.Delete(t.Context(), lost.Key); err != nil {
		t.Fatal(err)
	}

	var errs []string
	logged := 0
	refused := func(name string, code connect.Code, err error) {
		t.Helper()
		if connect.CodeOf(err) != code {
			t.Errorf("%s = %v, want %v", name, err, code)
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
		// Each refusal the handler answers leaves one line naming the operation and the reason; auth logs its own.
		line := m.logs.String()[logged:]
		if code != connect.CodePermissionDenied &&
			(strings.Count(line, "\n") != 1 || !strings.Contains(line, "operation=Restore") || !strings.Contains(line, code.String())) {
			t.Errorf("%s logged %q, want one line naming Restore and %v", name, line, code)
		}
		logged = m.logs.Len()
	}
	_, err := m.cl.Restore(t.Context(), withToken(&tkv1.RestoreRequest{CaptureId: "nope", Seq: 1, Hour: hourTS(hour)}, restorerToken))
	refused("a capture id that does not parse", connect.CodeInvalidArgument, err)
	_, err = m.cl.Restore(t.Context(), withToken(&tkv1.RestoreRequest{CaptureId: capture.String(), Seq: 1,
		Hour: timestamppb.New(time.Unix(0, hour*nsPerHour+1))}, restorerToken))
	refused("an hour that is no hour start", connect.CodeInvalidArgument, err)
	_, err = m.cl.Restore(t.Context(), withToken(&tkv1.RestoreRequest{CaptureId: capture.String(), Seq: 1}, restorerToken))
	refused("no hour", connect.CodeInvalidArgument, err)
	_, err = m.cl.Restore(t.Context(), withToken(&tkv1.RestoreRequest{CaptureId: capture.String(), Seq: closeSentinel, Hour: hourTS(hour)}, restorerToken))
	refused("the close sentinel", connect.CodeInvalidArgument, err)
	_, err = m.restore(t, capture, 3, hour)
	refused("a pack naming another encryption key id", connect.CodeFailedPrecondition, err)
	_, err = m.restore(t, capture, 4, hour)
	refused("a ciphertext that does not authenticate", connect.CodeFailedPrecondition, err)
	_, err = m.restore(t, plain, 1, hour)
	refused("a configuration without encryption", connect.CodeFailedPrecondition, err)
	_, err = m.restore(t, plain, 2, hour)
	refused("an absent seq under a configuration without encryption", connect.CodeNotFound, err)
	_, err = m.restore(t, plain, 1, hour+1)
	refused("an absent hour under a configuration without encryption", connect.CodeNotFound, err)
	_, err = m.restore(t, plain, 3, hour)
	var ce *connect.Error
	if refused("differing copies under a configuration without encryption", connect.CodeFailedPrecondition, err); !errors.As(err, &ce) || len(ce.Details()) != 1 {
		t.Errorf("differing copies under a configuration without encryption = %v, want a RecordConflict", err)
	}
	_, err = m.restore(t, plain, 4, hour)
	refused("an unreadable copy under a configuration without encryption", connect.CodeUnavailable, err)
	_, err = m.cl.Restore(t.Context(), withToken(&tkv1.RestoreRequest{CaptureId: capture.String(), Seq: 1, Hour: hourTS(hour)}, readerToken))
	refused("a pack-reader", connect.CodePermissionDenied, err)

	m.srv.Keys = failingKeys{fmt.Errorf("%w: m-1", keys.ErrUnknownKey)}
	_, err = m.restore(t, capture, 1, hour)
	refused("a key refused", connect.CodeFailedPrecondition, err)
	m.srv.Keys = failingKeys{errors.New("provider down")}
	_, err = m.restore(t, capture, 1, hour)
	refused("a key unavailable", connect.CodeUnavailable, err)
	m.srv.Keys = m.keys

	m.srv.Catalog = failingAudit{c}
	got, err := m.restore(t, capture, 1, hour)
	refused("a failed audit", connect.CodeInternal, err)
	if got != nil {
		t.Errorf("a failed audit returned %v", got)
	}
	m.srv.Catalog = c

	// A second copy whose object is gone refuses the restore, the readable one notwithstanding.
	gone := m.put(t, capture, hour, ms[:1], seqEntries(entries, 1), m.policy())
	if err := m.st.Delete(t.Context(), gone.Key); err != nil {
		t.Fatal(err)
	}
	_, err = m.restore(t, capture, 1, hour)
	refused("a copy that cannot be read", connect.CodeUnavailable, err)
	// Second copies whose stored bytes are damaged:
	// one truncated, which the comparison finds incomplete, and one whose block body does not read.
	m.patch = func(b []byte) []byte { return slices.Clone(b[:len(b)-1]) }
	m.put(t, capture, hour, ms[1:2], seqEntries(entries, 2), m.policy())
	_, err = m.restore(t, capture, 2, hour)
	refused("a truncated copy", connect.CodeUnavailable, err)
	m.patch = func(b []byte) []byte {
		b = slices.Clone(b)
		r, err := tracepack.Open(t.Context(), bytes.NewReader(b), int64(len(b)), tracepack.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		b[r.Blocks()[0].Offset+40] ^= 1 // the first byte of the body, past the 40-byte envelope, its CRC left as it was

		return b
	}
	m.put(t, plain, hour, rs[:1], nil, nil)
	_, err = m.restore(t, plain, 1, hour)
	refused("a copy whose block does not read", connect.CodeUnavailable, err)

	if d := auditDetails(t, c); len(d) != 0 {
		t.Errorf("refusals wrote audit rows %v", d)
	}
	text := strings.Join(errs, "\n") + m.logs.String()
	for _, cr := range cs {
		for _, s := range cr.Secrets {
			if bytes.Contains([]byte(text), s) {
				t.Errorf("a refusal's error or log holds %q", s)
			}
		}
	}
}

// What holds no original is refused: an annotation, an unobserved record, an unmasked one, a pack without encryption.
func TestRestorable(t *testing.T) {
	t.Parallel()
	enc := "m-1"
	pol := &tracepack.RedactionPolicy{EncryptionKeyID: &enc}
	redacted := tracepack.Record{Kind: tracepack.KindData}
	redacted.Quality |= tracepack.QualityRedacted
	for name, v := range map[string]copyOf{
		"an annotation":            {record: tracepack.Record{Kind: tracepack.KindAnnotation}, policy: pol},
		"a control record":         {record: tracepack.Record{Kind: tracepack.KindControl}, policy: pol},
		"an unobserved record":     {record: redacted, unobserved: true, policy: pol},
		"an unmasked record":       {record: tracepack.Record{Kind: tracepack.KindData}, policy: pol},
		"a pack without a policy":  {record: redacted},
		"another encryption key":   {record: redacted, policy: &tracepack.RedactionPolicy{EncryptionKeyID: new("m-2")}},
		"a policy without its key": {record: redacted, policy: &tracepack.RedactionPolicy{}},
	} {
		if err := v.restorable(enc); !errors.Is(err, errNotRestorable) {
			t.Errorf("%s: restorable = %v, want refused", name, err)
		}
	}
	ok := copyOf{record: redacted, policy: pol}
	if err := ok.restorable(enc); err != nil {
		t.Errorf("a masked record under the key: %v", err)
	}
}

// seqEntries are the entries of seq.
func seqEntries(all []tracepack.RedactionEntry, seq uint64) []tracepack.RedactionEntry {
	var out []tracepack.RedactionEntry
	for _, e := range all {
		if e.Seq == seq {
			out = append(out, e)
		}
	}

	return out
}

// countEntries is the number of entries of seqs 1..n.
func countEntries(all []tracepack.RedactionEntry, n uint64) int {
	k := 0
	for _, e := range all {
		if e.Seq <= n {
			k++
		}
	}

	return k
}

// Copies whose records agree but whose redaction entries differ are differing versions,
// the entry that differs the last of a record's several.
func TestRestoreComparesEntries(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		ms, entries := m.mask(t, capture, recordsOf(carriers(t), hour))
		i := slices.IndexFunc(ms, func(r tracepack.Record) bool { return len(seqEntries(entries, r.Seq)) > 1 })
		if i < 0 {
			t.Fatal("no carrier masks into several entries")
		}
		seq, own := ms[i].Seq, seqEntries(entries, ms[i].Seq)
		m.put(t, capture, hour, ms[i:i+1], own, m.policy())
		other := slices.Clone(own)
		last := &other[len(other)-1]
		last.Digest = bytes.Clone(last.Digest)
		last.Digest[0] ^= 1
		m.put(t, capture, hour, ms[i:i+1], other, m.policy())
		if _, err := m.restore(t, capture, seq, hour); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "copies differ") {
			t.Errorf("Restore of copies with differing entries = %v, want a conflict", err)
		}
	})
}

// flipReservedFlag returns pack, written uncompressed, with a reserved record_flags bit (bit 5 of header byte 43) of seq's record flipped:
// the block's header section is column-major (byte j of record i at j*count + i), its body and envelope CRCs are recomputed,
// and so are the footer's index entry, which repeats the body CRC, the footer's CRC and the trailer's.
// The copy decodes as the original does, yet differs as stored.
func flipReservedFlag(t *testing.T, pack []byte, seq uint64) []byte {
	t.Helper()
	b := bytes.Clone(pack)
	r, err := tracepack.Open(t.Context(), bytes.NewReader(b), int64(len(b)), tracepack.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const envelope = 40
	blocks := r.Blocks()
	for _, bl := range blocks {
		off, n := int(bl.Offset), int(bl.RecordCount)
		body := b[off+envelope : off+int(bl.OnDiskLen)]
		for i := range n {
			var raw [8]byte
			for k := range raw {
				raw[k] = body[k*n+i]
			}
			if binary.LittleEndian.Uint64(raw[:]) != seq {
				continue
			}
			body[43*n+i] ^= 1 << 5
			binary.LittleEndian.PutUint32(b[off+20:], crc32.ChecksumIEEE(body))
			binary.LittleEndian.PutUint32(b[off+36:], crc32.ChecksumIEEE(b[off:off+36]))
			rewriteFooterCRC(b, bl.Offset, crc32.ChecksumIEEE(body))

			return b
		}
	}
	t.Fatalf("no record header of seq %d", seq)

	return nil
}

// rewriteFooterCRC sets body_crc of the uncompressed footer's index entry of the block at offset to crc,
// then the footer_crc and the trailer_crc that cover it (the tracepack format specification §10, §11).
func rewriteFooterCRC(b []byte, offset uint64, crc uint32) {
	tr := b[len(b)-64:]
	fo, fl := binary.LittleEndian.Uint64(tr[0:]), binary.LittleEndian.Uint64(tr[8:])
	footer := b[fo : fo+fl]
	entries, size := binary.LittleEndian.Uint64(footer[16:]), binary.LittleEndian.Uint64(footer[8:])
	for e := entries; e+80 <= uint64(len(footer)); e += size {
		if binary.LittleEndian.Uint64(footer[e:]) == offset {
			binary.LittleEndian.PutUint32(footer[e+20:], crc)
			break
		}
	}
	binary.LittleEndian.PutUint32(tr[28:], crc32.ChecksumIEEE(footer))
	binary.LittleEndian.PutUint32(tr[52:], crc32.ChecksumIEEE(tr[:52]))
}

// Copies whose decoded records agree but whose stored record_flags differ are differing versions.
func TestRestoreComparesStoredFlags(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		rs := recordsOf(carriers(t)[:1], hour)
		ms, entries := m.mask(t, capture, rs)
		m.put(t, capture, hour, ms, entries, m.policy())
		m.patch = func(b []byte) []byte { return flipReservedFlag(t, b, 1) }
		flipped := m.put(t, capture, hour, ms, entries, m.policy())
		obj, err := m.st.Open(t.Context(), flipped.Key)
		if err != nil {
			t.Fatal(err)
		}
		defer obj.Close()
		fr, err := tracepack.Open(t.Context(), obj, obj.Size(), tracepack.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		v, err := readCopy(t.Context(), fr, 1, catalog.Copy{Pack: flipped.ID})
		if err != nil || !bytes.Equal(v.record.Payload, ms[0].Payload) || fr.Header().FooterErr != nil {
			t.Fatalf("the flipped copy reads %v, %v, footer %v; want it whole, decoding as the original", v, err, fr.Header().FooterErr)
		}
		_, err = m.restore(t, capture, 1, hour)
		var ce *connect.Error
		if !errors.As(err, &ce) || ce.Code() != connect.CodeFailedPrecondition || len(ce.Details()) != 1 {
			t.Errorf("Restore of copies a stored flag apart = %v, want a RecordConflict", err)
		}
		if d := auditDetails(t, c); len(d) != 0 {
			t.Errorf("audit rows %v", d)
		}
	})
}

// Copies at the last representable instant are compared as any others: equal ones restore, and ones a stored flag apart conflict.
func TestRestoreAtTheLastInstant(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitActive(t)
		cs := carriers(t)[:2]
		rs := recordsOf(cs, hour)
		for i := range rs {
			rs[i].TSUTCNs = math.MaxInt64 - int64(len(rs)-1-i)
		}
		ms, entries := m.mask(t, capture, rs)
		for range 2 {
			m.put(t, capture, lastHour, ms, entries, m.policy())
		}
		got, err := m.restore(t, capture, 2, lastHour)
		if err != nil || !bytes.Equal(got.GetPayload(), rs[1].Payload) || got.GetTime().AsTime().UnixNano() != math.MaxInt64 {
			t.Fatalf("Restore of equal copies at the last instant = %v, %v", got, err)
		}
		m.patch = func(b []byte) []byte { return flipReservedFlag(t, b, 2) }
		m.put(t, capture, lastHour, ms, entries, m.policy())
		if _, err := m.restore(t, capture, 2, lastHour); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("Restore of copies a stored flag apart at the last instant = %v, want a conflict", err)
		}
	})
}
