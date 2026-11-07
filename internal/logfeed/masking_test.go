package logfeed

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/go-secs/tracepack/classify"
	"github.com/arloliu/go-secs/v2/hsms"
	"github.com/arloliu/go-secs/v2/sml"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	"github.com/arloliu/tracekeep/internal/policy"
)

// Under the baseline, every carrier's secret is masked before its record is written: the data records an item rule names,
// and the text of an unparsed-entry, an encrypted body's included, and an unrecognised-line, which an annotation rule in domain source-text zeroes (logfeed.md §15.8).
func TestConverterMasks(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		h := newHarness(t, c)
		p := policy.Baseline()
		cfg := ingest.Config{
			Classifier: classify.NewNamed(classify.Name(), 0), Name: classify.Name(),
			Policy: &p, DigestKey: ingest.KeyRef{ID: "d-1"}, EncryptionKey: ingest.KeyRef{ID: "m-1"}, FormatMinor: 1,
		}
		var err error
		if cfg.Masking, err = p.Compile("d-1", "m-1"); err != nil {
			t.Fatal(err)
		}
		for id := range h.conv.Configs {
			h.conv.Configs[id] = cfg
		}
		declared := map[string]keys.Declaration{}
		for id, kind := range map[string]string{"d-1": catalog.KindDigest, "m-1": catalog.KindMaster} {
			path := filepath.Join(t.TempDir(), id)
			if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte(id[:1]), 32))), 0o600); err != nil {
				t.Fatal(err)
			}
			declared[id] = keys.Declaration{Provider: keys.ProviderFile, Ref: path, Kind: kind}
		}
		if h.conv.Keys, err = keys.New(keys.Config{Declared: declared}, c); err != nil {
			t.Fatal(err)
		}

		carriers, err := maskingtest.Carriers(&p)
		if err != nil {
			t.Fatal(err)
		}
		f := &fixture{t: t}
		var secrets [][]byte
		var encrypted []byte
		for _, cr := range carriers {
			secrets = append(secrets, cr.Secrets...)
			switch {
			case cr.Kind == tracepack.KindData:
				msg, err := hsms.DecodeHSMSMessage(cr.Payload)
				if err != nil {
					t.Fatal(err)
				}
				m, ok := msg.ToDataMessage()
				if !ok {
					t.Fatalf("%s is no data message", cr.Name)
				}
				hb := m.HeaderBytes()
				sf := "S" + itoa(m.Stream()) + "F" + itoa(m.Function())
				h.receive(&fakeMsg{data: f.raw(map[string]any{"kind": KindHeader, "dir": DirSent, "header": hex.EncodeToString(hb[:]), "sxfy": sf})},
					&fakeMsg{data: f.raw(map[string]any{"kind": KindBody, "sxfy": sf, "text": sml.MustEncodeMessage(m)})})
			case cr.Name == tracepack.AnnotationKindUnrecognisedLine.String():
				h.receive(&fakeMsg{data: f.raw(map[string]any{"kind": KindUnclassified, "text": string(cr.Secrets[0])})})
			case cr.Name == tracepack.AnnotationKindUnparsedEntry.String():
				h.receive(&fakeMsg{data: f.raw(map[string]any{"kind": KindBody, "sxfy": "S9F9", "text": string(cr.Secrets[0])})})
				encrypted = cr.Secrets[0]
			default: // an annotation kind a log capture never writes
			}
		}
		h.tick(2 * time.Second)
		h.tick(10 * time.Second)
		cp, ok, err := c.LogCaptureOf(t.Context(), catalog.LogKey{LogFile: catalog.LogFile{Source: "feed-a", EquipmentID: "T1", File: "host-20261107.log"}, Device: "eqp"})
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		redacted := 0
		recs := h.records(cp.ID)
		for _, r := range recs {
			for _, s := range secrets {
				if bytes.Contains(r.Payload, s) {
					t.Errorf("seq %d (%v) holds the secret %q", r.Seq, r.Kind, s)
				}
			}
			if r.Quality.HasAny(tracepack.QualityRedacted) {
				redacted++
			}
		}
		if redacted == 0 {
			t.Error("no record carries quality.redacted")
		}

		encryptedMasked(t, h, f, cp.ID, len(recs), encrypted)
	})
}

// encryptedMasked checks an encrypted body, an unparsed-entry holding its ciphertext as logged, is zeroed as the other annotations are (§15.9):
// its flush adds one redacted annotation to the capture's had records, without the ciphertext.
func encryptedMasked(t *testing.T, h *harness, f *fixture, capture tracepack.UUID, had int, ciphertext []byte) {
	t.Helper()
	h.receive(&fakeMsg{data: f.raw(map[string]any{"kind": KindEncrypted, "sxfy": "S9F9", "text": string(ciphertext)})})
	h.tick(2 * time.Second)
	h.tick(10 * time.Second)
	more := h.records(capture)
	if len(more) != had+1 || more[had].Kind != tracepack.KindAnnotation {
		t.Fatalf("%d records after the encrypted body, want one annotation more than %d", len(more), had)
	}
	if last := more[had]; bytes.Contains(last.Payload, ciphertext) || !last.Quality.HasAny(tracepack.QualityRedacted) {
		t.Errorf("the encrypted body's annotation holds its ciphertext or is not redacted: %+v", last)
	}
}

func itoa(v uint8) string { return strconv.Itoa(int(v)) }
