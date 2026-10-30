package ingest

import (
	"bytes"
	"log/slog"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/api"
	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	"github.com/arloliu/tracekeep/internal/compact"
	"github.com/arloliu/tracekeep/internal/keys"
	"github.com/arloliu/tracekeep/internal/maskingtest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
)

const (
	restorerToken = "a-static-token-of-a-restorer-0000001"
	scorerToken   = "a-static-token-of-a-scorer-000000001"
)

// served is the machine API over p's catalog and store, with p's keys, for a restorer and a scorer.
func served(t *testing.T, p *pipeline, k api.Keys) tracekeepv1connect.MachineServiceClient {
	t.Helper()
	s := &api.Server{
		Catalog: p.cat, Store: p.st, Keys: k, DigestLimit: &api.DigestLimit{Rate: 1e6, Burst: 1 << 20},
		Log: slog.New(slog.DiscardHandler),
		Auth: auth.NewAuthenticator(auth.AuthenticatorConfig{StaticEnabled: true, Static: []auth.StaticToken{
			{Token: restorerToken, Subject: "restorer", Roles: []string{auth.RoleRestorer}},
			{Token: scorerToken, Subject: "scorer", Roles: []string{auth.RoleScorer}},
		}}),
	}
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(hs.Close)

	return tracekeepv1connect.NewMachineServiceClient(hs.Client(), hs.URL)
}

func bearer[T any](msg *T, token string) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)

	return req
}

// activeMasking stages and activates the baseline's masking configuration in p's catalog, under d-1 and m-1,
// beside the pipeline's policy-less one, so the captures admitted after bind it; it returns the keys p masks with.
func activeMasking(t *testing.T, p *pipeline) *keys.Keys {
	t.Helper()
	var plain int64
	for id := range p.x.Configs {
		plain = id
	}
	mcfg := maskingConfig(t)
	id := stageMasking(t, p.cat, &mcfg)
	p.x.Configs[id], p.f.Configs[id] = mcfg, mcfg
	k := maskingKeys(t, p.cat)
	p.x.Keys = k
	if _, err := p.cat.Register(t.Context(), "r", []int64{plain, id}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := p.cat.ActivateStaged(t.Context(), id, nil, nil); err != nil {
		t.Fatal(err)
	}

	return k
}

// restoreAll restores every carrier's record and checks it is the record as received; an annotation is not restorable.
func restoreAll(t *testing.T, cl tracekeepv1connect.MachineServiceClient, capture tracepack.UUID, carriers []maskingtest.Carrier, from string) {
	t.Helper()
	for i := range carriers {
		r := carrierRecord(&carriers[i], uint64(i+1)) //nolint:gosec // small seqs
		resp, err := cl.Restore(t.Context(), bearer(&tkv1.RestoreRequest{
			CaptureId: capture.String(), Seq: r.Seq, Hour: timestamppb.New(time.Unix(HourOf(r.TSUTCNs)*3600, 0)),
		}, restorerToken))
		if carriers[i].Kind == tracepack.KindAnnotation {
			if connect.CodeOf(err) != connect.CodeFailedPrecondition {
				t.Errorf("%s: restore of an annotation from %s = %v, want not restorable", carriers[i].Name, from, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: restore from %s: %v", carriers[i].Name, from, err)
		}
		if !bytes.Equal(resp.Msg.GetPayload(), r.Payload) || resp.Msg.GetKind() != "data" {
			t.Errorf("%s: restored from %s %x, want the record as received %x", carriers[i].Name, from, resp.Msg.GetPayload(), r.Payload)
		}
	}
}

// inputOf is the Digest input of entry e of the record received as r: the encoded item, header included, for an item mask,
// the whole message text for a whole-text mask, the value bytes for an annotation's.
func inputOf(t *testing.T, r *tracepack.Record, e *tracepack.RedactionEntry) *tkv1.DigestInput {
	t.Helper()
	switch {
	case r.Kind == tracepack.KindAnnotation:
		var v []byte
		for _, mr := range e.MaskedRanges {
			v = append(v, r.Payload[mr.Offset:mr.Offset+mr.Length]...)
		}
		return &tkv1.DigestInput{Domain: e.Domain, Kind: "annotation", Original: v}
	case e.ItemPath == nil:
		return &tkv1.DigestInput{Domain: e.Domain, Kind: "text", Original: r.Payload[14:]}
	}
	off, n := e.MaskedRanges[0].Offset, e.MaskedRanges[0].Length
	for nlb := uint64(1); nlb <= 3 && nlb < off; nlb++ {
		h := r.Payload[off-1-nlb : off]
		length := uint64(0)
		for _, c := range h[1:] {
			length = length<<8 | uint64(c)
		}
		if uint64(h[0]&3) == nlb && length == n {
			return &tkv1.DigestInput{Domain: e.Domain, Kind: "item", Original: r.Payload[off-1-nlb : off+n]}
		}
	}
	t.Fatalf("seq %d: no item header before offset %d", r.Seq, off)

	return nil
}

// The gate's round trip (masking-keys.md §9): every carrier masked by ingest restores to the record as received,
// from its segments and then from the archive that replaced them; Digest equals the digest ingest wrote, for each input form.
func TestRestoreRoundTripsThroughIngest(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		p := newPipeline(t, c)
		k := activeMasking(t, p)
		carriers, _ := secretsOf(t)
		capture := tracepack.UUID{0x91}
		half := len(carriers) / 2
		feedCarriers(t, p, capture, carriers[:half])
		p.flushAll(t)
		feedFrom(t, p, capture, carriers[half:], uint64(half+1)) //nolint:gosec // a small count
		p.flushAll(t)
		if cp, err := c.Capture(t.Context(), capture); err != nil || p.x.Configs[cp.ConfigID].Policy == nil {
			t.Fatalf("the capture = %+v, %v; want it bound to the masking configuration", cp, err)
		}
		cl := served(t, p, k)
		restoreAll(t, cl, capture, carriers, "the segments")

		digestEveryEntry(t, cl, p, carriers)

		// Merged, then the segments deleted: the archive alone answers.
		lease, ok, err := c.TakeLease(t.Context(), "compactor/2", "test", time.Hour)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		at := time.Unix(0, now).Add(3 * time.Hour)
		r := &compact.Compactor{
			Catalog: c, Store: p.st, HorizonHour: func() int64 { return HourOf(now) - 24 }, Log: slog.New(slog.DiscardHandler),
			WorkDir: t.TempDir(), Writer: "tracekeep-test", QuietPeriod: 15 * time.Minute, DeleteDelay: time.Minute, Now: func() time.Time { return at },
		}
		if rep, err := r.Merges(t.Context(), lease); err != nil || len(rep.Failed) != 0 {
			t.Fatalf("Merges = %+v, %v", rep, err)
		}
		// The commit fixes delete_at by the catalog's own clock.
		at = time.Now().Add(time.Hour)
		if err := r.Deletions(t.Context(), lease, &compact.RetentionReport{}); err != nil {
			t.Fatal(err)
		}
		if n := len(storedKeys(t, p.st)); n != 1 {
			t.Fatalf("%d objects after the deletion, want the archive alone", n)
		}
		restoreAll(t, cl, capture, carriers, "the archive")

		refusedAsConflict(t, cl, p, capture, &carriers[0])

		// Every restore row holds the allowlist alone.
		rows, err := c.AuditByActor(t.Context(), "restorer")
		if err != nil || len(rows) == 0 {
			t.Fatalf("restore rows %v, %v", rows, err)
		}
		allow := regexp.MustCompile(`^seq \d+, hour \d+, pack [0-9a-f-]{36}, encryption key m-1$`)
		for _, row := range rows {
			if row.Action != "restore" || row.Capture == nil || *row.Capture != capture || !allow.MatchString(row.Detail) {
				t.Errorf("restore row %+v holds more than the allowlist", row)
			}
		}
	})
}

// digestEveryEntry digests the input of every entry ingest wrote and checks it equals the entry's digest, for each input form.
func digestEveryEntry(t *testing.T, cl tracekeepv1connect.MachineServiceClient, p *pipeline, carriers []maskingtest.Carrier) {
	t.Helper()
	forms := map[string]int{}
	for _, k := range storedKeys(t, p.st) {
		meta := header(t, p, k).Meta
		if meta == nil || meta.RedactionPolicy == nil {
			continue
		}
		for i := range meta.Redaction {
			e := &meta.Redaction[i]
			if e.Seq == 0 {
				continue
			}
			r := carrierRecord(&carriers[e.Seq-1], e.Seq)
			in := inputOf(t, &r, e)
			resp, err := cl.Digest(t.Context(), bearer(&tkv1.DigestRequest{DigestKeyId: meta.RedactionPolicy.KeyID, Inputs: []*tkv1.DigestInput{in}}, scorerToken))
			if err != nil {
				t.Fatalf("seq %d (%s): Digest: %v", e.Seq, in.GetKind(), err)
			}
			if got := resp.Msg.GetDigests()[0]; !bytes.Equal(got, e.Digest) {
				t.Errorf("seq %d (%s): Digest %x, want the entry's %x", e.Seq, in.GetKind(), got, e.Digest)
			}
			forms[in.GetKind()]++
		}
	}
	for _, f := range []string{"item", "text", "annotation"} {
		if forms[f] == 0 {
			t.Errorf("no %s input digested: %v", f, forms)
		}
	}
}

// refusedAsConflict delivers a differing version of seq 1, c's record, in the next hour and checks the restore is refused naming either hour.
func refusedAsConflict(t *testing.T, cl tracekeepv1connect.MachineServiceClient, p *pipeline, capture tracepack.UUID, c *maskingtest.Carrier) {
	t.Helper()
	later := carrierRecord(c, 1)
	later.TSUTCNs += int64(time.Hour)
	p.feed(t, inline(t, capture, later, nil))
	p.flushAll(t)
	for _, ts := range []int64{now, later.TSUTCNs} {
		_, err := cl.Restore(t.Context(), bearer(&tkv1.RestoreRequest{
			CaptureId: capture.String(), Seq: 1, Hour: timestamppb.New(time.Unix(HourOf(ts)*3600, 0)),
		}, restorerToken))
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "copies differ") {
			t.Errorf("restore of differing versions naming hour %d = %v, want a conflict", HourOf(ts), err)
		}
	}
}
