package api

import (
	"slices"
	"testing"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/catalog/catalogtest"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// message is a data record of seq at ns: S1Ff, W when w, system bytes {0, 0, 0, sys}, sent from the host when fromHost, with body.
func message(seq uint64, ns int64, f byte, w, fromHost bool, sys byte, body ...byte) tracepack.Record {
	b6 := byte(1)
	if w {
		b6 |= 0x80
	}
	frame := append([]byte{0, 0, 0, byte(10 + len(body)), 0, 1, b6, f, 0, 0, 0, 0, 0, sys}, body...)
	r := tracepack.Record{Seq: seq, TSUTCNs: ns, Kind: tracepack.KindData, Dir: tracepack.DirEquipmentToHost, Fidelity: tracepack.FidelityWireExact, Payload: frame}
	if fromHost {
		r.Dir = tracepack.DirHostToEquipment
	}
	r.SetCapturedFieldValidity()

	return r
}

// find looks up the transaction of seq in the test's hour.
func find(m *masked, t *testing.T, capture tracepack.UUID, seq uint64, token string) (*tkv1.FindTransactionResponse, error) {
	t.Helper()
	resp, err := m.cl.FindTransaction(t.Context(), withToken(&tkv1.FindTransactionRequest{CaptureId: capture.String(), Seq: seq, Hour: hourTS(hour)}, token))
	if err != nil {
		return nil, err
	}

	return resp.Msg, nil
}

// valid tells whether the record of seq is a valid match.
func valid(r *tkv1.FindTransactionResponse, seq uint64) bool {
	for _, tr := range r.GetRecords() {
		if tr.GetRecord().GetSeq() == seq {
			return tr.GetValid()
		}
	}

	return false
}

func roles(r *tkv1.FindTransactionResponse) map[uint64][]string {
	out := map[uint64][]string{}
	for _, tr := range r.GetRecords() {
		out[tr.GetRecord().GetSeq()] = tr.GetRoles()
	}

	return out
}

// A primary and its reply match within an hour and across an hour boundary;
// a conflict in the second hour is reported with its versions' packs.
func TestFindTransaction(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		m.srv.Dev = true
		capture := m.admitUnmasked(t)
		base := hour * nsPerHour
		m.put(t, capture, hour, []tracepack.Record{message(1, base+1, 1, true, true, 7), message(2, base+2, 2, false, false, 7)}, nil, nil)
		got, err := find(m, t, capture, 1, viewerToken)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetOutcome() != "matched" || !slices.Contains(roles(got)[1], "primary") || !slices.Contains(roles(got)[2], "candidate") || !valid(got, 2) {
			t.Errorf("a primary and its reply = %v, roles %v", got.GetOutcome(), roles(got))
		}
		for _, tr := range got.GetRecords() {
			if tr.GetRecord().GetSml() == "" && tr.GetRecord().GetSmlError() == "" {
				t.Errorf("seq %d carries neither SML nor its error", tr.GetRecord().GetSeq())
			}
		}
		// Across the boundary: a primary at the hour's last nanosecond, its reply in the next hour.
		end := (hour+1)*nsPerHour - 1
		m.put(t, capture, hour, []tracepack.Record{message(3, end, 3, true, true, 8)}, nil, nil)
		m.put(t, capture, hour+1, []tracepack.Record{message(4, end+5, 4, false, false, 8)}, nil, nil)
		got, err = find(m, t, capture, 3, viewerToken)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetOutcome() != "matched" || !slices.Contains(roles(got)[4], "candidate") || !valid(got, 4) || len(got.GetHours()) != 2 {
			t.Errorf("across the boundary = %v, roles %v, hours %v; want matched over two hours", got.GetOutcome(), roles(got), got.GetHours())
		}
		// A second version of the reply in the next hour: a conflict there.
		m.put(t, capture, hour+1, []tracepack.Record{message(4, end+5, 4, false, false, 8, 0xA5)}, nil, nil)
		got, err = find(m, t, capture, 3, viewerToken)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetOutcome() == "matched" || len(got.GetConflicts()) != 1 || got.GetConflicts()[0].GetSeq() != 4 || len(got.GetConflicts()[0].GetVersions()) != 2 {
			t.Errorf("a conflict in the second hour = %v, conflicts %v; want it reported, not matched", got.GetOutcome(), got.GetConflicts())
		}
		conflictHeld(t, got, 4)
		// A backward clock step: the reply's time precedes its primary's, the seqs still in order.
		m.put(t, capture, hour, []tracepack.Record{message(5, base+500, 5, true, true, 9), message(6, base+400, 6, false, false, 9)}, nil, nil)
		got, err = find(m, t, capture, 5, viewerToken)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetOutcome() != "matched" || !valid(got, 6) {
			t.Errorf("a reply after a backward clock step = %v, roles %v; want matched", got.GetOutcome(), roles(got))
		}
	})
}

// The lookup's refusals: a reply's seq, a withheld capture, an hour of too many packs, an hour that is not an hour's start, another role.
func TestFindTransactionRefusals(t *testing.T) {
	t.Parallel()
	catalogtest.Engines(t, func(t *testing.T, c *catalog.Catalog) {
		m := newMasked(t, c)
		capture := m.admitUnmasked(t)
		base := hour * nsPerHour
		m.put(t, capture, hour, []tracepack.Record{message(1, base+1, 1, true, true, 7)}, nil, nil)
		m.put(t, capture, hour, []tracepack.Record{message(2, base+2, 2, false, false, 7)}, nil, nil)
		if _, err := find(m, t, capture, 1, viewerToken); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("a withheld capture = %v, want failed_precondition", err)
		}
		m.srv.Dev = true
		if _, err := find(m, t, capture, 2, viewerToken); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("a reply's seq = %v, want failed_precondition", err)
		}
		if _, err := find(m, t, capture, 1, readerToken); connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("a pack reader = %v, want permission_denied", err)
		}
		req := &tkv1.FindTransactionRequest{CaptureId: capture.String(), Seq: 1, Hour: timestamppb.New(hourTS(hour).AsTime().Add(1))}
		if _, err := m.cl.FindTransaction(t.Context(), withToken(req, viewerToken)); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("an hour that is not an hour's start = %v, want invalid_argument", err)
		}
		m.srv.Records.MaxLookupPacks = 1
		if _, err := find(m, t, capture, 1, viewerToken); connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Errorf("an hour of two packs, at most one = %v, want resource_exhausted", err)
		}
	})
}

// conflictHeld checks that every version of seq the lookup read is flagged a conflict, with both its packs and their states.
func conflictHeld(t *testing.T, got *tkv1.FindTransactionResponse, seq uint64) {
	t.Helper()
	versions := 0
	for _, tr := range got.GetRecords() {
		r := tr.GetRecord()
		if r.GetSeq() != seq {
			continue
		}
		versions++
		if !r.GetConflict() || len(r.GetHolders()) != 2 || r.GetHolders()[0].GetPackId() == r.GetHolders()[1].GetPackId() {
			t.Errorf("a conflict's version = %v, want it flagged with its two packs", r)
		}
		for _, h := range r.GetHolders() {
			if h.GetState() != catalog.PackStaged {
				t.Errorf("holder %s is %q, want staged", h.GetPackId(), h.GetState())
			}
		}
	}
	if versions != 2 {
		t.Errorf("%d versions of seq %d read, want 2", versions, seq)
	}
}
