package sim

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/producer"
	"github.com/arloliu/tracekeep/producer/producertest"
)

// With a sampling rule, the simulator marks its forwarded S1F3 as interval polls:
// the capture declares S1F3 and S1F4 thinned, every third poll and its reply keep their bodies,
// and every other poll and its reply come back zero-filled past the header and unobserved.
// go-secs reports a frame it sends once the write returns, so a reply can be observed before its poll;
// the reply is then dequeued before its poll's binding and kept, a thin lost, never a wrong one.
func TestLoopbackThinsItsIntervalPolls(t *testing.T) {
	t.Parallel()
	const n, every = 30, 3
	bus := producertest.NewBus()
	opts := testOptions()
	opts.T3, opts.WithholdEvery = time.Second, 0
	opts.Thinning = &producer.SamplingRule{KeepEveryN: every, KeepInterval: time.Hour}
	l := newTestLoopbackWith(t, producer.Config{}, opts, producer.WithBus(bus))
	if err := l.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := l.Traffic(t.Context(), n); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var declared []tracepack.Thinning
	type key struct {
		sb       uint32
		function uint8
	}
	thinned := map[key]bool{}
	seqs := map[key]uint64{}
	data := 0
	for i := 1; data < 2*n; i++ {
		msgs, err := bus.WaitMessages(ctx, i)
		if err != nil {
			t.Fatalf("%d data records on the bus: %v", data, err)
		}
		br := decode(t, &msgs[i-1])
		if br.Descriptor != nil {
			m, err := tracepack.DecodeDescriptor(br.Descriptor)
			if err != nil {
				t.Fatal(err)
			}
			declared = m.Thinning
		}
		r := &br.Record
		if r.Kind != tracepack.KindData {
			continue
		}
		data++
		frame := r.Payload
		sb := binary.BigEndian.Uint32(frame[10:14])
		unobserved := r.Quality&tracepack.QualityUnobserved != 0
		text := frame[14:]
		zero := bytes.Count(text, []byte{0}) == len(text)
		if unobserved != zero || frame[6]&0x7f != 1 {
			t.Errorf("S%dF%d of System Bytes %d: unobserved %t, text zero %t", frame[6]&0x7f, frame[7], sb, unobserved, zero)
		}
		k := key{sb, frame[7]}
		if _, dup := seqs[k]; dup {
			t.Fatalf("S1F%d of System Bytes %d recorded twice", k.function, sb)
		}
		thinned[k], seqs[k] = unobserved, r.Seq
	}
	if len(declared) != 2 || declared[0].Function != 3 || declared[1].Function != 4 {
		t.Errorf("declared thinning %+v, want S1F3 and S1F4", declared)
	}
	for sb := uint32(1); sb <= n; sb++ {
		poll, reply := key{sb, 3}, key{sb, 4}
		if _, ok := seqs[poll]; !ok {
			t.Fatalf("no poll of System Bytes %d", sb)
		}
		if _, ok := seqs[reply]; !ok {
			t.Fatalf("no reply of System Bytes %d", sb)
		}
		kept := (sb-1)%every == 0 // the capture's first poll is kept, then every third
		lost := !kept && !thinned[reply] && seqs[reply] < seqs[poll]
		if thinned[poll] == kept || thinned[reply] != (!kept && !lost) {
			t.Errorf("System Bytes %d: poll thinned %t, reply thinned %t, want kept %t", sb, thinned[poll], thinned[reply], kept)
		}
	}
	l.Stop(t.Context())
}
