package store_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/arloliu/tracekeep/internal/benchscale"
)

// BenchmarkFSPut puts an object of a fixed size under a key 1, 4 or 8 levels deep in an fs store (P11).
// Its starting state is a store on a fresh directory whose key's directories an untimed first put created, so a timed put only syncs them.
// An iteration puts a fresh key, its time including the sync of the file and of every directory from the key's up to the root
// (point 22 records the temporary directory's filesystem); it asserts the put succeeded,
// and the object is deleted outside the timer, so every put meets a directory of the same size.
// Its sizes are objects of 1 KiB, 1 MiB and 16 MiB (TRACEKEEP_BENCH_SCALE); every depth runs at every size.
func BenchmarkFSPut(b *testing.B) {
	for _, size := range benchscale.Sizes(b, 1<<10, 1<<20, 16<<20) {
		body := bytes.Repeat([]byte{7}, size)
		for _, depth := range []int{1, 4, 8} {
			b.Run(fmt.Sprintf("bytes=%d/depth=%d", size, depth), func(b *testing.B) {
				s := newFS(b, b.TempDir())
				dir := strings.Repeat("d/", depth-1)
				// A first put creates the key's directories, so no timed put does.
				if err := s.Put(b.Context(), dir+"first", bytes.NewReader(body), int64(size)); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					i++
					key := fmt.Sprintf("%sobject-%d", dir, i)
					if err := s.Put(b.Context(), key, bytes.NewReader(body), int64(size)); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					if err := s.Delete(b.Context(), key); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			})
		}
	}
}
