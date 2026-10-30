package api

import (
	"math"
	"testing"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// nsPerHour is one hour in nanoseconds, the unit of a catalog hour.
const nsPerHour = int64(time.Hour)

// Timestamps convert exactly inside int64 nanoseconds and to the nearer bound outside, and every hour's start converts, the first included.
func TestTimeConversionAtTheLimits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		s    int64
		n    int32
		want int64
	}{
		{9223372036, 0, 9223372036_000000000},
		{9223372036, 500000000, 9223372036_500000000},
		{9223372036, 854775807, math.MaxInt64},
		{9223372036, 854775808, math.MaxInt64},
		{9223372037, 0, math.MaxInt64},
		{-9223372037, 145224192, math.MinInt64},
		{-9223372037, 145224193, math.MinInt64 + 1},
		{-9223372038, 0, math.MinInt64},
	} {
		if got := nanos(&timestamppb.Timestamp{Seconds: tc.s, Nanos: tc.n}); got != tc.want {
			t.Errorf("nanos(%d, %d) = %d, want %d", tc.s, tc.n, got, tc.want)
		}
	}
	first, last := int64(math.MinInt64)/nsPerHour-1, int64(math.MaxInt64)/nsPerHour
	for _, h := range []int64{first, -1, 0, last} {
		if got := hourTS(h).AsTime(); !got.Equal(time.Unix(h*3600, 0)) || !hourTS(h).IsValid() {
			t.Errorf("hourTS(%d) = %s", h, got)
		}
	}
}

// A listed thinning entry carries its sampling rule when the catalog parsed one, and only its text otherwise.
func TestCaptureOfCarriesTheSamplingRule(t *testing.T) {
	t.Parallel()
	n, ms := uint64(60), uint64(60000)
	got := captureOf(&catalog.CaptureInfo{Thinning: []catalog.Thinning{
		{Thinning: tracepack.Thinning{Stream: 1, Function: 3, Source: "rule"}, KeepEveryN: &n, KeepIntervalMs: &ms},
		{Thinning: tracepack.Thinning{Stream: 6, Function: 11, Source: "fdc"}},
	}}).GetThinning()
	if len(got) != 2 || got[0].GetKeepEveryN() != 60 || got[0].GetKeepIntervalMs() != 60000 || got[0].GetSource() != "rule" ||
		got[1].KeepEveryN != nil || got[1].KeepIntervalMs != nil || got[1].GetStream() != 6 {
		t.Errorf("thinning %v", got)
	}
}
