package catalog

import (
	"strconv"
	"strings"

	"github.com/arloliu/go-secs/tracepack"
)

// Thinning is one thinning entry of a capture's start descriptor,
// with the sampling rule its source names when the source parses as producer.md §4.6's key=value convention (T-47).
type Thinning struct {
	tracepack.Thinning
	// KeepEveryN and KeepIntervalMs are the rule's keep_every_n and keep_interval_ms; both nil when the source does not parse.
	KeepEveryN, KeepIntervalMs *uint64
}

func thinningOf(entries []tracepack.Thinning) []Thinning {
	if entries == nil {
		return nil
	}
	out := make([]Thinning, len(entries))
	for i, e := range entries {
		out[i] = Thinning{Thinning: e}
		out[i].KeepEveryN, out[i].KeepIntervalMs = samplingRule(e.Source)
	}

	return out
}

// samplingRule parses the keep_every_n and keep_interval_ms of a source's ";"-separated fields;
// a source without both, with either twice or not a number, or with a count of zero, gives neither.
func samplingRule(source string) (keepEveryN, keepIntervalMs *uint64) {
	var n, ms *uint64
	for field := range strings.SplitSeq(source, ";") {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		var dst **uint64
		switch strings.TrimSpace(key) {
		case "keep_every_n":
			dst = &n
		case "keep_interval_ms":
			dst = &ms
		default:
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil || *dst != nil {
			return nil, nil
		}
		*dst = &v
	}
	if n == nil || ms == nil || *n == 0 {
		return nil, nil
	}

	return n, ms
}
