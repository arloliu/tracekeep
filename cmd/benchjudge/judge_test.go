package main

import (
	"fmt"
	"strings"
	"testing"
)

// output writes go test -bench output: a package line, then one line a sample of each row, its metrics in the order given.
func output(rows map[string][]string) string {
	var b strings.Builder
	b.WriteString("goos: linux\npkg: example.test/p\n")
	for name, lines := range rows {
		for _, l := range lines {
			fmt.Fprintf(&b, "%s \t 1000\t%s\n", name, l)
		}
	}
	b.WriteString("PASS\nok  \texample.test/p\t1.0s\n")

	return b.String()
}

// around returns ten samples of ns/op near v, a spread of plus or minus 0.5%.
func around(v float64) []string {
	out := make([]string, 10)
	for i := range out {
		out[i] = fmt.Sprintf("%.2f ns/op", v*(1+float64(i-5)/1000))
	}

	return out
}

// judged parses three outputs and returns the judgement of row name's metric.
func judged(t *testing.T, base1, base2, head, name, metric string) judgement {
	t.Helper()
	sets := make([]rows, 3)
	for i, s := range []string{base1, base2, head} {
		r, err := parse(strings.NewReader(s))
		if err != nil {
			t.Fatal(err)
		}
		sets[i] = r
	}
	for _, j := range judge(sets[0], sets[1], sets[2]) {
		if j.row == "example.test/p "+name && j.metric == metric {
			return j
		}
	}
	t.Fatalf("no judgement of %s %s", name, metric)

	return judgement{}
}

func TestRepeatability(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		base2 float64
		r     float64
	}{
		"base2 below base1": {base2: 90, r: 0.10},
		"base2 at base1":    {base2: 100, r: 0},
		"base2 above base1": {base2: 110, r: 0.10},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const n = "BenchmarkX-8"
			j := judged(t, output(map[string][]string{n: around(100)}), output(map[string][]string{n: around(tc.base2)}),
				output(map[string][]string{n: around(100)}), n, "ns/op")
			if diff := j.r - tc.r; diff > 1e-9 || diff < -1e-9 {
				t.Errorf("r = %v, want %v", j.r, tc.r)
			}
			if diff := j.allowance - (0.05 + tc.r); diff > 1e-9 || diff < -1e-9 {
				t.Errorf("T = %v, want %v", j.allowance, 0.05+tc.r)
			}
			if j.verdict != pass {
				t.Errorf("an unchanged head = %v, want pass", j.verdict)
			}
		})
	}
}

func TestDecisions(t *testing.T) {
	t.Parallel()
	const n = "BenchmarkX-8"
	wide := []string{"95 ns/op", "100 ns/op", "105 ns/op", "110 ns/op", "115 ns/op", "98 ns/op", "112 ns/op", "103 ns/op", "108 ns/op", "117 ns/op"}
	for name, tc := range map[string]struct {
		base2, head []string
		metric      string
		want        verdict
	}{
		"same time":                         {around(100), around(100), "ns/op", pass},
		"10% slower":                        {around(100), around(110), "ns/op", regress},
		"10% slower, base repeatable to 8%": {around(108), around(110), "ns/op", pass},
		"faster":                            {around(100), around(80), "ns/op", pass},
		"interval across the allowance":     {around(100), wide, "ns/op", inconclusive},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			j := judged(t, output(map[string][]string{n: around(100)}), output(map[string][]string{n: tc.base2}), output(map[string][]string{n: tc.head}), n, tc.metric)
			if j.verdict != tc.want {
				t.Errorf("%s: %v, interval [%v, %v], allowance %v; want %v", name, j.verdict, j.low, j.high, j.allowance, tc.want)
			}
		})
	}
}

func TestExactMetrics(t *testing.T) {
	t.Parallel()
	const n = "BenchmarkX-8"
	lines := func(b, allocs int) []string {
		out := make([]string, 10)
		for i := range out {
			out[i] = fmt.Sprintf("%d ns/op\t%d B/op\t%d allocs/op", 100+i%2, b, allocs)
		}

		return out
	}
	for name, tc := range map[string]struct {
		base, head []string
		metric     string
		want       verdict
	}{
		"one more allocation":     {lines(64, 2), lines(64, 3), "allocs/op", regress},
		"as many allocations":     {lines(64, 2), lines(64, 2), "allocs/op", pass},
		"fewer allocations":       {lines(64, 2), lines(64, 1), "allocs/op", pass},
		"bytes from a zero base":  {lines(0, 0), lines(8, 0), "B/op", regress},
		"zero bytes on both":      {lines(0, 0), lines(0, 0), "B/op", pass},
		"2% more bytes, non-zero": {lines(1000, 2), lines(1020, 2), "B/op", pass},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			base := output(map[string][]string{n: tc.base})
			j := judged(t, base, base, output(map[string][]string{n: tc.head}), n, tc.metric)
			if j.verdict != tc.want {
				t.Errorf("%s: %v, want %v", name, j.verdict, tc.want)
			}
		})
	}
}

func TestRateIsJudgedAsItsInverse(t *testing.T) {
	t.Parallel()
	const n = "BenchmarkPublish-8"
	rate := func(v float64) []string {
		out := make([]string, 10)
		for i := range out {
			out[i] = fmt.Sprintf("1000 ns/op\t%.1f records/s", v*(1+float64(i-5)/1000))
		}

		return out
	}
	base := output(map[string][]string{n: rate(1000)})
	if j := judged(t, base, base, output(map[string][]string{n: rate(800)}), n, "records/s"); j.verdict != regress {
		t.Errorf("a rate 20%% lower = %v, want regress", j.verdict)
	}
	if j := judged(t, base, base, output(map[string][]string{n: rate(1200)}), n, "records/s"); j.verdict != pass {
		t.Errorf("a rate 20%% higher = %v, want pass", j.verdict)
	}
	if j := judged(t, base, output(map[string][]string{n: rate(1000 / 1.1)}), output(map[string][]string{n: rate(1000 / 1.12)}), n, "records/s"); j.verdict != pass {
		// Inverted, base2 is 10% slower than base1, so T is 15% and a head 12% slower passes.
		t.Errorf("a rate within its inverse's allowance = %v, r %v; want pass", j.verdict, j.r)
	}
}

func TestMissingRowsFail(t *testing.T) {
	t.Parallel()
	base := output(map[string][]string{"BenchmarkX-8": around(100), "BenchmarkY-8": around(100)})
	head := output(map[string][]string{"BenchmarkX-8": around(100), "BenchmarkZ-8": around(100)})
	if j := judged(t, base, base, head, "BenchmarkY-8", "ns/op"); j.verdict != fail {
		t.Errorf("a row missing in head = %v, want fail", j.verdict)
	}
	if j := judged(t, base, base, head, "BenchmarkZ-8", "ns/op"); j.verdict != fail {
		t.Errorf("a row missing in the base = %v, want fail", j.verdict)
	}
	if j := judged(t, base, base, head, "BenchmarkX-8", "ns/op"); j.verdict != pass {
		t.Errorf("the row on every side = %v, want pass", j.verdict)
	}
	other := output(map[string][]string{"BenchmarkX-4": around(100), "BenchmarkY-8": around(100)})
	if j := judged(t, base, base, other, "BenchmarkX-8", "ns/op"); j.verdict != fail {
		t.Errorf("a row run with other parameters = %v, want fail", j.verdict)
	}
	noBytes := output(map[string][]string{"BenchmarkX-8": around(100), "BenchmarkY-8": {"100 ns/op\t8 B/op"}})
	if j := judged(t, noBytes, noBytes, base, "BenchmarkY-8", "B/op"); j.verdict != fail {
		t.Errorf("a metric missing in head = %v, want fail", j.verdict)
	}
}

func TestExitCodes(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		verdicts []verdict
		want     int
	}{
		"all pass":                {[]verdict{pass, pass}, 0},
		"one inconclusive":        {[]verdict{pass, inconclusive}, exitInconclusive},
		"a regression":            {[]verdict{inconclusive, regress}, exitRegressed},
		"a failure":               {[]verdict{fail, pass}, exitRegressed},
		"nothing compared at all": {nil, exitRegressed},
	} {
		js := make([]judgement, len(tc.verdicts))
		for i, v := range tc.verdicts {
			js[i].verdict = v
		}
		if got := exitCode(js); got != tc.want {
			t.Errorf("%s: exit %d, want %d", name, got, tc.want)
		}
	}
}
