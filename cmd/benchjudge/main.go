// Command benchjudge decides whether a change regressed a benchmark (650-perf-sec.md, T-54).
//
// It reads three files of go test -bench output, each from -count 10 or so: two runs of the base and one of the head.
// For each row and each metric it takes the base's repeatability r, abs(median(base2) / median(base1) - 1),
// and the allowance T, 5% plus r, and a 95% percentile bootstrap interval of median(head) / median(base1):
// the metric passes when the interval's upper bound is at most 1 + T, regresses when its lower bound exceeds 1 + T,
// and is inconclusive otherwise, to be measured again, never passed.
// Allocations are exact when every sample of both base runs has the same count, and any metric whose base samples are all zero is exact:
// any increase of the median regresses. Allocations whose base samples vary are judged by the interval, as a time is,
// and a varying base whose median is zero, which has no ratio, is inconclusive.
// A rate, a unit ending in /s, is judged as its inverse, a time.
// A row or a metric missing on any side fails; a row run with other -cpu is another row, so it fails too.
// A row fails when its run's parameters differ between the sides, or within one, or its samples number differently:
// the parameters are the configuration lines in force when it appeared, key: value as go test writes goos: and pkg:,
// such as the benchtime:, count:, cpus: and scale: lines scripts/catalog.sh bench writes before each of its passes.
// A row a -fixed pattern matches runs at a fixed -benchtime Nx: every sample on every side must show the same iterations, or it fails.
// Each -require pattern must match a row on every side, or the run fails, so a benchmark that skipped everywhere is not passed.
//
// Usage:
//
//	benchjudge [-fixed regexp]... [-require regexp]... base1.txt base2.txt head.txt
//
// It prints a table and exits 0 when every metric passes, 3 when some are inconclusive and none regressed,
// and 1 when one regressed or failed, or nothing was compared.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
)

// The bootstrap: resamples and the interval's coverage, with a fixed seed so a decision repeats.
const (
	resamples   = 10000
	coverage    = 0.95
	baseAllowed = 0.05
)

// Exit codes besides 0, all passed; 2 is a usage or read error.
const (
	exitRegressed    = 1
	exitInconclusive = 3
)

type verdict int

const (
	pass verdict = iota
	inconclusive
	regress
	fail
)

func (v verdict) String() string { return [...]string{"pass", "inconclusive", "REGRESS", "FAIL"}[v] }

// rows maps a row, its package and its name with the -cpu suffix, to its samples.
type rows map[string]*row

// row is a row's samples: the iterations of each, and each metric's values,
// and the configuration its first sample appeared under, changed true when a later one appeared under another.
type row struct {
	iters   []int64
	metrics map[string][]float64
	config  string
	changed bool
}

// configLine is a configuration line of go test -bench output: a key starting with a lower-case letter, a colon and its value.
var configLine = regexp.MustCompile(`^([a-z][^\s:]*):\s*(.*)$`)

// patterns is a repeatable flag of regular expressions, matched against a row's package and name.
type patterns []*regexp.Regexp

func (p *patterns) String() string { return fmt.Sprint(*p) }

func (p *patterns) Set(v string) error {
	re, err := regexp.Compile(v)
	if err != nil {
		return err
	}
	*p = append(*p, re)

	return nil
}

func (p patterns) match(name string) bool {
	return slices.ContainsFunc(p, func(re *regexp.Regexp) bool { return re.MatchString(name) })
}

// options are the rows run at a fixed N and the rows required on every side.
type options struct {
	fixed, require patterns
}

// judgement is one metric of one row: the sides' medians, the repeatability and allowance, the interval and the verdict.
type judgement struct {
	row, metric        string
	base1, base2, head float64
	r, allowance       float64
	low, high          float64
	verdict            verdict
	why                string
}

func main() {
	var o options
	flag.Var(&o.fixed, "fixed", "a `regexp` of rows run at a fixed -benchtime Nx, whose iterations must match; repeatable")
	flag.Var(&o.require, "require", "a `regexp` that must match a row on every side; repeatable")
	flag.Parse()
	if flag.NArg() != 3 {
		fmt.Fprintln(os.Stderr, "usage: benchjudge [-fixed regexp]... [-require regexp]... base1.txt base2.txt head.txt")
		os.Exit(2)
	}
	var sets [3]rows
	for i, path := range flag.Args() {
		f, err := os.Open(path) //nolint:gosec // the operator names the files to read
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		sets[i], err = parse(f)
		_ = f.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
			os.Exit(2)
		}
	}
	js := judge(sets, o)
	report(os.Stdout, js)
	os.Exit(exitCode(js))
}

// parse reads go test -bench output: a pkg: line names the package of the rows after it,
// and a row's line is its name, its iterations and pairs of a value and its unit.
func parse(r io.Reader) (rows, error) {
	out := rows{}
	pkg := ""
	cfg := map[string]string{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		if m := configLine.FindStringSubmatch(line); m != nil {
			if m[1] == "pkg" {
				pkg = strings.TrimSpace(m[2])
			} else {
				cfg[m[1]] = strings.TrimSpace(m[2])
			}

			continue
		}
		f := strings.Fields(line)
		if len(f) < 4 || !strings.HasPrefix(f[0], "Benchmark") || len(f)%2 != 0 {
			continue
		}
		iters, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			continue
		}
		name := pkg + " " + f[0]
		config := render(cfg)
		if out[name] == nil {
			out[name] = &row{metrics: map[string][]float64{}, config: config}
		}
		r := out[name]
		r.changed = r.changed || r.config != config
		r.iters = append(r.iters, iters)
		for i := 2; i < len(f); i += 2 {
			v, err := strconv.ParseFloat(f[i], 64)
			if err != nil {
				return nil, fmt.Errorf("%s: %q is not a value", name, f[i])
			}
			if strings.HasSuffix(f[i+1], "/s") {
				v = 1 / v // a rate is judged as the time it takes
			}
			r.metrics[f[i+1]] = append(r.metrics[f[i+1]], v)
		}
	}

	return out, sc.Err()
}

// render is a configuration as one string, its keys in order.
func render(cfg map[string]string) string {
	keys := slices.Sorted(maps.Keys(cfg))
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + ": " + cfg[k]
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

// sideNames name the three sides, in the order judge takes them.
var sideNames = [3]string{"base1", "base2", "head"}

// judge judges every metric of every row any side holds, in the order of their names,
// a fixed-N row whose iterations differ failing whole; then it fails each required pattern no row of a side matches.
func judge(sides [3]rows, o options) []judgement {
	names := map[string]bool{}
	for _, set := range sides {
		for name, r := range set {
			for metric := range r.metrics {
				names[name+"\x00"+metric] = true
			}
		}
	}
	keys := make([]string, 0, len(names))
	for k := range names {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := make([]judgement, 0, len(keys))
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a reproducible bootstrap, not a secret
	for _, k := range keys {
		name, metric, _ := strings.Cut(k, "\x00")
		why := runsDiffer(sides, name)
		if why == "" && o.fixed.match(name) {
			why = iterationsDiffer(sides, name)
		}
		if why != "" {
			out = append(out, judgement{row: name, metric: metric, verdict: fail, why: why})

			continue
		}
		var s [3][]float64
		for i, set := range sides {
			if r := set[name]; r != nil {
				s[i] = r.metrics[metric]
			}
		}
		out = append(out, judgeMetric(rng, name, metric, s[0], s[1], s[2]))
	}
	for _, re := range o.require {
		for i, set := range sides {
			if !slices.ContainsFunc(slices.Collect(maps.Keys(set)), re.MatchString) {
				out = append(out, judgement{row: "-require " + re.String(), metric: "-", verdict: fail, why: "no row in " + sideNames[i]})
			}
		}
	}

	return out
}

// runsDiffer explains why a row's runs are not the same run on every side it appears on, or returns "":
// its parameters changing within a side, or differing between sides, or its samples numbering differently.
func runsDiffer(sides [3]rows, name string) string {
	var first *row
	firstSide := ""
	for i, set := range sides {
		r := set[name]
		switch {
		case r == nil:
			continue
		case r.changed:
			return "parameters change within " + sideNames[i]
		case first == nil:
			first, firstSide = r, sideNames[i]
		case r.config != first.config:
			return fmt.Sprintf("parameters differ: %s in %s, %s in %s", first.config, firstSide, r.config, sideNames[i])
		case len(r.iters) != len(first.iters):
			return fmt.Sprintf("samples differ: %d in %s, %d in %s", len(first.iters), firstSide, len(r.iters), sideNames[i])
		default: // the same run as the first side's
		}
	}

	return ""
}

// iterationsDiffer explains why a fixed-N row's samples ran different iterations, on one side or between sides, or returns "".
func iterationsDiffer(sides [3]rows, name string) string {
	var first int64
	for i, set := range sides {
		r := set[name]
		if r == nil {
			continue
		}
		for _, n := range r.iters {
			if first == 0 {
				first = n
			}
			if n != first {
				return fmt.Sprintf("fixed N differs: %d, and %d in %s", first, n, sideNames[i])
			}
		}
	}

	return ""
}

func judgeMetric(rng *rand.Rand, row, metric string, b1, b2, h []float64) judgement {
	j := judgement{row: row, metric: metric}
	for _, side := range []struct {
		name string
		s    []float64
	}{{"base1", b1}, {"base2", b2}, {"head", h}} {
		if len(side.s) == 0 {
			j.verdict, j.why = fail, "missing in "+side.name

			return j
		}
	}
	// Allocations whose base never varied, and anything from an all-zero base, are exact; varying allocations are judged as a time is.
	base := append(slices.Clone(b1), b2...)
	allZero := !slices.ContainsFunc(base, func(v float64) bool { return v != 0 })
	deterministic := metric == "allocs/op" && !slices.ContainsFunc(base, func(v float64) bool { return v != b1[0] })
	j.base1, j.base2, j.head = median(b1), median(b2), median(h)
	if j.base1 == 0 && !deterministic && !allZero {
		// A varying base whose median is zero has no ratio to judge: measure it again, never pass it.
		j.verdict, j.why = inconclusive, "a varying base with a zero median"

		return j
	}
	if deterministic || allZero {
		j.verdict, j.why = pass, "exact"
		if j.head > j.base1 {
			j.verdict = regress
		}

		return j
	}
	j.r = math.Abs(j.base2/j.base1 - 1)
	j.allowance = baseAllowed + j.r
	j.low, j.high = interval(rng, b1, h)
	switch {
	case j.high <= 1+j.allowance:
		j.verdict = pass
	case j.low > 1+j.allowance:
		j.verdict = regress
	default:
		j.verdict, j.why = inconclusive, "measure again"
	}

	return j
}

// interval is the percentile bootstrap interval of median(head) / median(base) at coverage.
func interval(rng *rand.Rand, base, head []float64) (low, high float64) {
	ratios := make([]float64, resamples)
	bs, hs := make([]float64, len(base)), make([]float64, len(head))
	for i := range ratios {
		for k := range bs {
			bs[k] = base[rng.IntN(len(base))]
		}
		for k := range hs {
			hs[k] = head[rng.IntN(len(head))]
		}
		ratios[i] = ratio(median(hs), median(bs))
	}
	slices.Sort(ratios)
	tail := (1 - coverage) / 2

	return ratios[int(tail*resamples)], ratios[int(math.Ceil((1-tail)*resamples))-1]
}

// ratio is head / base, with a zero base read as no change when head is zero too and as unbounded otherwise.
func ratio(head, base float64) float64 {
	switch {
	case base != 0:
		return head / base
	case head == 0:
		return 1
	default:
		return math.Inf(1)
	}
}

// median is s's median; s is reordered.
func median(s []float64) float64 {
	slices.Sort(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}

	return (s[n/2-1] + s[n/2]) / 2
}

func exitCode(js []judgement) int {
	if len(js) == 0 {
		return exitRegressed
	}
	worst := pass
	for _, j := range js {
		worst = max(worst, j.verdict)
	}
	switch worst {
	case pass:
		return 0
	case inconclusive:
		return exitInconclusive
	case regress, fail:
		return exitRegressed
	}

	return exitRegressed
}

// report prints the judgements as a table, a rate's medians as its inverse's.
func report(w io.Writer, js []judgement) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "row\tmetric\tbase1\tbase2\thead\tr\tT\tinterval\tverdict")
	for _, j := range js {
		metric := j.metric
		if strings.HasSuffix(metric, "/s") {
			metric = "1/(" + metric + ")"
		}
		fmt.Fprintf(tw, "%s\t%s\t%.4g\t%.4g\t%.4g\t%.1f%%\t%.1f%%\t[%.3f, %.3f]\t%s %s\n",
			j.row, metric, j.base1, j.base2, j.head, 100*j.r, 100*j.allowance, j.low, j.high, j.verdict, j.why)
	}
	_ = tw.Flush()
}
