package benchscale

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// root is the repository's root, from this package's directory.
const root = "../.."

// benchWorkflow is the part of bench.yaml TestBenchGroups reads: each job's matrix of groups.
type benchWorkflow struct {
	Jobs map[string]benchJob
}

type benchJob struct {
	Strategy benchStrategy
}

type benchStrategy struct {
	Matrix benchMatrix
}

type benchMatrix struct {
	Include []benchGroup
}

// benchGroup is one group: its name, the packages scripts/catalog.sh bench runs and its -bench expression.
type benchGroup struct {
	Group, Packages, Bench string
}

// TestBenchGroups checks bench.yaml's groups against the benchmarks that reach MariaDB, those of a package whose tests run an engine loop
// (benchEngines or catalogtest.BenchEngines): each such benchmark is in exactly one group, by its package and -bench expression,
// so the groups' jobs together run every MariaDB row once; and every group names such a package and holds a benchmark.
func TestBenchGroups(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(root, ".github/workflows/bench.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var wf benchWorkflow
	if err := yaml.Unmarshal(b, &wf); err != nil {
		t.Fatal(err)
	}
	groups := wf.Jobs["mariadb"].Strategy.Matrix.Include
	if len(groups) == 0 {
		t.Fatal("bench.yaml's mariadb job has no groups")
	}
	benchmarks := mariadbBenchmarks(t)
	held := map[string][]string{}
	for _, g := range groups {
		re, err := regexp.Compile(g.Bench)
		if err != nil {
			t.Fatalf("group %s: %v", g.Group, err)
		}
		n := 0
		for _, pkg := range strings.Fields(g.Packages) {
			names, ok := benchmarks[filepath.Clean(pkg)]
			if !ok {
				t.Errorf("group %s names %s, whose benchmarks reach no MariaDB", g.Group, pkg)
			}
			for _, name := range names {
				if re.MatchString(name) {
					key := filepath.Clean(pkg) + "." + name
					held[key] = append(held[key], g.Group)
					n++
				}
			}
		}
		if n == 0 {
			t.Errorf("group %s holds no benchmark", g.Group)
		}
	}
	for pkg, names := range benchmarks {
		for _, name := range names {
			if key := pkg + "." + name; len(held[key]) != 1 {
				t.Errorf("%s is in groups %v, want exactly one", key, held[key])
			}
		}
	}
}

// mariadbBenchmarks returns, by package directory from the root, the Benchmark functions of every package whose tests run an engine loop.
func mariadbBenchmarks(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	loops := map[string]bool{}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		pkg, _ := filepath.Rel(root, filepath.Dir(path))
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Benchmark") {
				out[pkg] = append(out[pkg], fn.Name.Name)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					loops[pkg] = loops[pkg] || fun.Name == "benchEngines"
				case *ast.SelectorExpr:
					loops[pkg] = loops[pkg] || fun.Sel.Name == "BenchEngines"
				default:
				}
			}

			return true
		})

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for pkg := range out {
		if !loops[pkg] {
			delete(out, pkg)
		}
	}
	for _, want := range []string{"internal/catalog", "internal/metrics"} {
		if len(out[want]) == 0 {
			t.Fatalf("found no MariaDB benchmark in %s, only in %v", want, slices.Sorted(maps.Keys(out)))
		}
	}

	return out
}
