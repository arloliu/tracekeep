package benchscale

import (
	"fmt"
	"slices"
	"testing"
)

// fatalTB records a Fatalf instead of stopping the test, so a refusal can be asserted.
type fatalTB struct {
	testing.TB
	failed string
}

func (f *fatalTB) Helper() {}

func (f *fatalTB) Fatalf(format string, args ...any) { f.failed = fmt.Sprintf(format, args...) }

func TestEngines(t *testing.T) {
	all := []string{"sqlite", "mariadb"}
	for _, tc := range []struct {
		env  string
		want []string
	}{
		{"", all},
		{" ", all},
		{"mariadb", []string{"mariadb"}},
		{"sqlite", []string{"sqlite"}},
		{"mariadb,sqlite", all},
		{" mariadb , sqlite ", all},
	} {
		t.Run(fmt.Sprintf("%q", tc.env), func(t *testing.T) {
			t.Setenv(EnginesEnv, tc.env)
			if got := Engines(t, all...); !slices.Equal(got, tc.want) {
				t.Fatalf("Engines = %v, want %v", got, tc.want)
			}
		})
	}
	for _, env := range []string{"postgres", "mariadb,", "MariaDB"} {
		t.Run(fmt.Sprintf("refuses %q", env), func(t *testing.T) {
			t.Setenv(EnginesEnv, env)
			f := &fatalTB{TB: t}
			Engines(f, all...)
			if f.failed == "" {
				t.Fatalf("Engines accepted %s=%q", EnginesEnv, env)
			}
		})
	}
}
