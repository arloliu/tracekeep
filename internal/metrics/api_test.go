package metrics

import (
	"strings"
	"testing"
)

func TestAuthFailures(t *testing.T) {
	t.Parallel()
	s := NewServer()
	m, err := NewAPI(s)
	if err != nil {
		t.Fatal(err)
	}
	m.AuthFailure("expired")
	m.AuthFailure("expired")
	m.AuthFailure("forbidden")
	(*API)(nil).AuthFailure("missing")
	b := body(t, s)
	for _, want := range []string{`tracekeep_api_auth_failures_total{reason="expired"} 2`, `tracekeep_api_auth_failures_total{reason="forbidden"} 1`} {
		if !strings.Contains(b, want) {
			t.Errorf("no %s in\n%s", want, b)
		}
	}
}
