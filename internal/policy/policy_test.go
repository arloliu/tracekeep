package policy

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// semBaseline is the tracepack semantics specification §8's baseline table, typed here from the table itself
// and not from baseline.yaml, so the two transcriptions check each other.
var semBaseline = struct {
	items []ItemRule
	notes []AnnotationRule
}{
	items: []ItemRule{
		{7, 3, "2", "ppbody"}, {7, 6, "2", "ppbody"},
		{7, 36, "3", "ppbody"},
		{7, 23, "4", "formatted-pp"}, {7, 26, "4", "formatted-pp"}, {7, 31, "4", "formatted-pp"},
		{13, 6, "4/*", "dataset"},
		{15, 13, "4/*/2", "rcpattr"}, {15, 13, "5", "rcpbody"}, {15, 27, "4/*/2", "rcpattr"}, {15, 27, "5", "rcpbody"},
		{15, 32, "2/*/2", "rcpattr"}, {15, 32, "3", "rcpbody"},
		{15, 15, "4", "rcp-sections"},
		{15, 18, "1", "rcp-sections"},
		{15, 28, "2/*/2", "rcpattr"},
		{15, 30, "4/*/2", "rcpattr"},
		{6, 25, "6/*/2", "rcpattr"},
		{15, 25, "4/*/2", "rcpparval"},
		{15, 33, "3/*/2/*/2", "rcpparval"},
		{16, 3, "4/3/*/2", "rcpparval"},
		{16, 11, "5/3/*/2", "rcpparval"},
		{16, 15, "2/*/4/3/*/2", "rcpparval"},
		{16, 23, "2/*/2", "rcpparval"},
	},
	notes: []AnnotationRule{{"unparsed-entry", "source-text"}, {"skipped-bytes", "source-text"}, {"unrecognised-line", "source-text"}},
}

// The embedded default is the baseline table, every row and both paths of a two-path row, nothing more.
func TestBaselineIsTheTable(t *testing.T) {
	t.Parallel()
	b := Baseline()
	if b.ID != "baseline" || b.Version != 1 {
		t.Errorf("baseline is %s version %d, want baseline version 1", b.ID, b.Version)
	}
	if len(b.ItemRules) != len(semBaseline.items) || len(b.AnnotationRules) != len(semBaseline.notes) {
		t.Errorf("baseline holds %d item and %d annotation rules, the table %d and %d",
			len(b.ItemRules), len(b.AnnotationRules), len(semBaseline.items), len(semBaseline.notes))
	}
	table := Policy{ItemRules: semBaseline.items, AnnotationRules: semBaseline.notes}
	if m := b.Missing(&table); len(m) != 0 {
		t.Errorf("the embedded baseline lacks %q of the table", m)
	}
	if m := table.Missing(&b); len(m) != 0 {
		t.Errorf("the embedded baseline holds %q beyond the table", m)
	}
	// With encryption no two rules may overlap; the baseline compiles under both key ids.
	if _, err := b.Compile("d-1", "m-1"); err != nil {
		t.Errorf("the baseline does not compile with encryption: %v", err)
	}
}

func TestParseRefuses(t *testing.T) {
	t.Parallel()
	ok := "policy_id: p\nversion: 1\nitem_rules:\n  - {stream: 7, function: 3, path: \"2\", domain: ppbody}\n"
	if _, err := Parse([]byte(ok)); err != nil {
		t.Fatalf("Parse of a valid policy = %v", err)
	}
	for name, doc := range map[string]string{
		"an unknown field":            ok + "digest_key_id: d-1\n",
		"an unknown rule field":       "policy_id: p\nversion: 1\nitem_rules:\n  - {stream: 7, function: 3, path: \"2\", domain: d, key: x}\n",
		"no id":                       "version: 1\n",
		"a padded id":                 "policy_id: ' p'\nversion: 1\n",
		"an id of 129 characters":     "policy_id: " + strings.Repeat("p", 129) + "\nversion: 1\n",
		"version 0":                   "policy_id: p\nversion: 0\n",
		"a negative version":          "policy_id: p\nversion: -1\n",
		"a version above int64":       "policy_id: p\nversion: 9223372036854775808\n",
		"a stream above 127":          "policy_id: p\nversion: 1\nitem_rules:\n  - {stream: 128, function: 3, path: \"2\", domain: d}\n",
		"a function above 255":        "policy_id: p\nversion: 1\nitem_rules:\n  - {stream: 7, function: 256, path: \"2\", domain: d}\n",
		"a bad path":                  "policy_id: p\nversion: 1\nitem_rules:\n  - {stream: 7, function: 3, path: \"0/x\", domain: d}\n",
		"an empty domain":             "policy_id: p\nversion: 1\nitem_rules:\n  - {stream: 7, function: 3, path: \"2\", domain: \"\"}\n",
		"an unknown annotation":       "policy_id: p\nversion: 1\nannotation_rules:\n  - {kind: comment, domain: d}\n",
		"the unknown annotation":      "policy_id: p\nversion: 1\nannotation_rules:\n  - {kind: unknown, domain: d}\n",
		"two documents":               ok + "---\n" + ok,
		"a malformed second document": ok + "---\n[unterminated\n",
		"a fractional version":        "policy_id: p\nversion: 1.5\n",
		"a fractional stream":         "policy_id: p\nversion: 1\nitem_rules:\n  - {stream: 7.5, function: 3, path: \"2\", domain: d}\n",
		"a fractional function":       "policy_id: p\nversion: 1\nitem_rules:\n  - {stream: 7, function: 3.0, path: \"2\", domain: d}\n",
		"an exponent version":         "policy_id: p\nversion: 1e0\n",
		"not YAML":                    "{",
	} {
		if _, err := Parse([]byte(doc)); !errors.Is(err, ErrInvalid) {
			t.Errorf("Parse with %s = %v, want ErrInvalid", name, err)
		}
	}
}

// Overlapping rules are a policy without encryption may hold, and compiling them with an encryption key refuses them.
func TestOverlapRefusedWithEncryption(t *testing.T) {
	t.Parallel()
	p, err := Parse([]byte("policy_id: p\nversion: 1\nitem_rules:\n  - {stream: 15, function: 13, path: \"4\", domain: a}\n  - {stream: 15, function: 13, path: \"4/*/2\", domain: b}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Compile("d-1", ""); err != nil {
		t.Errorf("overlapping rules without encryption = %v", err)
	}
	if _, err := p.Compile("d-1", "m-1"); !errors.Is(err, ErrInvalid) {
		t.Errorf("overlapping rules with encryption = %v, want ErrInvalid", err)
	}
}

// The canonical bytes read back to the same policy, depend on the rules' order, and not on the YAML's layout.
func TestCanonical(t *testing.T) {
	t.Parallel()
	b := Baseline()
	bytesA, sumA := b.Canonical()
	back, err := FromCanonical(bytesA)
	if err != nil {
		t.Fatal(err)
	}
	if _, sumB := back.Canonical(); sumB != sumA {
		t.Error("the canonical form does not read back to the same hash")
	}
	flow, err := Parse([]byte("policy_id: p\nversion: 1\nitem_rules: [{stream: 7, function: 3, path: \"2\", domain: ppbody}]\n"))
	if err != nil {
		t.Fatal(err)
	}
	block, err := Parse([]byte("# a comment\npolicy_id: p\nversion: 1\nitem_rules:\n  - stream: 7\n    function: 3\n    path: \"2\"\n    domain: ppbody\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, s1 := flow.Canonical(); s1 != func() [32]byte { _, s := block.Canonical(); return s }() {
		t.Error("two layouts of one policy hash differently")
	}
	reordered := b
	reordered.ItemRules = slices.Clone(b.ItemRules)
	slices.Reverse(reordered.ItemRules)
	if _, s := reordered.Canonical(); s == sumA {
		t.Error("reordered rules hash the same")
	}
	if _, err := FromCanonical([]byte(`{"policy_id":"p","version":1,"item_rules":[],"annotation_rules":[],"extra":1}`)); !errors.Is(err, ErrInvalid) {
		t.Errorf("FromCanonical with an unknown field = %v", err)
	}
}

// A policy holding the baseline and more is complete; one lacking a rule names it.
func TestMissing(t *testing.T) {
	t.Parallel()
	base := Baseline()
	more := base
	more.ItemRules = append(slices.Clone(base.ItemRules), ItemRule{2, 41, "2/*/2", "cpval"})
	if m := more.Missing(&base); len(m) != 0 {
		t.Errorf("a policy beyond the baseline lacks %q", m)
	}
	less := base
	less.ItemRules = slices.Clone(base.ItemRules[1:])
	less.AnnotationRules = base.AnnotationRules[:2]
	if m := less.Missing(&base); len(m) != 2 || !strings.Contains(m[0], "S7F3") || !strings.Contains(m[1], "unrecognised-line") {
		t.Errorf("Missing = %q, want S7F3's rule and unrecognised-line", m)
	}
	otherDomain := base
	otherDomain.ItemRules = slices.Clone(base.ItemRules)
	otherDomain.ItemRules[0].Domain = "elsewhere"
	if m := otherDomain.Missing(&base); len(m) != 1 {
		t.Errorf("a rule in another domain: Missing = %q, want it missing", m)
	}
}

// The redact.Policy a configuration compiles carries its key ids, encryption only with an encryption key id.
func TestRedactCarriesTheKeyIDs(t *testing.T) {
	t.Parallel()
	b := Baseline()
	r := b.Redact("d-1", "m-1")
	if r.ID != "baseline" || r.Version != 1 || r.DigestKeyID != "d-1" || r.Encryption == nil || r.Encryption.KeyID != "m-1" ||
		len(r.ItemRules) != len(b.ItemRules) || len(r.AnnotationRules) != 3 || r.AnnotationRules[0].Kind.String() != "unparsed-entry" {
		t.Errorf("Redact = %+v", r)
	}
	if r := b.Redact("d-1", ""); r.Encryption != nil {
		t.Error("Redact without an encryption key id has encryption")
	}
}
