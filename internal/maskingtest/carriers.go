// Package maskingtest builds synthetic records that carry recipe content where a policy's rules point,
// with the secret bytes a masked pack must never hold, for the tests of masking at ingest (phase 3.3's no-clear-text boundary).
package maskingtest

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/policy"
)

// Carrier is one synthetic record: its kind and payload, and the secret bytes in it a mask must hide.
type Carrier struct {
	Name    string
	Kind    tracepack.Kind
	Payload []byte
	Secrets [][]byte
	// Rules are the item rules the record answers, by stream, function and path; none for an annotation.
	Rules []policy.ItemRule
}

// Carriers returns one data record per stream and function of p's item rules, every rule's path resolving to a leaf holding its own secret,
// so the record is masked item by item rather than whole; and one annotation record per annotation rule, its text a secret.
func Carriers(p *policy.Policy) ([]Carrier, error) {
	type sf struct{ stream, function uint8 }
	var order []sf
	groups := map[sf][]policy.ItemRule{}
	for _, r := range p.ItemRules {
		k := sf{r.Stream, r.Function}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	var out []Carrier
	for _, k := range order {
		root := &node{}
		var secrets [][]byte
		for i, r := range groups[k] {
			secret := []byte(fmt.Sprintf("SECRET-S%dF%d-%d", k.stream, k.function, i))
			if err := root.insert(r.Path, secret); err != nil {
				return nil, fmt.Errorf("maskingtest: S%dF%d %q: %w", k.stream, k.function, r.Path, err)
			}
			secrets = append(secrets, secret)
		}
		out = append(out, Carrier{
			Name: fmt.Sprintf("S%dF%d", k.stream, k.function), Kind: tracepack.KindData,
			Payload: Frame(k.stream, k.function, root.render()), Secrets: secrets, Rules: groups[k],
		})
	}
	for _, r := range p.AnnotationRules {
		secret := "SECRET-" + r.Kind
		a, err := annotation(r.Kind, secret)
		if err != nil {
			return nil, err
		}
		b, err := a.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("maskingtest: %s: %w", r.Kind, err)
		}
		out = append(out, Carrier{Name: r.Kind, Kind: tracepack.KindAnnotation, Payload: b, Secrets: [][]byte{[]byte(secret)}})
	}

	return out, nil
}

// annotation is an annotation of kind holding secret, as text or, for skipped bytes, as raw bytes.
func annotation(kind, secret string) (*tracepack.Annotation, error) {
	for k := tracepack.AnnotationKindNote; k <= tracepack.AnnotationKindUnrecognisedLine; k++ {
		if k.String() != kind {
			continue
		}
		if k == tracepack.AnnotationKindSkippedBytes {
			return &tracepack.Annotation{AnnotationKind: k, Raw: []byte(secret)}, nil
		}

		return &tracepack.Annotation{AnnotationKind: k, Text: &secret}, nil
	}

	return nil, fmt.Errorf("maskingtest: unknown annotation kind %q", kind)
}

// WholeText is an S7F3 whose message text is not one valid item, holding a secret, which a policy masks whole.
func WholeText() Carrier {
	secret := []byte("SECRET-WHOLE-TEXT")
	text := append([]byte{0xFF, 0xFF}, secret...)

	return Carrier{Name: "whole text", Kind: tracepack.KindData, Payload: Frame(7, 3, text), Secrets: [][]byte{secret}}
}

// TooDeep is an S7F3 whose text nests lists past the masking walker's limit of 64, which masking cannot screen.
func TooDeep() Carrier {
	secret := []byte("SECRET-TOO-DEEP")
	text := ASCII(string(secret))
	for range 70 {
		text = List(text)
	}

	return Carrier{Name: "too deep", Kind: tracepack.KindData, Payload: Frame(7, 3, text), Secrets: [][]byte{secret}}
}

// node is a message text being built: its leaf secret, or its numbered children and its every-element child.
type node struct {
	secret   []byte
	children map[int]*node
	star     *node
}

// insert puts secret at path, a 1-based item path pattern with * for every element.
func (n *node) insert(path string, secret []byte) error {
	if path == "" {
		if n.children != nil || n.star != nil {
			return fmt.Errorf("a leaf where a list is needed")
		}
		n.secret = secret

		return nil
	}
	if n.secret != nil {
		return fmt.Errorf("a list where a leaf is")
	}
	seg, rest, _ := strings.Cut(path, "/")
	if seg == "*" {
		if n.star == nil {
			n.star = &node{}
		}

		return n.star.insert(rest, secret)
	}
	i, err := strconv.Atoi(seg)
	if err != nil || i < 1 {
		return fmt.Errorf("bad path step %q", seg)
	}
	if n.children == nil {
		n.children = map[int]*node{}
	}
	if n.children[i] == nil {
		n.children[i] = &node{}
	}

	return n.children[i].insert(rest, secret)
}

// render encodes n: a leaf as a binary item holding its secret, a list with every numbered child at its index,
// fillers elsewhere, and two elements for an every-element child.
func (n *node) render() []byte {
	if n.secret != nil {
		return Item(0o10, n.secret...)
	}
	if n.star != nil {
		return List(n.star.render(), n.star.render())
	}
	size := 0
	for i := range n.children {
		size = max(size, i)
	}
	elems := make([][]byte, size)
	for i := range elems {
		if c, ok := n.children[i+1]; ok {
			elems[i] = c.render()
		} else {
			elems[i] = ASCII("ID") // an identifier, which stays visible
		}
	}

	return List(elems...)
}

// Item is a SECS-II item of format code code with the fewest length bytes that hold body.
func Item(code byte, body ...byte) []byte {
	n := len(body)
	switch {
	case n < 1<<8:
		return append([]byte{code<<2 | 1, byte(n)}, body...)
	case n < 1<<16:
		return append([]byte{code<<2 | 2, byte(n >> 8), byte(n)}, body...)
	default:
		return append([]byte{code<<2 | 3, byte(n >> 16), byte(n >> 8), byte(n)}, body...)
	}
}

// ASCII is an ASCII item of s.
func ASCII(s string) []byte { return Item(0o20, []byte(s)...) }

// List is a SECS-II list of elems.
func List(elems ...[]byte) []byte {
	return slices.Concat(append([][]byte{{1, byte(len(elems))}}, elems...)...)
}

// Frame is the HSMS data frame of stream and function with text as its message text: W set, session 1, System Bytes 1.
func Frame(stream, function byte, text []byte) []byte {
	f := make([]byte, 14, 14+len(text))
	binary.BigEndian.PutUint32(f, uint32(10+len(text))) //nolint:gosec // synthetic texts are small
	f[5], f[6], f[7], f[13] = 1, 0x80|stream, function, 1

	return append(f, text...)
}
