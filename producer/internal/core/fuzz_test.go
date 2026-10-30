package core

import (
	"bytes"
	"testing"
)

// item parses one complete SECS-II item with no zero-length item in it and returns the rest, recursively; ok is false otherwise.
func item(b []byte) (rest []byte, ok bool) {
	if len(b) == 0 || b[0]&3 == 0 || len(b) < 1+int(b[0]&3) {
		return nil, false
	}
	nb := int(b[0] & 3)
	n := 0
	for _, x := range b[1 : 1+nb] {
		n = n<<8 | int(x)
	}
	list := b[0]>>2 == listFormat
	b = b[1+nb:]
	if n == 0 {
		return nil, false
	}
	if !list {
		if len(b) < n {
			return nil, false
		}
		return b[n:], true
	}
	for range n {
		if b, ok = item(b); !ok {
			return nil, false
		}
	}

	return b, true
}

// The flat scan agrees with a recursive parse: a text is normal exactly when it is one complete item with no zero-length item.
func FuzzAbnormal(f *testing.F) {
	for _, seed := range [][]byte{sv, {0x01, 0x00}, {0x01, 0x01}, {0x01, 0x01, 0x42, 0x00, 0x03, 'a', 'b', 'c'}, {0xa5, 0x01, 0x07, 0x00}, {0x01, 0x03, 0xa5, 0x01, 0x07}, bytes.Repeat([]byte{0x03, 0xff, 0xff, 0xff}, 129)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text []byte) {
		if len(text) > 1<<12 {
			return // a deep recursion is the reference's limit, not the scan's
		}
		rest, ok := item(text)
		if want := !ok || len(rest) != 0; abnormal(text) != want {
			t.Errorf("abnormal(% x) = %v, the recursive parse says %v", text, !want, want)
		}
	})
}
