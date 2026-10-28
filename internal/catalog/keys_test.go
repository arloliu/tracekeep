package catalog

import (
	"errors"
	"testing"
)

// downloadBinding is key id dl-1's download binding to fingerprint.
func downloadBinding(fingerprint string) KeyBinding {
	return KeyBinding{KeyID: "dl-1", Kind: KindDownload, ProviderRef: "file:dl-1", Fingerprint: fingerprint}
}

func TestBindKey(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		if _, ok, err := c.KeyBinding(ctx, "dl-1"); ok || err != nil {
			t.Fatalf("KeyBinding before any = %v, %v", ok, err)
		}
		first, err := c.BindKey(ctx, downloadBinding("aa"))
		if err != nil || first.BoundAt == 0 || first.Fingerprint != "aa" || first.Kind != KindDownload {
			t.Fatalf("first BindKey = %+v, %v", first, err)
		}
		stored, ok, err := c.KeyBinding(ctx, "dl-1")
		if !ok || err != nil || stored != first {
			t.Fatalf("KeyBinding = %+v, %v, %v; want %+v", stored, ok, err, first)
		}
		// A matching binding returns the stored row, never updated: its reference and version stay the first resolution's.
		again := downloadBinding("aa")
		again.ProviderVersion = "7"
		if got, err := c.BindKey(ctx, again); err != nil || got != first {
			t.Errorf("matching BindKey = %+v, %v; want the stored %+v", got, err, first)
		}
		other := downloadBinding("bb")
		if got, err := c.BindKey(ctx, other); !errors.Is(err, ErrKeyBindingMismatch) || got != first {
			t.Errorf("BindKey of other material = %+v, %v; want the stored binding and a mismatch", got, err)
		}
		purpose := downloadBinding("aa")
		purpose.Kind = KindSession
		if got, err := c.BindKey(ctx, purpose); !errors.Is(err, ErrKeyBindingMismatch) || got != first {
			t.Errorf("BindKey under another kind = %+v, %v; want the stored binding and a mismatch", got, err)
		}
		if stored, _, _ := c.KeyBinding(ctx, "dl-1"); stored != first {
			t.Errorf("binding after the mismatches = %+v, want %+v unchanged", stored, first)
		}
		if _, err := c.BindKey(ctx, KeyBinding{KeyID: "x", Kind: "cookie", Fingerprint: "aa"}); err == nil {
			t.Error("BindKey of an unknown kind succeeded")
		}
	})
}

// Two first resolutions of one key id race: the second waits on the first's insert,
// then compares with what the first bound, so with equal material both agree and with different material the second fails closed.
func TestTwoFirstBindings(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		second string
	}{{"equal material", "aa"}, {"different material", "bb"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engines(t, func(t *testing.T, c *Catalog) {
				held, release := holdAt(c, hookBeforeCommit)
				var b1, b2 KeyBinding
				var err1, err2 error
				a := run(func() { b1, err1 = c.BindKey(t.Context(), downloadBinding("aa")) })
				<-held
				waits := c.write.Stats().WaitCount
				b := run(func() { b2, err2 = c.BindKey(t.Context(), downloadBinding(tc.second)) })
				waitBlocked(t, c, waits, c.d.insertIgnore+" INTO key_bindings", 1)
				close(release)
				<-a
				<-b
				if err1 != nil || b1.Fingerprint != "aa" {
					t.Fatalf("first BindKey = %+v, %v", b1, err1)
				}
				if b2 != b1 {
					t.Errorf("second BindKey returned %+v, want the first's binding %+v", b2, b1)
				}
				if mismatch := errors.Is(err2, ErrKeyBindingMismatch); mismatch != (tc.second != "aa") || (!mismatch && err2 != nil) {
					t.Errorf("second BindKey error = %v", err2)
				}
			})
		})
	}
}
