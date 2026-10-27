package admin

import (
	"context"

	"github.com/arloliu/tracekeep/internal/catalog"
)

// Objects is object list: the claim objects retention leaves to the operator (compactor.md §8.4).
type Objects struct {
	// Survivors are the claim objects an eviction kept for a parked version, with their evicted hour and size,
	// deletable ones included until the compactor removes them.
	Survivors []catalog.ClaimObject `json:"survivors,omitempty"`
	// Unattributed are the claim bucket's objects without the SDK's metadata, which only an operator deletes.
	Unattributed []catalog.UnattributedObject `json:"unattributed,omitempty"`
}

// ObjectList is object list: the survivors, the unattributed objects, or both when neither is asked for.
func (a *Admin) ObjectList(ctx context.Context, survivors, unattributed bool) (Objects, error) {
	both := !survivors && !unattributed
	var out Objects
	var err error
	if survivors || both {
		if out.Survivors, err = a.Catalog.ClaimObjects(ctx, "survivor", "deletable"); err != nil {
			return Objects{}, err
		}
	}
	if unattributed || both {
		if out.Unattributed, err = a.Catalog.UnattributedObjects(ctx); err != nil {
			return Objects{}, err
		}
	}

	return out, nil
}
