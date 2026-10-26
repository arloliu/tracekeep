package store_test

import (
	"testing"

	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/internal/store/storetest"
)

func TestMemConformance(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(*testing.T) store.ObjectStore { return store.NewMem() })
}
