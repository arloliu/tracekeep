package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Mem is an ObjectStore in memory, for tests.
type Mem struct {
	mu   sync.RWMutex
	objs map[string][]byte
}

var _ ObjectStore = (*Mem)(nil)

// NewMem returns an empty in-memory store.
func NewMem() *Mem {
	return &Mem{objs: make(map[string][]byte)}
}

// Put implements ObjectStore.
func (m *Mem) Put(_ context.Context, key string, r io.ReadSeeker, size int64) error {
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, r, size); err != nil {
		return fmt.Errorf("store: put %s: %w", key, err)
	}
	m.mu.Lock()
	m.objs[key] = buf.Bytes()
	m.mu.Unlock()

	return nil
}

// Open implements ObjectStore.
func (m *Mem) Open(_ context.Context, key string) (Object, error) {
	m.mu.RLock()
	b, ok := m.objs[key]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}

	return memObject{bytes.NewReader(b)}, nil
}

// Stat implements ObjectStore.
func (m *Mem) Stat(_ context.Context, key string) (ObjectInfo, error) {
	m.mu.RLock()
	b, ok := m.objs[key]
	m.mu.RUnlock()
	if !ok {
		return ObjectInfo{}, fmt.Errorf("%w: %s", ErrNotFound, key)
	}

	return ObjectInfo{Key: key, Size: int64(len(b))}, nil
}

// List implements ObjectStore.
func (m *Mem) List(_ context.Context, prefix string, fn func(ObjectInfo) error) error {
	m.mu.RLock()
	var infos []ObjectInfo
	for k, b := range m.objs {
		if strings.HasPrefix(k, prefix) {
			infos = append(infos, ObjectInfo{Key: k, Size: int64(len(b))})
		}
	}
	m.mu.RUnlock()
	for _, info := range infos {
		if err := fn(info); err != nil {
			return err
		}
	}

	return nil
}

// Delete implements ObjectStore.
func (m *Mem) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.objs, key)
	m.mu.Unlock()

	return nil
}

type memObject struct{ *bytes.Reader }

func (memObject) Close() error { return nil }
