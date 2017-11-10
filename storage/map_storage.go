package storage

import (
	"errors"
	"sync"

	"github.com/minaevmike/godis/godis_proto"
)

var (
	ErrKeyDoesntExists = errors.New("key doesn't exists")
)

func NewMapStorage() Storage {
	return &mapStorage{
		m: make(map[string]*godis_proto.Value),
	}
}

type mapStorage struct {
	m  map[string]*godis_proto.Value
	mu sync.RWMutex
}

func (ms *mapStorage) Get(key string) (*godis_proto.Value, error) {
	ms.mu.RLock()
	v, ok := ms.m[key]
	ms.mu.RUnlock()
	if !ok {
		return nil, ErrKeyDoesntExists
	}
	return v, nil
}

func (ms *mapStorage) Set(key string, value *godis_proto.Value) error {
	ms.mu.Lock()
	ms.m[key] = value
	ms.mu.Unlock()
	return nil
}

func (ms *mapStorage) Delete(key string) error {
	ms.mu.Lock()
	delete(ms.m, key)
	ms.mu.Unlock()
	return nil
}

func (ms *mapStorage) ForEach(fn ForEachFunc) {
	ms.mu.RLock()
	for k, v := range ms.m {
		fn(k, v)
	}
	ms.mu.RUnlock()
}
