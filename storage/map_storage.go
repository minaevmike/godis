package storage

import (
	"errors"
	"sync"

	"time"

	"github.com/minaevmike/godis/godis_proto"
)

var (
	ErrKeyDoesntExists = errors.New("key doesn't exists")
	ErrKeyExpired      = errors.New("key ttl expired")
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
	if time.Now().UnixNano() > v.Ttl {
		//key expired
		ms.Delete(key)
		return nil, ErrKeyExpired
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
	now := time.Now().UnixNano()
	var keysToDelete []string
	for k, v := range ms.m {
		if now > v.Ttl {
			// we need to delete this key
			// but to delete we need exclusive lock
			keysToDelete = append(keysToDelete, k)
		} else {
			fn(k, v)
		}
	}
	ms.mu.RUnlock()

	if len(keysToDelete) > 0 {
		go func() {
			ms.mu.Lock()
			for _, key := range keysToDelete {
				delete(ms.m, key)
			}
			ms.mu.Unlock()
		}()
	}

}
