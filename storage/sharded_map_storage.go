package storage

import (
	"sync"

	"github.com/cespare/xxhash"
	"github.com/minaevmike/godis/godis_proto"
)

type shardMapStorage struct {
	shards      []*mapStorage
	shardsCount uint64
}

func NewShardMapStorage(shards int) Storage {
	out := &shardMapStorage{}

	for i := 0; i < shards; i++ {
		out.shards = append(out.shards, &mapStorage{
			m: make(map[string]*godis_proto.Value),
		})
	}
	out.shardsCount = uint64(shards)
	return out
}

func (s *shardMapStorage) getShard(key string) *mapStorage {
	return s.shards[xxhash.Sum64String(key)%uint64(s.shardsCount)]
}

func (s *shardMapStorage) Get(key string) (*godis_proto.Value, error) {
	return s.getShard(key).Get(key)
}

func (s *shardMapStorage) Set(key string, value *godis_proto.Value) error {
	return s.getShard(key).Set(key, value)
}

func (s *shardMapStorage) Delete(key string) error {
	return s.getShard(key).Delete(key)
}

func (s *shardMapStorage) ForEach(fn ForEachFunc) {
	wg := &sync.WaitGroup{}
	for _, shard := range s.shards {
		wg.Add(1)
		go func(shard *mapStorage) {
			shard.ForEach(fn)
			wg.Done()
		}(shard)
	}

	wg.Wait()
}
