package storage

/*
$ go test -bench=.
goos: linux
goarch: amd64
pkg: github.com/minaevmike/godis/storage
BenchmarkMapStorage_Get-8              	10000000	       225 ns/op
BenchmarkShardMapStorage_Get-8         	10000000	       231 ns/op
BenchmarkMapStorage_Set-8              	 1000000	      1291 ns/op
BenchmarkShardMapStorage_Set-8         	 1000000	      1230 ns/op
BenchmarkMapStorage_SetAndGet-8        	 3000000	       536 ns/op
BenchmarkShardMapStorage_SetAndGet-8   	 5000000	       393 ns/op
BenchmarkMapStorage_ForEach-8          	   10000	    543119 ns/op
BenchmarkShardMapStorage_ForEach-8     	   10000	    251906 ns/op
PASS
ok  	github.com/minaevmike/godis/storage	77.827s

*/

import (
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/minaevmike/godis/godis_proto"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func BenchmarkMapStorage_Get(b *testing.B) {
	st := NewMapStorage()
	benchGet(st, b)
}

func BenchmarkShardMapStorage_Get(b *testing.B) {
	st := NewShardMapStorage(32)
	benchGet(st, b)
}

func BenchmarkMapStorage_Set(b *testing.B) {
	st := NewMapStorage()
	benchSet(st, b)
}

func BenchmarkShardMapStorage_Set(b *testing.B) {
	st := NewShardMapStorage(32)
	benchSet(st, b)
}

func BenchmarkMapStorage_SetAndGet(b *testing.B) {
	st := NewMapStorage()
	benchSetAndGet(st, b)
}

func BenchmarkShardMapStorage_SetAndGet(b *testing.B) {
	st := NewShardMapStorage(32)
	benchSetAndGet(st, b)
}

func BenchmarkMapStorage_ForEach(b *testing.B) {
	st := NewMapStorage()
	benchForEach(st, b)
}

func BenchmarkShardMapStorage_ForEach(b *testing.B) {
	st := NewShardMapStorage(32)
	benchForEach(st, b)
}

func benchGet(storage Storage, b *testing.B) {
	keys := []string{}
	for i := 0; i < b.N; i++ {
		key := randSeq(10)
		keys = append(keys, key)
		storage.Set(randSeq(10), &godis_proto.Value{})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			storage.Get(keys[rand.Int()%len(keys)])
		}
	})
}

func benchSet(storage Storage, b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			storage.Set(randSeq(10), &godis_proto.Value{})
		}
	})
}

func benchSetAndGet(storage Storage, b *testing.B) {
	keys := []string{}
	n := b.N
	n *= 4
	for i := 0; i < n; i++ {
		key := randSeq(10)
		keys = append(keys, key)
	}
	pos := int64(0)
	lenKeys := int64(len(keys))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			storage.Set(keys[pos%lenKeys], &godis_proto.Value{})
			pos = atomic.AddInt64(&pos, 1)
			storage.Get(keys[pos%lenKeys])
			pos = atomic.AddInt64(&pos, 1)
		}
	})
}

func benchForEach(storage Storage, b *testing.B) {
	n := b.N
	n *= 4
	for i := 0; i < n; i++ {
		key := randSeq(10)
		storage.Set(key, &godis_proto.Value{})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.ForEach(func(key string, value *godis_proto.Value) {

		})
	}
}
