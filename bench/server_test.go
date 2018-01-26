package bench

import (
	"testing"
	"time"

	"math/rand"
	"os"
	"sort"
	"sync/atomic"

	"github.com/minaevmike/godis/client"
	"github.com/minaevmike/godis/server"
	"go.uber.org/zap"
)

/*
$ go test -bench=. -race
goos: linux
goarch: amd64
pkg: github.com/minaevmike/godis/bench
Benchmark_ServerGet-8                    	   20000	     77328 ns/op	3879.57 MB/s
Benchmark_ServerGet_Parallel-8           	   50000	     27596 ns/op	27177.49 MB/s
Benchmark_Server_100000Keys-8            	       2	1004202453 ns/op	   1.99 MB/s
Benchmark_Server_100000Keys_Parallel-8   	       1	1034168047 ns/op	   0.97 MB/s
Benchmark_ServerSet-8                    	   10000	    100471 ns/op	2488.28 MB/s
Benchmark_ServerSet_Parallel-8           	   30000	     54008 ns/op	13886.78 MB/s
PASS
ok  	github.com/minaevmike/godis/bench	22.430s

$ go test -bench=.
goos: linux
goarch: amd64
pkg: github.com/minaevmike/godis/bench
Benchmark_ServerGet-8                    	   50000	     27510 ns/op	27261.98 MB/s
Benchmark_ServerGet_Parallel-8           	  200000	      8815 ns/op	340297.77 MB/s
Benchmark_Server_100000Keys-8            	      20	  65291591 ns/op	 306.32 MB/s
Benchmark_Server_100000Keys_Parallel-8   	      50	  25912034 ns/op	1929.61 MB/s
Benchmark_ServerSet-8                    	   50000	     27873 ns/op	44846.00 MB/s
Benchmark_ServerSet_Parallel-8           	  200000	      9157 ns/op	545978.75 MB/s
PASS
ok  	github.com/minaevmike/godis/bench	13.552s


*/

const (
	addr = "localhost:6543"
)

var s *server.Server
var cl *client.Client
var keys []string

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func Benchmark_ServerGet(b *testing.B) {
	keysSize := len(keys)
	read := int64(0)
	for i := 0; i < b.N; i++ {
		val, err := cl.GetString(keys[i%keysSize])
		if err != nil {
			b.Error(err)
		}
		read += int64(len(val))
	}
	b.SetBytes(read)
}

func Benchmark_ServerGet_Parallel(b *testing.B) {
	keysSize := int64(len(keys))
	i := int64(0)
	read := int64(0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddInt64(&i, 1)
			val, err := cl.GetString(keys[atomic.LoadInt64(&i)%keysSize])
			if err != nil {
				b.Error(err)
			}
			atomic.AddInt64(&read, int64(len(val)))
		}
	})
	b.SetBytes(atomic.LoadInt64(&read))
}

func Benchmark_Server_100000Keys(b *testing.B) {
	read := int64(0)
	for i := 0; i < b.N; i++ {
		keys, err := cl.Keys(".*")
		if err != nil {
			b.Error(err)
		}
		for i := range keys {
			read += int64(len(keys[i]))
		}
	}
	b.SetBytes(read)
}

func Benchmark_Server_100000Keys_Parallel(b *testing.B) {
	read := int64(0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keys, err := cl.Keys(".*")
			if err != nil {
				b.Error(err)
			}
			for i := range keys {
				atomic.AddInt64(&read, int64(len(keys[i])))
			}
		}
	})
	b.SetBytes(atomic.LoadInt64(&read))
}

func Benchmark_ServerSet(b *testing.B) {
	keysSize := len(keys)
	read := int64(0)
	for i := 0; i < b.N; i++ {
		err := cl.SetString(randSeq(15), keys[i%keysSize], time.Hour)
		if err != nil {
			b.Error(err)
		}
		read += int64(15 + len(keys[i%keysSize]))
	}
	b.SetBytes(read)
}

func Benchmark_ServerSet_Parallel(b *testing.B) {
	keysSize := int64(len(keys))
	read := int64(0)
	i := int64(0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddInt64(&i, 0)
			err := cl.SetString(randSeq(15), keys[atomic.LoadInt64(&i)%keysSize], time.Hour)
			if err != nil {
				b.Error(err)
			}
			atomic.AddInt64(&read, int64(15+len(keys[atomic.LoadInt64(&i)%keysSize])))
		}
	})

	b.SetBytes(atomic.LoadInt64(&read))
}

func TestMain(m *testing.M) {
	l, _ := zap.NewProduction()
	s = server.NewServer(l)
	go func() {
		s.Run(addr)
	}()
	time.Sleep(time.Millisecond)
	var err error
	cl, err = client.Dial(addr)
	if err != nil {
		panic(err)
	}
	//create storage with 100000 keys
	for i := 0; i < 100000; i++ {
		k, v := randSeq(10), randSeq(15)
		err := cl.SetString(k, v, time.Hour)
		if err != nil {
			panic(err)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	code := m.Run()

	s.Shutdown()
	cl.Close()

	os.Exit(code)
}
