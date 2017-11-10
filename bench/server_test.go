package bench

import (
	"testing"
	"time"

	"github.com/minaevmike/godis/client"
	"github.com/minaevmike/godis/server"
	"go.uber.org/zap"
)

const (
	addr = "localhost:6543"
)

func Benchmark_ServerGet(b *testing.B) {
	l, _ := zap.NewProduction()
	s := server.NewServer(l)
	go s.Run(addr)
	time.Sleep(time.Millisecond)
	cl, _ := client.Dial(addr)
	for i := 0; i < b.N; i++ {
		cl.Get("abc")
	}
	//s.Shutdown()
}
