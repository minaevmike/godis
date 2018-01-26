package main

import (
	"fmt"
	"os"

	//"github.com/minaevmike/godis/server"
	"time"

	"github.com/minaevmike/godis/wal"
	"go.uber.org/zap"
)

func main() {
	log, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("can't create logger: %v", err)
		os.Exit(1)
	}
	w := wal.NewIntervalWAL("./godis.wal", time.Second, log)
	err = w.Write(wal.Write, []byte("key1"), []byte("value1"))
	if err != nil {
		log.Error("er", zap.Error(err))
	}

	err = w.Write(wal.Write, []byte("key2"), []byte("value2"))
	if err != nil {
		log.Error("er", zap.Error(err))
	}
	time.Sleep(2 * time.Second)
	//s := server.NewServer(log)
	//err = s.Run("localhost:4321")
	//if err != nil {
	//	log.Fatal("run server", zap.Error(err))
	//}
}
