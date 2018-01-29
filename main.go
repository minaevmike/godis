package main

import (
	"fmt"
	"os"

	"github.com/minaevmike/godis/server"
	"go.uber.org/zap"
)

func main() {
	log, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("can't create logger: %v", err)
		os.Exit(1)
	}

	s := server.NewServer(log)
	err = s.Run("localhost:4321")
	if err != nil {
		log.Fatal("run server", zap.Error(err))
	}
}
