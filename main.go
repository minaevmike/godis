package main

import (
	"github.com/minaevmike/godis/server"
	"flag"
	"log"
)

func main() {
	addr := flag.String("endpoint", "localhost:4321", "endpoint to listen")
	flag.Parse()
	err := server.ListenAndServe(*addr)
	if err != nil {
		log.Fatal(err)
	}

}
