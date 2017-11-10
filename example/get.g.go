package main

import (
	"log"

	"github.com/minaevmike/godis/client"
)

func main() {
	cl, err := client.Dial("localhost:4321")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		log.Print(cl.Get("abc"))
	}
}
