package main

import (
	"log"

	"fmt"


	"github.com/minaevmike/godis/client"
	"time"
)

func main() {
	cl, err := client.Dial("localhost:4321")
	if err != nil {
		log.Fatal(err)
	}
	// Set string
	err = cl.SetString("key", "value", time.Hour)
	if err != nil {
		log.Fatal(err)
	}

	// Get string
	val, err := cl.GetString("key")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(val) // value

	// Keys get all by regexp
	keys, err := cl.Keys(".*")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(keys) // [key]

	// remove key
	err = cl.Remove("key")
	if err != nil {
		log.Fatal(err)
	}

	keys, err = cl.Keys(".*")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(len(keys)) // empty slice
}
