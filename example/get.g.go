package main

import (
	"log"

	"fmt"

	"github.com/minaevmike/godis/client"
	"github.com/minaevmike/godis/godis_proto"
)

func main() {
	cl, err := client.Dial("localhost:4321")
	if err != nil {
		log.Fatal(err)
	}
	err = cl.Set("abc", &godis_proto.Value{Value: &godis_proto.Value_StringVal{StringVal: "abc213"}})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(cl.Get("abc"))
}
