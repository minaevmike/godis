package main

import (
	"log"

	"fmt"

	"io/ioutil"
	"sync"

	"github.com/minaevmike/godis/client"
	"github.com/minaevmike/godis/godis_proto"
)

func main() {
	cl, err := client.Dial("localhost:4321")
	if err != nil {
		log.Fatal(err)
	}
	wg := sync.WaitGroup{}
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = cl.Set("abc", &godis_proto.Value{Value: &godis_proto.Value_StringVal{StringVal: "abc213"}})
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(cl.Get("abc"))
		}()
	}
	ioutil.NopCloser()
	wg.Wait()
}
