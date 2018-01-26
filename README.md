# godis
godis is a simple key value storage.
## build
```
go get github.com/minaevmike/godis
cd $GOPATH/src/github.com/minaevmike/godis
go build ./
./godis
```
Without any parameters it would listen `localhost:4321`.
## Supported commands
Get
Set
Delete
Keys
## Protocol
As serializer/deserializer godis uses protobuf.
wire protocol is very simple:
* client serialize request into bytes
* client writes request size in big-endian byte order to server
* client write serialized message
* server makes same
* client reads size of message and then message itself
## Benchmarks
Intel(R) Core(TM) i7-3770 CPU @ 3.40GHz 16 Gb Ram
```
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

```
## Value
`Value` can be one of:
* `string`
* `[]string`
* `map[string]string`
## API
low level api discrives in [proto file](https://github.com/minaevmike/godis/blob/master/godis_proto/godis.proto)
### Get
Returns value(`Value` in protoubf) by key
### Set 
Sets value for given key, old key would be overriten
### Remove
Removes key from storage, NOTE: no error would be return if removing key doesnt' exists
### Keys
Keys returned all keys that matches to given regexp(regexp syntax take from [go `regexp`](https://golang.org/pkg/regexp/#pkg-overview)
### GetByIndex
If stored value by given key is slice - it would return element with given index from this slice
### GetByKey
If stored value by given key is map- it would return element value from this map by given key
## Client
[client soruce](https://github.com/minaevmike/godis/tree/master/client)
## Example
```
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
```
