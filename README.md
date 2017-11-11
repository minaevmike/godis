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
