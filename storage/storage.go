package storage

import "github.com/minaevmike/godis/godis_proto"

type ForEachFunc func(key string, value *godis_proto.Value)

type Storage interface {
	Get(key string) (*godis_proto.Value, error)
	Set(key string, value *godis_proto.Value) error
	Delete(key string) error
	ForEach(fn ForEachFunc)
}
