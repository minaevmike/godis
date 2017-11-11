package storage

import "github.com/minaevmike/godis/godis_proto"

type ForEachFunc func(key string, value *godis_proto.Value)

// Storage interface describes low level storage api
type Storage interface {
	// Get - gets value from storage by key
	Get(key string) (*godis_proto.Value, error)
	// Set - sets key with value, if key already exists - it would be overwritten
	Set(key string, value *godis_proto.Value) error
	// Delete - delete value from storage by key. In case when key doesn't exists no error would be returned
	Delete(key string) error
	// ForEach - executes given function with data in storage. fn can be called in separate goroutines
	ForEach(fn ForEachFunc)
}
