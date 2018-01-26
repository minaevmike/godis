package test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/minaevmike/godis/client"
	"github.com/minaevmike/godis/server"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func startServer(addr string) *server.Server {
	l, _ := zap.NewProduction()
	s := server.NewServer(l)
	go s.Run(addr)
	time.Sleep(time.Millisecond)
	return s
}

func TestServer_SetGetString(t *testing.T) {
	addr := fmt.Sprintf("localhost:%d", freeport.GetPort())
	s := startServer(addr)
	cl, err := client.Dial(addr)
	assert.Nil(t, err)

	err = cl.SetString("abc", "test", time.Hour)
	assert.Nil(t, err)

	valStr, err := cl.GetString("abc")
	assert.Nil(t, err)
	assert.Equal(t, valStr, "test")

	valArr, err := cl.GetSlice("abc")
	assert.Nil(t, valArr)
	assert.NotNil(t, err)

	valMap, err := cl.GetMap("abc")
	assert.Nil(t, valMap)
	assert.NotNil(t, err)

	s.Shutdown()
	cl.Close()
}

func TestServer_SetGetSlice(t *testing.T) {
	addr := fmt.Sprintf("localhost:%d", freeport.GetPort())
	s := startServer(addr)
	cl, err := client.Dial(addr)
	assert.Nil(t, err)

	err = cl.SetSlice("abc", []string{"test1", "test2"}, time.Hour)
	assert.Nil(t, err)

	valStr, err := cl.GetString("abc")
	assert.NotNil(t, err)
	assert.Equal(t, valStr, "")

	valArr, err := cl.GetSlice("abc")
	assert.Nil(t, err)
	assert.Equal(t, valArr, []string{"test1", "test2"})

	valMap, err := cl.GetMap("abc")
	assert.Nil(t, valMap)
	assert.NotNil(t, err)

	val1, err := cl.GetByIndex("abc", 1)
	assert.Nil(t, err)
	assert.Equal(t, val1, "test2")

	val2, err := cl.GetByIndex("abc", 3)
	assert.NotNil(t, err)
	assert.Equal(t, val2, "")

	s.Shutdown()
	cl.Close()
}

func TestServer_SetGetMap(t *testing.T) {
	addr := fmt.Sprintf("localhost:%d", freeport.GetPort())
	s := startServer(addr)
	cl, err := client.Dial(addr)
	assert.Nil(t, err)

	m := map[string]string{
		"abc": "test",
	}

	err = cl.SetMap("abc", m, time.Hour)
	assert.Nil(t, err)

	valStr, err := cl.GetString("abc")
	assert.NotNil(t, err)
	assert.Equal(t, valStr, "")

	valArr, err := cl.GetSlice("abc")
	assert.Nil(t, valArr)
	assert.NotNil(t, err)

	valMap, err := cl.GetMap("abc")
	assert.Nil(t, err)
	assert.Equal(t, valMap, m)

	val1, err := cl.GetByMapKey("abc", "abc")
	assert.Nil(t, err)
	assert.Equal(t, val1, "test")

	val2, err := cl.GetByMapKey("abc", "nothing")
	assert.NotNil(t, err)
	assert.Equal(t, val2, "")

	s.Shutdown()
	cl.Close()
}

func TestServer_SetGetRemove(t *testing.T) {
	addr := fmt.Sprintf("localhost:%d", freeport.GetPort())
	s := startServer(addr)
	cl, err := client.Dial(addr)
	assert.Nil(t, err)

	err = cl.SetString("abc", "test", time.Hour)
	assert.Nil(t, err)

	valStr, err := cl.GetString("abc")
	assert.Nil(t, err)
	assert.Equal(t, valStr, "test")

	err = cl.Remove("abc")
	assert.Nil(t, err)

	valStr, err = cl.GetString("abc")
	assert.NotNil(t, err)
	assert.Equal(t, valStr, "")

	s.Shutdown()
	cl.Close()
}

func TestServer_Keys(t *testing.T) {
	addr := fmt.Sprintf("localhost:%d", freeport.GetPort())
	s := startServer(addr)
	cl, err := client.Dial(addr)
	assert.Nil(t, err)

	err = cl.SetString("hello", "aaa", time.Hour)
	assert.Nil(t, err)

	err = cl.SetString("hallo", "bbb", time.Hour)
	assert.Nil(t, err)

	err = cl.SetString("hi", "ccc", time.Hour)
	assert.Nil(t, err)

	val, err := cl.Keys("h*")

	assert.Nil(t, err)
	sort.Strings(val)
	expRes1 := []string{"hello", "hallo", "hi"}
	sort.Strings(expRes1)
	assert.Equal(t, val, expRes1)

	val, err = cl.Keys("h?llo")
	assert.Nil(t, err)
	sort.Strings(val)
	expRes2 := []string{"hello", "hallo"}
	sort.Strings(expRes2)
	assert.Equal(t, val, expRes2)

	val, err = cl.Keys("nothing")
	assert.Nil(t, err)
	assert.Equal(t, len(val), 0)
	s.Shutdown()
	cl.Close()
}

func TestServer_Ttl(t *testing.T) {
	addr := fmt.Sprintf("localhost:%d", freeport.GetPort())
	s := startServer(addr)
	cl, err := client.Dial(addr)
	assert.Nil(t, err)

	err = cl.SetString("hi", "ccc", time.Millisecond)
	assert.Nil(t, err)

	// Ttl must expire
	time.Sleep(10 * time.Millisecond)

	val, err := cl.GetString("hi")
	assert.NotNil(t, err)
	assert.Equal(t, val, "")

	err = cl.SetString("a", "aaa", time.Millisecond)
	assert.Nil(t, err)
	err = cl.SetString("aa", "aaaa", 2*time.Second)

	// Ttl must expire for a
	time.Sleep(10 * time.Millisecond)

	keys, err := cl.Keys("a?")
	assert.Nil(t, err)
	assert.Equal(t, keys, []string{"aa"})

	val, err = cl.GetString("a")
	assert.NotNil(t, err)
	assert.Equal(t, val, "")

	s.Shutdown()
	cl.Close()
}
