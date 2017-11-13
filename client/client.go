package client

import (
	"net"

	"errors"
	"fmt"
	"github.com/minaevmike/godis/codec"
	"github.com/minaevmike/godis/godis_proto"
	"github.com/minaevmike/godis/wire"
	"gopkg.in/fatih/pool.v2"
	"time"
)

func Dial(addr string) (*Client, error) {
	factory := func() (net.Conn, error) {
		return net.Dial("tcp", addr)
	}
	p, err := pool.NewChannelPool(0, 30, factory)
	if err != nil {
		return nil, err
	}

	return &Client{connectionPool: p, wireProtocol: wire.NewSimpleWireProtocol(codec.NewProtoCodec())}, nil
}

type Client struct {
	connectionPool pool.Pool
	wireProtocol   wire.Protocol
}

func (c *Client) Close() {
	c.connectionPool.Close()
}

func (c *Client) get(key string) (*godis_proto.Response, error) {
	conn, err := c.connectionPool.Get()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	req := &godis_proto.Request{
		Key:       key,
		Operation: godis_proto.Operation_Get,
	}

	resp, err := c.writeRequestReadResponse(conn, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) set(key string, val *godis_proto.Value) error {
	conn, err := c.connectionPool.Get()
	if err != nil {
		return err
	}
	defer conn.Close()

	req := &godis_proto.Request{
		Key:       key,
		Operation: godis_proto.Operation_Set,
		Value:     val,
	}

	_, err = c.writeRequestReadResponse(conn, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Keys(exp string) ([]string, error) {
	conn, err := c.connectionPool.Get()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	req := &godis_proto.Request{
		Key:       exp,
		Operation: godis_proto.Operation_Keys,
	}

	resp, err := c.writeRequestReadResponse(conn, req)
	if err != nil {
		return nil, err
	}

	return resp.GetValue().GetStringSlice().GetStringArrayVal(), nil
}

func (c *Client) Remove(key string) error {
	conn, err := c.connectionPool.Get()
	if err != nil {
		return err
	}
	defer conn.Close()

	req := &godis_proto.Request{
		Key:       key,
		Operation: godis_proto.Operation_Remove,
	}

	_, err = c.writeRequestReadResponse(conn, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) writeRequestReadResponse(conn net.Conn, req *godis_proto.Request) (*godis_proto.Response, error) {
	err := c.wireProtocol.Write(conn, req)
	if err != nil {
		return nil, err
	}
	resp := &godis_proto.Response{}
	err = c.wireProtocol.Read(conn, resp)
	if err != nil {
		return nil, err
	}

	if resp.GetError() != nil {
		return nil, errors.New(resp.GetError().GetMessage())
	}

	return resp, nil
}

func (c *Client) SetString(key, val string, ttl time.Duration) error {
	return c.set(key, &godis_proto.Value{
		Value: &godis_proto.Value_StringVal{StringVal: val},
		Ttl:   ttl.Nanoseconds() + time.Now().UnixNano(),
	},
	)
}

func (c *Client) SetSlice(key string, val []string, ttl time.Duration) error {
	return c.set(key, &godis_proto.Value{
		Value: &godis_proto.Value_StringSlice{StringSlice: &godis_proto.RepeatedString{StringArrayVal: val}},
		Ttl:   ttl.Nanoseconds() + time.Now().UnixNano(),
	})
}

func (c *Client) SetMap(key string, val map[string]string, ttl time.Duration) error {
	return c.set(key, &godis_proto.Value{
		Value: &godis_proto.Value_StringMap{StringMap: &godis_proto.MapString{StringMap: val}},
		Ttl:   ttl.Nanoseconds() + time.Now().UnixNano(),
	})
}

func (c *Client) GetString(key string) (string, error) {
	v, err := c.get(key)
	if err != nil {
		return "", err
	}

	switch t := v.GetValue().GetValue().(type) {
	case *godis_proto.Value_StringVal:
		return t.StringVal, nil
	default:
		return "", fmt.Errorf("key has another type %T", t)
	}
}

func (c *Client) GetSlice(key string) ([]string, error) {
	v, err := c.get(key)
	if err != nil {
		return nil, err
	}

	switch t := v.GetValue().GetValue().(type) {
	case *godis_proto.Value_StringSlice:
		return t.StringSlice.GetStringArrayVal(), nil
	default:
		return nil, fmt.Errorf("key has another type %T", t)
	}
}

func (c *Client) GetMap(key string) (map[string]string, error) {
	v, err := c.get(key)
	if err != nil {
		return nil, err
	}

	switch t := v.GetValue().GetValue().(type) {
	case *godis_proto.Value_StringMap:
		return t.StringMap.GetStringMap(), nil
	default:
		return nil, fmt.Errorf("key has another type %T", t)
	}
}

func (c *Client) GetByIndex(key string, index int) (string, error) {
	conn, err := c.connectionPool.Get()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	req := &godis_proto.Request{
		Key:       key,
		Operation: godis_proto.Operation_GetByIndex,
		Index: uint32(index),
	}

	v, err := c.writeRequestReadResponse(conn, req)
	if err != nil {
		return "", err
	}

	switch t := v.GetValue().GetValue().(type) {
	case *godis_proto.Value_StringVal:
		return t.StringVal, nil
	default:
		return "", fmt.Errorf("key has another type %T", t)
	}
}

func (c *Client) GetByMapKey(key string, mapKey string) (string, error) {
	conn, err := c.connectionPool.Get()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	req := &godis_proto.Request{
		Key:       key,
		Operation: godis_proto.Operation_GetByKey,
		MapKey: mapKey,
	}

	v, err := c.writeRequestReadResponse(conn, req)
	if err != nil {
		return "", err
	}

	switch t := v.GetValue().GetValue().(type) {
	case *godis_proto.Value_StringVal:
		return t.StringVal, nil
	default:
		return "", fmt.Errorf("key has another type %T", t)
	}
}