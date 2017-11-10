package client

import (
	"net"

	"github.com/minaevmike/godis/codec"
	"github.com/minaevmike/godis/godis_proto"
	"github.com/minaevmike/godis/wire"
	"gopkg.in/fatih/pool.v2"
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

func (c *Client) Get(key string) (*godis_proto.Response, error) {
	conn, err := c.connectionPool.Get()
	if err != nil {
		return nil, err
	}

	req := &godis_proto.Request{
		Key:       key,
		Operation: godis_proto.Operation_Get,
	}

	err = c.wireProtocol.Write(conn, req)
	if err != nil {
		return nil, err
	}
	resp := &godis_proto.Response{}
	err = c.wireProtocol.Read(conn, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) Set(key string, val *godis_proto.Value) error {
	conn, err := c.connectionPool.Get()
	if err != nil {
		return err
	}

	req := &godis_proto.Request{
		Key:       key,
		Operation: godis_proto.Operation_Set,
		Value:     val,
	}

	err = c.wireProtocol.Write(conn, req)
	if err != nil {
		return err
	}
	resp := &godis_proto.Response{}
	err = c.wireProtocol.Read(conn, resp)
	if err != nil {
		return err
	}
	return nil
}
