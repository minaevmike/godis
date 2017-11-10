package wire

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"

	"github.com/golang/protobuf/proto"
)

type Protocol interface {
	Read(conn net.Conn, dst proto.Message) error
	Write(conn net.Conn, src proto.Message) error
}

func NewSimpleWireProtocol() Protocol {
	return &simpleProtocol{}
}

type simpleProtocol struct {
}

func (s *simpleProtocol) Read(conn net.Conn, dst proto.Message) error {
	var length uint32
	err := binary.Read(conn, binary.BigEndian, &length)
	if err != nil {
		return err
	}

	message := make([]byte, length)
	_, err = io.ReadFull(conn, message)
	if err != nil {
		return err
	}

	return proto.Unmarshal(message, dst)
}
func (s *simpleProtocol) Write(conn net.Conn, src proto.Message) error {
	message, err := proto.Marshal(src)
	if err != nil {
		return err
	}

	buf := bufio.NewWriter(conn)
	err = binary.Write(buf, binary.BigEndian, uint32(len(message)))
	if err != nil {
		return err
	}

	_, err = buf.Write(message)
	if err != nil {
		return err
	}

	return buf.Flush()
}
