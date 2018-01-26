package wire

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"

	"github.com/minaevmike/godis/codec"
)

// wire protocol is very simple:
// 1. client serialize request into bytes
// 2. client writes request size in big-endian byte order to server
// 3. client writes message
// 4. server makes same
// 5. client reads size of message and then message itself
type Protocol interface {
	Read(conn net.Conn, dst interface{}) error
	Write(conn net.Conn, src interface{}) error
}

func NewSimpleWireProtocol(codec codec.Codec) Protocol {
	return &simpleProtocol{codec: codec}
}

type simpleProtocol struct {
	codec codec.Codec
}

func (s *simpleProtocol) Read(conn net.Conn, dst interface{}) error {
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

	return s.codec.Unmarshal(message, dst)
}
func (s *simpleProtocol) Write(conn net.Conn, src interface{}) error {
	message, err := s.codec.Marshal(src)
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
