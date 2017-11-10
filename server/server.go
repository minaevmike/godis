package server

import (
	"io"
	"net"

	"github.com/minaevmike/godis/godis_proto"
	"github.com/minaevmike/godis/wire"
	"go.uber.org/zap"
)

func NewServer(
	logger *zap.Logger,
) *Server {
	return &Server{
		log:          logger,
		wireProtocol: wire.NewSimpleWireProtocol(),
	}
}

type Server struct {
	log          *zap.Logger
	wireProtocol wire.Protocol
}

func errorPermament(err error) bool {
	if ne, ok := err.(net.Error); ok {
		if !ne.Temporary() {
			return true
		}
	}
	return false

}

func (s *Server) Run(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			s.log.Error("error accepting", zap.Error(err))
			if errorPermament(err) {
				return err
			}
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		req := &godis_proto.Request{}
		err := s.wireProtocol.Read(conn, req)
		if err != nil {
			if err != io.EOF {
				s.log.Error("can't read", zap.Error(err))
			}
			return
		}
		s.log.Debug("have request", zap.Any("request", req))
	}
}
