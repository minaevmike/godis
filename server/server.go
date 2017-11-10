package server

import (
	"io"
	"net"

	"github.com/minaevmike/godis/codec"
	"github.com/minaevmike/godis/godis_proto"
	"github.com/minaevmike/godis/storage"
	"github.com/minaevmike/godis/wire"
	"go.uber.org/zap"
)

func NewServer(
	logger *zap.Logger,
) *Server {
	return &Server{
		log:          logger,
		wireProtocol: wire.NewSimpleWireProtocol(codec.NewProtoCodec()),
		stopChan:     make(chan struct{}),
		storage:      storage.NewShardMapStorage(32),
	}
}

type Server struct {
	log          *zap.Logger
	wireProtocol wire.Protocol
	stopChan     chan struct{}
	storage      storage.Storage
}

func errorPermament(err error) bool {
	if ne, ok := err.(net.Error); ok {
		if !ne.Temporary() {
			return true
		}
	}
	return false

}

func (s *Server) Shutdown() {
	s.stopChan <- struct{}{}
}

func (s *Server) Run(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil
	}
	go func() {
		<-s.stopChan
		l.Close()
	}()
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
		switch req.Operation {
		case godis_proto.Operation_Get:
			v, err := s.storage.Get(req.GetKey())
			if err != nil {
				s.wireProtocol.Write(conn, &godis_proto.Response{Error: &godis_proto.Error{Message: err.Error()}})
				continue
			}
			s.wireProtocol.Write(conn, &godis_proto.Response{Value: v})
		case godis_proto.Operation_Set:
			err := s.storage.Set(req.GetKey(), req.GetValue())
			if err != nil {
				s.wireProtocol.Write(conn, &godis_proto.Response{Error: &godis_proto.Error{Message: err.Error()}})
				continue
			}
			s.wireProtocol.Write(conn, &godis_proto.Response{})
		default:
			s.wireProtocol.Write(conn, &godis_proto.Response{Error: &godis_proto.Error{Message: "not implemented"}})
		}

	}
}
