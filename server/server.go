package server

import (
	"io"
	"net"

	"fmt"
	"os"
	"regexp"

	"time"

	"github.com/minaevmike/godis/codec"
	"github.com/minaevmike/godis/godis_proto"
	"github.com/minaevmike/godis/storage"
	"github.com/minaevmike/godis/wal"
	"github.com/minaevmike/godis/wire"
	"go.uber.org/zap"
)

func ListenAndServe(addr string) error {
	log, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("can't create logger: %v", err)
		os.Exit(1)
	}
	s := NewServer(log)
	return s.Run(addr)
}

func NewServer(
	logger *zap.Logger,
) *Server {
	st := storage.NewShardMapStorage(32)
	cd := codec.NewProtoCodec()
	return &Server{
		log:          logger,
		wireProtocol: wire.NewSimpleWireProtocol(cd),
		stopChan:     make(chan struct{}),
		storage:      st,
		wal: wal.NewIntervalWAL("./godis.wal", time.Second, logger, func(record *wal.Record) {
			v := &godis_proto.Value{}
			err := cd.Unmarshal(record.Value, v)
			if err != nil {
				logger.Error("can't unmarshal data from wal", zap.Error(err))
				return
			}
			switch record.Cmd {
			case wal.Write:
				err = st.Set(string(record.Key), v)
				if err != nil {
					logger.Error("can't set value from wal", zap.Error(err))
					return
				}
			case wal.Delete:
				err = st.Delete(string(record.Key))
				if err != nil {
					logger.Error("can't delete value from wal", zap.Error(err))
					return
				}
			}
		}),
		cd: cd,
	}
}

type Server struct {
	log          *zap.Logger
	wireProtocol wire.Protocol
	stopChan     chan struct{}
	storage      storage.Storage
	wal          wal.WAL
	cd           codec.Codec
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
			if errorPermament(err) {
				return err
			}
			s.log.Error("error accepting", zap.Error(err))
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) marshal(v *godis_proto.Value) []byte {
	if v == nil {
		return nil
	}
	data, err := s.cd.Marshal(v)
	if err != nil {
		s.log.Error("can't marshal data", zap.Error(err))
		return []byte(err.Error())
	}
	return data
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
				s.wireProtocol.Write(conn, getErrorResponse(err.Error()))
				continue
			}
			s.wireProtocol.Write(conn, &godis_proto.Response{ResponseValue: &godis_proto.Response_Value{
				Value: v,
			}})

		case godis_proto.Operation_Set:
			err := s.storage.Set(req.GetKey(), req.GetValue())
			if err != nil {
				s.wireProtocol.Write(conn, getErrorResponse(err.Error()))
				continue
			}
			err = s.wal.Write(wal.Write, []byte(req.GetKey()), s.marshal(req.GetValue()))
			if err != nil {
				s.log.Error("can't write to wal", zap.Error(err))
			}
			s.wireProtocol.Write(conn, &godis_proto.Response{})

		case godis_proto.Operation_Remove:
			err := s.storage.Delete(req.GetKey())
			if err != nil {
				s.wireProtocol.Write(conn, getErrorResponse(err.Error()))
				continue
			}
			err = s.wal.Write(wal.Delete, []byte(req.GetKey()), s.marshal(req.GetValue()))
			if err != nil {
				s.log.Error("can't write to wal", zap.Error(err))
			}
			s.wireProtocol.Write(conn, &godis_proto.Response{})

		case godis_proto.Operation_Keys:
			keyReg, err := regexp.Compile(req.GetKey())
			if err != nil {
				s.wireProtocol.Write(conn, getErrorResponse(err.Error()))
				continue
			}

			result := &syncStringSlice{}

			s.storage.ForEach(func(key string, _ *godis_proto.Value) {
				if keyReg.MatchString(key) {
					result.add(key)
				}
			})

			s.wireProtocol.Write(conn, &godis_proto.Response{
				ResponseValue: &godis_proto.Response_Value{
					Value: &godis_proto.Value{
						Value: &godis_proto.Value_StringSlice{
							StringSlice: &godis_proto.RepeatedString{
								StringArrayVal: result.data,
							},
						},
					},
				},
			})

		case godis_proto.Operation_GetByIndex:
			v, err := s.storage.Get(req.GetKey())
			if err != nil {
				s.wireProtocol.Write(conn, getErrorResponse(err.Error()))
				continue
			}
			switch t := v.GetValue().(type) {
			case *godis_proto.Value_StringSlice:
				arr := t.StringSlice.GetStringArrayVal()
				if int(req.GetIndex()) > len(arr) {
					s.wireProtocol.Write(conn, getErrorResponse("index out of range"))
					continue
				}
				s.wireProtocol.Write(conn, &godis_proto.Response{ResponseValue: &godis_proto.Response_Value{
					Value: &godis_proto.Value{
						Value: &godis_proto.Value_StringVal{
							StringVal: arr[int(req.GetIndex())],
						},
						Ttl: v.GetTtl(),
					},
				}})
			default:
				s.wireProtocol.Write(conn, getErrorResponse(fmt.Sprintf("bad key type: %T", t)))
				continue
			}
		case godis_proto.Operation_GetByKey:
			v, err := s.storage.Get(req.GetKey())
			if err != nil {
				s.wireProtocol.Write(conn, getErrorResponse(err.Error()))
				continue
			}
			switch t := v.GetValue().(type) {
			case *godis_proto.Value_StringMap:
				m := t.StringMap.GetStringMap()
				val, ok := m[req.GetMapKey()]
				if !ok {
					s.wireProtocol.Write(conn, getErrorResponse(fmt.Sprintf("key `%s` doesn't exists", req.GetMapKey())))
					continue
				}
				s.wireProtocol.Write(conn, &godis_proto.Response{ResponseValue: &godis_proto.Response_Value{
					Value: &godis_proto.Value{
						Value: &godis_proto.Value_StringVal{
							StringVal: val,
						},
						Ttl: v.GetTtl(),
					},
				}})
			default:
				s.wireProtocol.Write(conn, getErrorResponse(fmt.Sprintf("bad key type: %T", t)))
				continue
			}

		default:
			s.wireProtocol.Write(conn, getErrorResponse("not implemented"))
		}

	}
}

func getErrorResponse(err string) *godis_proto.Response {
	return &godis_proto.Response{
		ResponseValue: &godis_proto.Response_Error{
			Error: &godis_proto.Error{Message: err},
		},
	}
}
