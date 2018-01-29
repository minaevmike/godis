package wal

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
	"time"

	"bytes"

	"go.uber.org/zap"
)

type Record struct {
	Cmd   Command
	Key   []byte
	Value []byte
}

// this is simple binary serialization format
// KLKLKLKLVLVLVLVLCCCCK....KV....V
// |..Key len 8 byte.||..Value len 8 byte..||..command 1 byte ...||..Key..||..Value..|
//
func (r *Record) WriteTo(w io.Writer) (int64, error) {
	b := &bytes.Buffer{}
	// Key len
	err := binary.Write(b, binary.BigEndian, int64(len(r.Key)))
	if err != nil {
		return int64(b.Len()), err
	}
	// Value len
	err = binary.Write(b, binary.BigEndian, int64(len(r.Value)))
	if err != nil {
		return int64(b.Len()), err
	}

	//Cmd
	err = binary.Write(b, binary.BigEndian, r.Cmd)
	if err != nil {
		return int64(b.Len()), err
	}

	// Key
	_, err = b.Write(r.Key)
	if err != nil {
		return int64(b.Len()), err
	}

	// Value
	_, err = b.Write(r.Value)
	if err != nil {
		return int64(b.Len()), err
	}
	return io.Copy(w, b)
}

func (r *Record) ReadFrom(rr io.Reader) (int64, error) {
	total := int64(0)

	keyLen := int64(0)

	err := binary.Read(rr, binary.BigEndian, &keyLen)
	if err != nil {
		return total, err
	}

	total += 8

	valueLen := int64(0)

	err = binary.Read(rr, binary.BigEndian, &valueLen)
	if err != nil {
		return total, err
	}

	total += 8

	err = binary.Read(rr, binary.BigEndian, &r.Cmd)
	if err != nil {
		return total, err
	}

	total += 1

	r.Key = make([]byte, keyLen)

	n, err := io.ReadFull(rr, r.Key)
	if err != nil {
		return total, err
	}

	total += int64(n)

	r.Value = make([]byte, valueLen)
	n, err = io.ReadFull(rr, r.Value)
	if err != nil {
		return total, err
	}

	total += int64(n)
	return total, nil
}

type syncedWALRecords struct {
	mu      sync.Mutex
	records []*Record
}

func (wr *syncedWALRecords) Add(record *Record) {
	wr.mu.Lock()
	wr.records = append(wr.records, record)
	wr.mu.Unlock()
}

func (wr *syncedWALRecords) Swap() []*Record {
	wr.mu.Lock()
	old := wr.records
	wr.records = make([]*Record, 0)
	wr.mu.Unlock()
	return old
}

func NewIntervalWAL(file string, timeout time.Duration, logger *zap.Logger, cb func(record *Record)) WAL {
	var walFile *os.File
	if _, err := os.Stat(file); os.IsNotExist(err) {
		walFile, err = os.Create(file)
	} else {
		walFile, err = os.OpenFile(file, os.O_RDWR, 0644)
		if err != nil {
			logger.Error("can't create file", zap.Error(err))
			return nil
		}
		for {
			c := &Record{}
			_, err := c.ReadFrom(walFile)
			if err != nil {
				if err == io.EOF {
					break
				}
				logger.Error("can't read data", zap.Error(err))
				return nil
			}
			if cb != nil {
				cb(c)
			}
			logger.Debug("read wal record", zap.String("Key", string(c.Key)))
		}
	}

	w := &intervalWAL{
		logger:           logger,
		timeout:          timeout,
		w:                walFile,
		syncedWALRecords: &syncedWALRecords{},
	}

	go w.monitor()

	return w
}

type walWriter interface {
	io.Writer
	Sync() error
}

type intervalWAL struct {
	w                walWriter
	timeout          time.Duration
	syncedWALRecords *syncedWALRecords
	logger           *zap.Logger
}

func (iw *intervalWAL) Write(cmd Command, key []byte, data []byte) error {
	iw.syncedWALRecords.Add(&Record{
		Cmd:   cmd,
		Key:   key,
		Value: data,
	})
	return nil
}

func (iw *intervalWAL) monitor() {
	t := time.NewTicker(iw.timeout)

	select {
	case <-t.C:
		data := iw.syncedWALRecords.Swap()
		for i := range data {
			_, err := data[i].WriteTo(iw.w)
			if err != nil {
				iw.logger.Error("can't write record", zap.Error(err))
			}
		}
		iw.w.Sync()
	}
}
