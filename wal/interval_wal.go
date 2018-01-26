package wal

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
)

type walRecord struct {
	cmd   Command
	key   []byte
	value []byte
}

// this is simple binary serialization format
// KLKLKLKLVLVLVLVLCCCCK....KV....V
// |..key len 8 byte.||..value len 8 byte..||..command 1 byte ...||..key..||..value..|
//
func (r *walRecord) WriteTo(w io.Writer) (int64, error) {
	total := int64(0)
	// key len
	err := binary.Write(w, binary.BigEndian, int64(len(r.key)))
	if err != nil {
		return total, err
	}
	total += 8
	// value len
	err = binary.Write(w, binary.BigEndian, int64(len(r.value)))
	if err != nil {
		return total, err
	}
	total += 8

	//cmd
	err = binary.Write(w, binary.BigEndian, r.cmd)
	if err != nil {
		return total, err
	}
	total += 1

	// key
	n, err := w.Write(r.key)
	if err != nil {
		return total, err
	}
	total += int64(n)

	// value
	n, err = w.Write(r.value)
	if err != nil {
		return total, err
	}
	total += int64(n)
	return total, nil
}

func (r *walRecord) ReadFrom(rr io.Reader) (int64, error) {
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

	err = binary.Read(rr, binary.BigEndian, &r.cmd)
	if err != nil {
		return total, err
	}

	total += 1

	r.key = make([]byte, keyLen)

	n, err := io.ReadFull(rr, r.key)
	if err != nil {
		return total, err
	}

	total += int64(n)

	r.value = make([]byte, valueLen)
	n, err = io.ReadFull(rr, r.value)
	if err != nil {
		return total, err
	}

	total += int64(n)
	return total, nil
}

type syncedWALRecords struct {
	mu      sync.Mutex
	records []*walRecord
}

func (wr *syncedWALRecords) Add(record *walRecord) {
	wr.mu.Lock()
	wr.records = append(wr.records, record)
	wr.mu.Unlock()
}

func (wr *syncedWALRecords) Swap() []*walRecord {
	wr.mu.Lock()
	old := wr.records
	wr.records = make([]*walRecord, 0)
	wr.mu.Unlock()
	return old
}

func NewIntervalWAL(file string, timeout time.Duration, logger *zap.Logger) Wal {
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
			c := &walRecord{}
			_, err := c.ReadFrom(walFile)
			if err != nil {
				if err == io.EOF {
					break
				}
				logger.Error("can't read data", zap.Error(err))
				return nil
			}
			logger.Debug("read wal record", zap.String("key", string(c.key)))
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
	iw.syncedWALRecords.Add(&walRecord{
		cmd:   cmd,
		key:   key,
		value: data,
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
