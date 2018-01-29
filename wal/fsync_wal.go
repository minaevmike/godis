package wal

import (
	"io"
	"os"
	"sync"

	"go.uber.org/zap"
)

type fsyncWal struct {
	f      *os.File
	mu     sync.Locker
	logger *zap.Logger
}

func (fw *fsyncWal) Write(cmd Command, key []byte, data []byte) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	r := &Record{Value: data, Cmd: cmd, Key: key}
	_, err := r.WriteTo(fw.f)
	return err
}

func NewFsyncWAL(file string, logger *zap.Logger, cb func(record *Record)) WAL {
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

	w := &fsyncWal{
		logger: logger,
		f:      walFile,
		mu:     &sync.Mutex{},
	}

	return w
}
