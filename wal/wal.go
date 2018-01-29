package wal

type Command uint8

const (
	Write Command = iota
	Delete
)

type WAL interface {
	Write(cmd Command, key []byte, data []byte) error
}

type NoopWAL struct {
}

func (NoopWAL) Write(cmd Command, key []byte, data []byte) error {
	return nil
}
