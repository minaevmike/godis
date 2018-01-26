package wal

type Command uint8

const (
	Write Command = iota
	Delete
)

type Wal interface {
	Write(cmd Command, key []byte, data []byte) error
}

type NoopWal struct {
}

func (NoopWal) Write(cmd Command, key []byte, data []byte) error {
	return nil
}
