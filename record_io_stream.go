package rangedb

import (
	"io"
)

type RecordIoStream interface {
	Read(io.Reader) (<-chan *Record, <-chan error)
	Write(io.Writer, <-chan *Record) <-chan error
	Bind(events ...Event)
}
