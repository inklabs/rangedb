package rangedb

import (
	"io"
)

// RecordIoStream is the interface that (de)serializes a stream of Records.
type RecordIoStream interface {
	Read(io.Reader) (<-chan *Record, <-chan error)
	Write(io.Writer, <-chan *Record) <-chan error
	Bind(events ...Event)
}
