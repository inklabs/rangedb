package rangedb

import (
	"io"
)

// RecordIoStream is the interface that (de)serializes a stream of Records.
type RecordIoStream interface {
	Read(io.Reader) RecordIterator
	Write(io.Writer, RecordIterator) <-chan error
	Bind(events ...Event)
}
