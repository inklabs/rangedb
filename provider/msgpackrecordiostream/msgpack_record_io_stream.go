package msgpackrecordiostream

import (
	"io"
	"reflect"

	"github.com/vmihailenco/msgpack"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/msgpackrecordserializer"
)

type msgpackRecordIoStream struct {
	eventTypes map[string]reflect.Type
}

func New() *msgpackRecordIoStream {
	return &msgpackRecordIoStream{
		eventTypes: map[string]reflect.Type{},
	}
}

func (s *msgpackRecordIoStream) Bind(events ...rangedb.Event) {
	for _, e := range events {
		s.eventTypes[e.EventType()] = getType(e)
	}
}

func (s *msgpackRecordIoStream) Write(writer io.Writer, records <-chan *rangedb.Record) <-chan error {
	errors := make(chan error)

	go func() {
		defer close(errors)

		for record := range records {
			serializedRecord, err := msgpackrecordserializer.MarshalRecord(record)
			if err != nil {
				errors <- err
				return
			}
			_, _ = writer.Write(serializedRecord)
		}
	}()

	return errors
}

func (s *msgpackRecordIoStream) Read(reader io.Reader) (<-chan *rangedb.Record, <-chan error) {
	ch := make(chan *rangedb.Record)
	errors := make(chan error)

	go func() {
		defer close(ch)
		defer close(errors)

		decoder := msgpack.NewDecoder(reader)
		decoder.UseJSONTag(true)

		for true {
			record, err := msgpackrecordserializer.UnmarshalRecord(decoder, s.eventTypeLookup)
			if err != nil {
				if err == msgpackrecordserializer.EOF {
					return
				}

				errors <- err
				return
			}

			ch <- record
		}
	}()

	return ch, errors
}

func (s *msgpackRecordIoStream) eventTypeLookup(eventTypeName string) (r reflect.Type, b bool) {
	eventType, ok := s.eventTypes[eventTypeName]
	return eventType, ok
}

func getType(object interface{}) reflect.Type {
	t := reflect.TypeOf(object)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}
