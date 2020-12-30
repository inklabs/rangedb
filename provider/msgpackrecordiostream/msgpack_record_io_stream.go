package msgpackrecordiostream

import (
	"io"
	"reflect"

	"github.com/vmihailenco/msgpack/v4"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/msgpackrecordserializer"
)

type msgpackRecordIoStream struct {
	eventTypes map[string]reflect.Type
}

// New constructs a msgpackRecordIoStream.
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

func (s *msgpackRecordIoStream) Write(writer io.Writer, recordIterator rangedb.RecordIterator) <-chan error {
	errors := make(chan error)

	go func() {
		defer close(errors)

		for recordIterator.Next() {
			if recordIterator.Err() != nil {
				errors <- recordIterator.Err()
				return
			}

			serializedRecord, err := msgpackrecordserializer.MarshalRecord(recordIterator.Record())
			if err != nil {
				errors <- err
				return
			}

			_, _ = writer.Write(serializedRecord)
		}
	}()

	return errors
}

func (s *msgpackRecordIoStream) Read(reader io.Reader) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		decoder := msgpack.NewDecoder(reader)
		decoder.UseJSONTag(true)

		for {
			record, err := msgpackrecordserializer.UnmarshalRecord(decoder, s.eventTypeLookup)
			if err != nil {
				if err == msgpackrecordserializer.ErrorEOF {
					return
				}

				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    err,
				}
				return
			}

			// TODO: Add cancel context to avoid deadlock
			resultRecords <- rangedb.ResultRecord{
				Record: record,
				Err:    nil,
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
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
