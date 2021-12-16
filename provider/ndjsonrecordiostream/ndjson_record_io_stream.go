package ndjsonrecordiostream

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

type ndJSONRecordIoStream struct {
	eventIdentifier rangedb.EventTypeIdentifier
}

// New constructs an ndjson implementation of rangedb.RecordIoStream.
func New() *ndJSONRecordIoStream {
	return &ndJSONRecordIoStream{
		eventIdentifier: rangedb.NewEventIdentifier(),
	}
}

func (s *ndJSONRecordIoStream) Write(writer io.Writer, recordIterator rangedb.RecordIterator) <-chan error {
	errors := make(chan error)

	go func() {
		defer close(errors)

		totalSaved := 0
		for recordIterator.Next() {
			if recordIterator.Err() != nil {
				errors <- recordIterator.Err()
				return
			}

			if totalSaved > 0 {
				_, _ = fmt.Fprint(writer, "\n")
			}

			data, err := json.Marshal(recordIterator.Record())
			if err != nil {
				errors <- fmt.Errorf("failed marshalling event: %v", err)
				return
			}

			_, _ = fmt.Fprintf(writer, "%s", data)

			totalSaved++
		}
	}()

	return errors
}

func (s *ndJSONRecordIoStream) Read(reader io.Reader) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		decoder := json.NewDecoder(reader)
		decoder.UseNumber()

		for decoder.More() {
			record, err := jsonrecordserializer.UnmarshalRecord(decoder, s)
			if err != nil {
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

func (s *ndJSONRecordIoStream) Bind(events ...rangedb.Event) {
	s.eventIdentifier.Bind(events...)
}

func (s *ndJSONRecordIoStream) EventTypeLookup(eventTypeName string) (r reflect.Type, b bool) {
	return s.eventIdentifier.EventTypeLookup(eventTypeName)
}
