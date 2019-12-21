package ndjsonrecordiostream

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

type ndJsonIoStream struct {
	eventTypes map[string]reflect.Type
}

func New() *ndJsonIoStream {
	return &ndJsonIoStream{
		eventTypes: map[string]reflect.Type{},
	}
}

func (s *ndJsonIoStream) Write(writer io.Writer, records <-chan *rangedb.Record) <-chan error {
	errors := make(chan error)
	go func() {
		defer close(errors)

		totalSaved := 0
		for record := range records {
			if totalSaved > 0 {
				_, _ = fmt.Fprint(writer, "\n")
			}

			data, err := json.Marshal(record)
			if err != nil {
				errors <- fmt.Errorf("failed marshalling event: %v", err)
			}

			_, _ = fmt.Fprintf(writer, "%s", data)

			totalSaved++
		}
	}()

	return errors
}

func (s *ndJsonIoStream) Read(reader io.Reader) (<-chan *rangedb.Record, <-chan error) {
	ch := make(chan *rangedb.Record)
	errors := make(chan error)

	go func() {
		defer close(ch)
		defer close(errors)

		decoder := json.NewDecoder(reader)
		decoder.UseNumber()

		for decoder.More() {
			record, err := jsonrecordserializer.UnmarshalRecord(decoder, s.eventTypeLookup)
			if err != nil {
				errors <- err
				return
			}

			ch <- record
		}
	}()

	return ch, errors
}

func (s *ndJsonIoStream) Bind(events ...rangedb.Event) {
	for _, e := range events {
		s.eventTypes[e.EventType()] = getType(e)
	}
}

func (s *ndJsonIoStream) eventTypeLookup(eventTypeName string) (r reflect.Type, b bool) {
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
