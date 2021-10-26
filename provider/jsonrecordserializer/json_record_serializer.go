package jsonrecordserializer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/inklabs/rangedb"
)

type jsonSerializer struct {
	eventIdentifier rangedb.EventTypeIdentifier
}

// New constructs a jsonSerializer.
func New() *jsonSerializer {
	return &jsonSerializer{
		eventIdentifier: rangedb.NewEventIdentifier(),
	}
}

func (s *jsonSerializer) Bind(events ...rangedb.Event) {
	s.eventIdentifier.Bind(events...)
}

func (s *jsonSerializer) Serialize(record *rangedb.Record) ([]byte, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("failed marshalling record: %v", err)
	}

	return data, nil
}

func (s *jsonSerializer) Deserialize(serializedData []byte) (*rangedb.Record, error) {
	decoder := json.NewDecoder(bytes.NewReader(serializedData))
	decoder.UseNumber()

	return UnmarshalRecord(decoder, s.eventIdentifier)
}

func (s *jsonSerializer) EventTypeLookup(eventTypeName string) (reflect.Type, bool) {
	return s.eventIdentifier.EventTypeLookup(eventTypeName)
}

// UnmarshalRecord decodes a Record using the supplied JSON decoder.
//
// Event data will be parsed into a struct if supplied by getEventType.
func UnmarshalRecord(decoder *json.Decoder, eventTypeIdentifier rangedb.EventTypeIdentifier) (*rangedb.Record, error) {
	var rawEvent json.RawMessage
	record := rangedb.Record{
		Data: &rawEvent,
	}
	err := decoder.Decode(&record)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling record: %v", err)
	}

	data, err := DecodeJsonData(record.EventType, bytes.NewReader(rawEvent), eventTypeIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling event within record: %v", err)
	}

	record.Data = data

	return &record, nil
}

// DecodeJsonData decodes raw json into a struct or interface{}.
//
// Event data will be parsed into a struct if supplied by getEventType.
func DecodeJsonData(eventTypeName string, rawJsonData io.Reader, eventTypeIdentifier rangedb.EventTypeIdentifier) (interface{}, error) {
	dataDecoder := json.NewDecoder(rawJsonData)
	dataDecoder.UseNumber()

	eventType, ok := eventTypeIdentifier.EventTypeLookup(eventTypeName)
	if !ok {
		var data interface{}
		err := dataDecoder.Decode(&data)
		if err != nil {
			return nil, err
		}

		return data, nil
	}

	data := reflect.New(eventType).Interface()
	err := dataDecoder.Decode(&data)
	if err != nil {
		return nil, err
	}

	return data, nil
}
