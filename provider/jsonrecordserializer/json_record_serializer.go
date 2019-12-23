package jsonrecordserializer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/inklabs/rangedb"
)

type jsonSerializer struct {
	eventTypes map[string]reflect.Type
}

func New() *jsonSerializer {
	return &jsonSerializer{
		eventTypes: map[string]reflect.Type{},
	}
}

func (s *jsonSerializer) Bind(events ...rangedb.Event) {
	for _, e := range events {
		s.eventTypes[e.EventType()] = getType(e)
	}
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

	return UnmarshalRecord(decoder, s.eventTypeLookup)
}

func (s *jsonSerializer) eventTypeLookup(eventTypeName string) (r reflect.Type, b bool) {
	eventType, ok := s.eventTypes[eventTypeName]
	return eventType, ok
}

func UnmarshalRecord(decoder *json.Decoder, getEventType func(eventTypeName string) (reflect.Type, bool)) (*rangedb.Record, error) {
	var rawEvent json.RawMessage
	record := rangedb.Record{
		Data: &rawEvent,
	}
	err := decoder.Decode(&record)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling record: %v", err)
	}

	dataDecoder := json.NewDecoder(bytes.NewReader(rawEvent))
	dataDecoder.UseNumber()

	eventType, ok := getEventType(record.EventType)
	if ok {

		data := reflect.New(eventType).Interface()
		err = dataDecoder.Decode(&data)
		if err != nil {
			return nil, fmt.Errorf("failed unmarshalling event within record: %v", err)
		}

		record.Data = data
	} else {
		var data interface{}
		err = dataDecoder.Decode(&data)
		if err != nil {
			return nil, fmt.Errorf("failed unmarshalling event within record: %v", err)
		}

		record.Data = data
	}

	return &record, nil
}

func getType(object interface{}) reflect.Type {
	t := reflect.TypeOf(object)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}
