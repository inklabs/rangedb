package jsonrecordserializer

import (
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
	var rawEvent json.RawMessage
	record := rangedb.Record{
		Data: &rawEvent,
	}
	err := json.Unmarshal(serializedData, &record)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling record [%s]: %v", serializedData, err)
	}

	eventType, ok := s.eventTypes[record.EventType]
	if ok {
		data := reflect.New(eventType).Interface()
		err = json.Unmarshal(rawEvent, data)
		if err != nil {
			return nil, fmt.Errorf("failed unmarshalling event within record: %v", err)
		}

		record.Data = data
	} else {
		var data interface{}
		err = json.Unmarshal(rawEvent, &data)
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
