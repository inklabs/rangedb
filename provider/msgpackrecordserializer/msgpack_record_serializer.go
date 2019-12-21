package msgpackrecordserializer

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack"

	"github.com/inklabs/rangedb"
)

type msgpackSerializer struct {
	eventTypes map[string]reflect.Type
}

func New() *msgpackSerializer {
	return &msgpackSerializer{
		eventTypes: map[string]reflect.Type{},
	}
}

func (s *msgpackSerializer) Serialize(record *rangedb.Record) ([]byte, error) {
	var buf bytes.Buffer

	newRecord := *record
	newRecord.Data = nil

	encoder := msgpack.NewEncoder(&buf)
	encoder.UseJSONTag(true)

	err := encoder.Encode(newRecord)
	if err != nil {
		return nil, fmt.Errorf("failed encoding record: %v", err)
	}

	err = encoder.Encode(record.Data)
	if err != nil {
		return nil, fmt.Errorf("failed encoding record data: %v", err)
	}

	return buf.Bytes(), nil
}

func (s *msgpackSerializer) Deserialize(serializedData []byte) (*rangedb.Record, error) {
	record := rangedb.Record{}

	decoder := msgpack.NewDecoder(bytes.NewBuffer(serializedData))
	decoder.UseJSONTag(true)

	err := decoder.Decode(&record)
	if err != nil {
		return nil, fmt.Errorf("failed decoding record [%s]: %v", serializedData, err)
	}

	eventType, ok := s.eventTypes[record.EventType]
	if ok {
		data := reflect.New(eventType).Interface()
		err = decoder.Decode(data)
		if err != nil {
			return nil, fmt.Errorf("failed decoding event after record: %v", err)
		}

		record.Data = data
	} else {
		var data interface{}
		err := decoder.Decode(&data)
		if err != nil {
			return nil, fmt.Errorf("failed decoding event after record: %v", err)
		}

		record.Data = data
	}

	return &record, nil
}

func (s *msgpackSerializer) Bind(events ...rangedb.Event) {
	for _, e := range events {
		s.eventTypes[e.EventType()] = getType(e)
	}
}

func getType(object interface{}) reflect.Type {
	t := reflect.TypeOf(object)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}
