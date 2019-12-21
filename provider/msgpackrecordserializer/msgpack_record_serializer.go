package msgpackrecordserializer

import (
	"bytes"
	"errors"
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

func (s *msgpackSerializer) Bind(events ...rangedb.Event) {
	for _, e := range events {
		s.eventTypes[e.EventType()] = getType(e)
	}
}

func (s *msgpackSerializer) Serialize(record *rangedb.Record) ([]byte, error) {
	return MarshalRecord(record)
}

func (s *msgpackSerializer) Deserialize(serializedData []byte) (*rangedb.Record, error) {
	decoder := msgpack.NewDecoder(bytes.NewBuffer(serializedData))
	decoder.UseJSONTag(true)

	return UnmarshalRecord(decoder, s.eventTypeLookup)
}

func (s *msgpackSerializer) eventTypeLookup(eventTypeName string) (r reflect.Type, b bool) {
	eventType, ok := s.eventTypes[eventTypeName]
	return eventType, ok
}

func MarshalRecord(record *rangedb.Record) ([]byte, error) {
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

func UnmarshalRecord(decoder *msgpack.Decoder, getEventType func(eventTypeName string) (reflect.Type, bool)) (*rangedb.Record, error) {
	record := rangedb.Record{}

	decodeErr := decoder.Decode(&record)
	if decodeErr != nil {
		var err error
		if decodeErr.Error() == "EOF" {
			err = EOF
		} else {
			err = fmt.Errorf("failed decoding record: %v", decodeErr)
		}

		return nil, err
	}

	eventType, ok := getEventType(record.EventType)
	if ok {
		data := reflect.New(eventType).Interface()
		err := decoder.Decode(data)
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

var EOF = errors.New("EOF")

func getType(object interface{}) reflect.Type {
	t := reflect.TypeOf(object)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}
