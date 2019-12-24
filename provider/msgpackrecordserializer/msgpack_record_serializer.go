package msgpackrecordserializer

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v4"

	"github.com/inklabs/rangedb"
)

type msgpackSerializer struct {
	eventTypes map[string]reflect.Type
}

// New constructs a msgpackSerializer.
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

// MarshalRecord encodes a Record as msgpack.
//
// The record, excluding data, is encoded first. Then, event data is encoded.
// Encoding the event data second allows decoding to parse into a struct if defined.
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

// UnmarshalRecord decodes a Record using the supplied msgpack decoder.
//
// The record, excluding data, is decoded first. Then, event data is decoded.
// Event data will be parsed into a struct if supplied by getEventType.
func UnmarshalRecord(decoder *msgpack.Decoder, getEventType func(eventTypeName string) (reflect.Type, bool)) (*rangedb.Record, error) {
	record := rangedb.Record{}

	decodeErr := decoder.Decode(&record)
	if decodeErr != nil {
		var err error
		if decodeErr.Error() == "EOF" {
			err = ErrorEOF
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

// ErrorEOF defines an end of file error.
var ErrorEOF = errors.New("EOF")

func getType(object interface{}) reflect.Type {
	t := reflect.TypeOf(object)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}
