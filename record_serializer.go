package rangedb

import (
	"reflect"
)

// RecordSerializer is the interface that (de)serializes Records.
type RecordSerializer interface {
	Serialize(record *Record) ([]byte, error)
	Deserialize(data []byte) (*Record, error)
	Bind(events ...Event)
}

// EventTypeIdentifier is the interface for retrieving an event type.
type EventTypeIdentifier interface {
	EventTypeLookup(eventTypeName string) (reflect.Type, bool)
}
