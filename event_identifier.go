package rangedb

import (
	"reflect"
)

type eventIdentifier struct {
	eventTypes map[string]reflect.Type
}

// NewEventIdentifier constructs an eventIdentifier.
func NewEventIdentifier() *eventIdentifier {
	return &eventIdentifier{
		eventTypes: map[string]reflect.Type{},
	}
}

func (s *eventIdentifier) Bind(events ...Event) {
	for _, e := range events {
		s.eventTypes[e.EventType()] = getType(e)
	}
}

func (s *eventIdentifier) EventTypeLookup(eventTypeName string) (r reflect.Type, b bool) {
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
