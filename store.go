package rangedb

import (
	"fmt"
)

// Record contains event data and metadata.
type Record struct {
	AggregateType        string      `msgpack:"a" json:"aggregateType"`
	AggregateID          string      `msgpack:"i" json:"aggregateID"`
	GlobalSequenceNumber uint64      `msgpack:"g" json:"globalSequenceNumber"`
	StreamSequenceNumber uint64      `msgpack:"s" json:"sequenceNumber"`
	InsertTimestamp      uint64      `msgpack:"u" json:"insertTimestamp"`
	EventID              string      `msgpack:"e" json:"eventID"`
	EventType            string      `msgpack:"t" json:"eventType"`
	Data                 interface{} `msgpack:"d" json:"data"`
	Metadata             interface{} `msgpack:"m" json:"metadata"`
}

// Store is the interface that stores and retrieves event records.
type Store interface {
	AllEvents() <-chan *Record
	EventsByStream(stream string) <-chan *Record
	EventsByStreamStartingWith(stream string, eventNumber uint64) <-chan *Record
	EventsByAggregateType(aggregateType string) <-chan *Record
	EventsByAggregateTypes(aggregateTypes ...string) <-chan *Record
	EventsByAggregateTypeStartingWith(aggregateType string, eventNumber uint64) <-chan *Record
	Save(event Event, metadata interface{}) error
	SaveEvent(aggregateType, aggregateID, eventType, eventID string, event, metadata interface{}) error
	Subscribe(subscribers ...RecordSubscriber)
	SubscribeAndReplay(subscribers ...RecordSubscriber)
}

// Event is the interface that defines the required event methods.
type Event interface {
	AggregateMessage
	EventType() string
}

// AggregateMessage is the interface that supports building an event stream name.
type AggregateMessage interface {
	AggregateID() string
	AggregateType() string
}

// RecordSubscriber is the interface that defines how a projection receives Records.
type RecordSubscriber interface {
	Accept(record *Record)
}

// GetEventStream returns the stream name for an event.
func GetEventStream(message AggregateMessage) string {
	return GetStream(message.AggregateType(), message.AggregateID())
}

// GetStream returns the stream name for an aggregateType and aggregateID.
func GetStream(aggregateType, aggregateID string) string {
	return fmt.Sprintf("%s!%s", aggregateType, aggregateID)
}

// GetEventsByAggregateTypes returns a slice of Record channels by aggregateType.
func GetEventsByAggregateTypes(store Store, aggregateTypes ...string) []<-chan *Record {
	var channels []<-chan *Record
	for _, aggregateType := range aggregateTypes {
		channels = append(channels, store.EventsByAggregateType(aggregateType))
	}
	return channels
}

// ReplayEvents applies all events to each subscriber.
func ReplayEvents(store Store, subscribers ...RecordSubscriber) {
	for record := range store.AllEvents() {
		for _, subscriber := range subscribers {
			subscriber.Accept(record)
		}
	}
}
