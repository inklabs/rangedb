package rangedb

import (
	"fmt"
	"github.com/inklabs/rangedb/pkg/paging"
)

const Version = "0.2.0"

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
	AllEventsByAggregateType(aggregateType string) <-chan *Record
	AllEventsByAggregateTypes(aggregateTypes ...string) <-chan *Record
	AllEventsByStream(stream string) <-chan *Record
	EventsByAggregateType(pagination paging.Pagination, aggregateType string) <-chan *Record
	EventsByAggregateTypeStartingWith(aggregateType string, eventNumber uint64) <-chan *Record
	EventsByStream(pagination paging.Pagination, streamName string) <-chan *Record
	EventsByStreamStartingWith(streamName string, eventNumber uint64) <-chan *Record
	Save(event Event, metadata interface{}) error
	SaveEvent(aggregateType, aggregateID, eventType, eventID string, event, metadata interface{}) error
	Subscribe(subscribers ...RecordSubscriber)
	SubscribeAndReplay(subscribers ...RecordSubscriber)
	TotalEventsInStream(streamName string) uint64
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

// GetAllEventsByAggregateTypes returns a slice of Record channels by aggregateType.
func GetAllEventsByAggregateTypes(store Store, aggregateTypes ...string) []<-chan *Record {
	var channels []<-chan *Record
	for _, aggregateType := range aggregateTypes {
		channels = append(channels, store.AllEventsByAggregateType(aggregateType))
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

// RecordChannelToSlice reads all records from the channel into a slice
func RecordChannelToSlice(records <-chan *Record) []*Record {
	var events []*Record

	for record := range records {
		events = append(events, record)
	}

	return events
}
