package rangedb

import (
	"context"
	"fmt"
	"strings"
)

const Version = "0.4.0-dev"

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

type EventBinder interface {
	Bind(events ...Event)
}

// Store is the interface that stores and retrieves event records.
type Store interface {
	EventBinder
	EventsStartingWith(ctx context.Context, eventNumber uint64) <-chan *Record
	EventsByAggregateTypesStartingWith(ctx context.Context, eventNumber uint64, aggregateTypes ...string) <-chan *Record
	EventsByStreamStartingWith(ctx context.Context, eventNumber uint64, streamName string) <-chan *Record
	Save(event Event, metadata interface{}) error
	SaveEvent(aggregateType, aggregateID, eventType, eventID string, event, metadata interface{}) error
	Subscribe(subscribers ...RecordSubscriber)
	SubscribeStartingWith(ctx context.Context, eventNumber uint64, subscribers ...RecordSubscriber)
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

// The RecordSubscriberFunc type is an adapter to allow the use of
// ordinary functions as record subscribers. If f is a function
// with the appropriate signature, RecordSubscriberFunc(f) is a
// Handler that calls f.
type RecordSubscriberFunc func(*Record)

func (f RecordSubscriberFunc) Accept(record *Record) {
	f(record)
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

// ParseStream returns the aggregateType and aggregateID for a stream name.
func ParseStream(streamName string) (aggregateType, aggregateID string) {
	pieces := strings.Split(streamName, "!")
	return pieces[0], pieces[1]
}

// ReplayEvents applies all events to each subscriber.
func ReplayEvents(store Store, eventNumber uint64, subscribers ...RecordSubscriber) {
	for record := range store.EventsStartingWith(context.Background(), eventNumber) {
		for _, subscriber := range subscribers {
			subscriber.Accept(record)
		}
	}
}

// ReadNRecords reads up to N records from the channel returned by f into a slice
func ReadNRecords(totalEvents uint64, f func(context.Context) <-chan *Record) []*Record {
	var records []*Record
	ctx, done := context.WithCancel(context.Background())
	cnt := uint64(0)
	for record := range f(ctx) {
		cnt++
		if cnt > totalEvents {
			break
		}

		records = append(records, record)
	}
	done()

	return records
}
