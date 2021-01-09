package rangedb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Version for RangeDB.
const Version = "0.6.0-dev"

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

// EventRecord stores the event and metadata to be used for persisting.
type EventRecord struct {
	Event    Event
	Metadata interface{}
}

// EventBinder defines how to bind events for serialization.
type EventBinder interface {
	Bind(events ...Event)
}

// Store is the interface that stores and retrieves event records.
type Store interface {
	EventBinder
	Events(ctx context.Context, globalSequenceNumber uint64) RecordIterator
	EventsByAggregateTypes(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) RecordIterator
	EventsByStream(ctx context.Context, streamSequenceNumber uint64, streamName string) RecordIterator
	OptimisticSave(ctx context.Context, expectedStreamSequenceNumber uint64, eventRecords ...*EventRecord) (uint64, error)
	Save(ctx context.Context, eventRecords ...*EventRecord) (uint64, error)
	Subscribe(ctx context.Context, subscribers ...RecordSubscriber) error
	TotalEventsInStream(ctx context.Context, streamName string) (uint64, error)
}

// ResultRecord combines Record and error as a result struct for event queries.
type ResultRecord struct {
	Record *Record
	Err    error
}

// RecordIterator is used to traverse a stream of record events.
type RecordIterator interface {
	Next() bool
	Record() *Record
	Err() error
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

// Accept receives a record.
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

// ReadNRecords reads up to N records from the channel returned by f into a slice
func ReadNRecords(totalEvents uint64, f func() (RecordIterator, context.CancelFunc)) []*Record {
	var records []*Record
	cnt := uint64(0)
	recordIterator, done := f()

	for recordIterator.Next() {
		if recordIterator.Err() != nil {
			break
		}
		cnt++
		if cnt > totalEvents {
			break
		}

		records = append(records, recordIterator.Record())
	}

	done()
	for recordIterator.Next() {
	}

	return records
}

type rawEvent struct {
	aggregateType string
	aggregateID   string
	eventType     string
	data          interface{}
}

// NewRawEvent constructs a new raw event when an event struct is unavailable or unknown.
func NewRawEvent(aggregateType, aggregateID, eventType string, data interface{}) Event {
	return &rawEvent{
		aggregateType: aggregateType,
		aggregateID:   aggregateID,
		eventType:     eventType,
		data:          data,
	}
}

func (e rawEvent) AggregateID() string {
	return e.aggregateID
}

func (e rawEvent) AggregateType() string {
	return e.aggregateType
}

func (e rawEvent) EventType() string {
	return e.eventType
}

func (e rawEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.data)
}
