package rangedb

import (
	"fmt"
)

type Record struct {
	AggregateType        string      `msgpack:"a" json:"aggregateType"`
	AggregateId          string      `msgpack:"i" json:"aggregateId"`
	GlobalSequenceNumber uint64      `msgpack:"g" json:"globalSequenceNumber"`
	StreamSequenceNumber uint64      `msgpack:"s" json:"sequenceNumber"`
	InsertTimestamp      uint64      `msgpack:"u" json:"insertTimestamp"`
	EventId              string      `msgpack:"e" json:"eventId"`
	EventType            string      `msgpack:"t" json:"eventType"`
	Data                 interface{} `msgpack:"d" json:"data"`
	Metadata             interface{} `msgpack:"m" json:"metadata"`
}

type Store interface {
	AllEvents() <-chan *Record
	EventsByStream(stream string) <-chan *Record
	EventsByStreamStartingWith(stream string, eventNumber uint64) <-chan *Record
	EventsByAggregateType(aggregateType string) <-chan *Record
	EventsByAggregateTypes(aggregateTypes ...string) <-chan *Record
	EventsByAggregateTypeStartingWith(aggregateType string, eventNumber uint64) <-chan *Record
	Save(event Event, metadata interface{}) error
	SaveEvent(aggregateType, aggregateId, eventType, eventId string, event, metadata interface{}) error
	Subscribe(subscribers ...RecordSubscriber)
}

type Event interface {
	AggregateMessage
	EventType() string
}

type AggregateMessage interface {
	AggregateId() string
	AggregateType() string
}

type RecordSubscriber interface {
	Accept(record *Record)
}

func GetEventStream(message AggregateMessage) string {
	return GetStream(message.AggregateType(), message.AggregateId())
}

func GetStream(aggregateType, aggregateId string) string {
	return fmt.Sprintf("%s!%s", aggregateType, aggregateId)
}

func GetEventsByAggregateTypes(store Store, aggregateTypes ...string) []<-chan *Record {
	var channels []<-chan *Record
	for _, aggregateType := range aggregateTypes {
		channels = append(channels, store.EventsByAggregateType(aggregateType))
	}
	return channels
}
