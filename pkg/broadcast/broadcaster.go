package broadcast

import (
	"time"

	"github.com/inklabs/rangedb"
)

// DefaultTimeout for broadcasting records to subscribers.
const DefaultTimeout = time.Millisecond * 100

// SendRecordChan is a write only channel for rangedb.Record.
type SendRecordChan chan<- *rangedb.Record

// RecordSubscriber defines how a rangedb.Record is received.
type RecordSubscriber interface {
	Receiver() SendRecordChan
	Stop()
}

// AggregateTypeSubscription defines an aggregate type subscriber.
type AggregateTypeSubscription struct {
	AggregateType string
	Subscriber    RecordSubscriber
}

// Broadcaster defines a record broadcaster.
type Broadcaster interface {
	Accept(record *rangedb.Record)
	SubscribeAllEvents(subscribers ...RecordSubscriber)
	UnsubscribeAllEvents(subscribers ...RecordSubscriber)
	SubscribeAggregateTypes(subscriber RecordSubscriber, aggregateTypes ...string)
	UnsubscribeAggregateTypes(subscriber RecordSubscriber, aggregateTypes ...string)
	Close()
}

type broadcaster struct {
	subscribeAllEvents        chan RecordSubscriber
	unsubscribeAllEvents      chan RecordSubscriber
	subscribeAggregateTypes   chan AggregateTypeSubscription
	unsubscribeAggregateTypes chan AggregateTypeSubscription
	bufferedRecords           chan *rangedb.Record
	stopChan                  chan struct{}
	allEventsSubscribers      map[RecordSubscriber]struct{}
	aggregateTypeSubscribers  map[string]map[RecordSubscriber]struct{}
	timeout                   time.Duration
}

// New constructs a broadcaster.
func New(bufferSize int, timeout time.Duration) *broadcaster {
	broadcaster := &broadcaster{
		subscribeAllEvents:        make(chan RecordSubscriber),
		unsubscribeAllEvents:      make(chan RecordSubscriber),
		subscribeAggregateTypes:   make(chan AggregateTypeSubscription),
		unsubscribeAggregateTypes: make(chan AggregateTypeSubscription),
		bufferedRecords:           make(chan *rangedb.Record, bufferSize),
		stopChan:                  make(chan struct{}),
		allEventsSubscribers:      make(map[RecordSubscriber]struct{}),
		aggregateTypeSubscribers:  make(map[string]map[RecordSubscriber]struct{}),
		timeout:                   timeout,
	}

	go broadcaster.start()

	return broadcaster
}

func (b *broadcaster) Accept(record *rangedb.Record) {
	b.bufferedRecords <- record
}

func (b *broadcaster) SubscribeAllEvents(subscribers ...RecordSubscriber) {
	for _, subscriber := range subscribers {
		b.subscribeAllEvents <- subscriber
	}
}
func (b *broadcaster) UnsubscribeAllEvents(subscribers ...RecordSubscriber) {
	for _, subscriber := range subscribers {
		b.unsubscribeAllEvents <- subscriber
	}
}

func (b *broadcaster) SubscribeAggregateTypes(subscriber RecordSubscriber, aggregateTypes ...string) {
	for _, aggregateType := range aggregateTypes {
		b.subscribeAggregateTypes <- AggregateTypeSubscription{
			AggregateType: aggregateType,
			Subscriber:    subscriber,
		}
	}
}

func (b *broadcaster) UnsubscribeAggregateTypes(subscriber RecordSubscriber, aggregateTypes ...string) {
	for _, aggregateType := range aggregateTypes {
		b.unsubscribeAggregateTypes <- AggregateTypeSubscription{
			AggregateType: aggregateType,
			Subscriber:    subscriber,
		}
	}
}

func (b *broadcaster) start() {
	for {
		select {
		case subscriber := <-b.subscribeAllEvents:
			b.allEventsSubscribers[subscriber] = struct{}{}

		case subscriber := <-b.unsubscribeAllEvents:
			delete(b.allEventsSubscribers, subscriber)

		case subscription := <-b.subscribeAggregateTypes:
			if _, ok := b.aggregateTypeSubscribers[subscription.AggregateType]; !ok {
				b.aggregateTypeSubscribers[subscription.AggregateType] = make(map[RecordSubscriber]struct{})
			}
			b.aggregateTypeSubscribers[subscription.AggregateType][subscription.Subscriber] = struct{}{}

		case subscription := <-b.unsubscribeAggregateTypes:
			delete(b.aggregateTypeSubscribers[subscription.AggregateType], subscription.Subscriber)

		case record := <-b.bufferedRecords:
			b.broadcastRecord(record)

		case <-b.stopChan:
			b.closeAllSubscribers()
			return
		}
	}
}

func (b *broadcaster) broadcastRecord(record *rangedb.Record) {
	for subscriber := range b.allEventsSubscribers {
		select {
		case <-time.After(b.timeout):
			delete(b.allEventsSubscribers, subscriber)
			subscriber.Stop()

		case subscriber.Receiver() <- record:
		}
	}

	for subscriber := range b.aggregateTypeSubscribers[record.AggregateType] {
		select {
		case <-time.After(b.timeout):
			delete(b.aggregateTypeSubscribers[record.AggregateType], subscriber)
			subscriber.Stop()

		case subscriber.Receiver() <- record:
		}
	}
}

func (b *broadcaster) Close() {
	close(b.stopChan)
}

func (b *broadcaster) closeAllSubscribers() {
	for subscriber := range b.allEventsSubscribers {
		subscriber.Stop()
	}

	for _, subscribers := range b.aggregateTypeSubscribers {
		for subscriber := range subscribers {
			subscriber.Stop()
		}
	}
}
