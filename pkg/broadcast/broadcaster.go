package broadcast

import (
	"log"
	"time"

	"github.com/inklabs/rangedb"
)

// SendRecordChan is a write only channel for rangedb.Record.
type SendRecordChan chan<- *rangedb.Record

// RecordSubscriber defines how a rangedb.Record is received.
type RecordSubscriber interface {
	Receiver() SendRecordChan
}

type Broadcaster interface {
	Accept(record *rangedb.Record)
	SubscribeAllEvents(subscribers ...RecordSubscriber)
	UnsubscribeAllEvents(subscribers ...RecordSubscriber)
	SubscribeAggregateTypes(subscriber RecordSubscriber, aggregateTypes ...string)
	UnsubscribeAggregateTypes(subscriber RecordSubscriber, aggregateTypes ...string)
	SetTimeout(duration time.Duration)
	Close() error
}

type broadcaster struct {
	subscribeAllEventsChan        chan SendRecordChan
	unsubscribeAllEventsChan      chan SendRecordChan
	subscribeAggregateTypesChan   chan AggregateTypeSubscription
	unsubscribeAggregateTypesChan chan AggregateTypeSubscription
	bufferedRecords               chan *rangedb.Record
	stopChan                      chan struct{}
	allEventsSubscribers          map[SendRecordChan]struct{}
	aggregateTypeSubscribers      map[string]map[SendRecordChan]struct{}
	timeout                       time.Duration
}

func New(bufferSize int) *broadcaster {
	broadcaster := &broadcaster{
		subscribeAllEventsChan:        make(chan SendRecordChan),
		unsubscribeAllEventsChan:      make(chan SendRecordChan),
		subscribeAggregateTypesChan:   make(chan AggregateTypeSubscription),
		unsubscribeAggregateTypesChan: make(chan AggregateTypeSubscription),
		bufferedRecords:               make(chan *rangedb.Record, bufferSize),
		stopChan:                      make(chan struct{}),
		allEventsSubscribers:          make(map[SendRecordChan]struct{}),
		aggregateTypeSubscribers:      make(map[string]map[SendRecordChan]struct{}),
		timeout:                       time.Millisecond * 100,
	}

	go broadcaster.start()

	return broadcaster
}

func (b *broadcaster) Accept(record *rangedb.Record) {
	b.bufferedRecords <- record
}

func (b *broadcaster) SubscribeAllEvents(subscribers ...RecordSubscriber) {
	for _, subscriber := range subscribers {
		b.subscribeAllEventsChan <- subscriber.Receiver()
	}
}
func (b *broadcaster) UnsubscribeAllEvents(subscribers ...RecordSubscriber) {
	for _, subscriber := range subscribers {
		b.unsubscribeAllEventsChan <- subscriber.Receiver()
	}
}

type AggregateTypeSubscription struct {
	AggregateType  string
	SendRecordChan SendRecordChan
}

func (b *broadcaster) SubscribeAggregateTypes(subscriber RecordSubscriber, aggregateTypes ...string) {
	for _, aggregateType := range aggregateTypes {
		b.subscribeAggregateTypesChan <- AggregateTypeSubscription{
			AggregateType:  aggregateType,
			SendRecordChan: subscriber.Receiver(),
		}
	}
}

func (b *broadcaster) UnsubscribeAggregateTypes(subscriber RecordSubscriber, aggregateTypes ...string) {
	for _, aggregateType := range aggregateTypes {
		b.unsubscribeAggregateTypesChan <- AggregateTypeSubscription{
			AggregateType:  aggregateType,
			SendRecordChan: subscriber.Receiver(),
		}
	}
}

func (b *broadcaster) start() {
	for {
		select {
		case ch := <-b.subscribeAllEventsChan:
			b.allEventsSubscribers[ch] = struct{}{}

		case ch := <-b.unsubscribeAllEventsChan:
			delete(b.allEventsSubscribers, ch)

		case subscription := <-b.subscribeAggregateTypesChan:
			if _, ok := b.aggregateTypeSubscribers[subscription.AggregateType]; !ok {
				b.aggregateTypeSubscribers[subscription.AggregateType] = make(map[SendRecordChan]struct{})
			}
			b.aggregateTypeSubscribers[subscription.AggregateType][subscription.SendRecordChan] = struct{}{}

		case subscription := <-b.unsubscribeAggregateTypesChan:
			delete(b.aggregateTypeSubscribers[subscription.AggregateType], subscription.SendRecordChan)

		case record := <-b.bufferedRecords:
			b.broadcastRecord(record)

		case <-b.stopChan:
			return
		}
	}
}

func (b *broadcaster) broadcastRecord(record *rangedb.Record) {
	for subscriber := range b.allEventsSubscribers {
		select {
		case <-time.After(b.timeout):
			// TODO: drop connection since the client missed a record
			log.Printf("timeout after %s", b.timeout.String())
		case subscriber <- record:
		}
	}

	for subscriber := range b.aggregateTypeSubscribers[record.AggregateType] {
		select {
		case <-time.After(b.timeout):
			// TODO: drop connection since the client missed a record
			log.Printf("timeout after %s", b.timeout.String())
		case subscriber <- record:
		}
	}
}

func (b *broadcaster) SetTimeout(duration time.Duration) {
	b.timeout = duration
}

func (b *broadcaster) Close() error {
	close(b.stopChan)
	return nil
}
