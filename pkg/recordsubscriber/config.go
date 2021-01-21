package recordsubscriber

import (
	"context"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
)

// Config defines a record subscription configuration.
type Config struct {
	BufferSize    int
	GetRecords    GetRecordsIteratorFunc
	ConsumeRecord ConsumeRecordFunc
	Subscribe     SubscribeFunc
	Unsubscribe   SubscribeFunc
	DoneChan      <-chan struct{}
}

// AllEventsConfig returns a configuration to subscribe to all events.
func AllEventsConfig(ctx context.Context, store rangedb.Store, broadcaster broadcast.Broadcaster, bufferLength int, consumeRecord ConsumeRecordFunc) Config {
	return Config{
		BufferSize: bufferLength,
		DoneChan:   ctx.Done(),
		Subscribe: func(subscriber broadcast.RecordSubscriber) {
			broadcaster.SubscribeAllEvents(subscriber)
		},
		Unsubscribe: func(subscriber broadcast.RecordSubscriber) {
			broadcaster.UnsubscribeAllEvents(subscriber)
		},
		GetRecords: func(globalSequenceNumber uint64) rangedb.RecordIterator {
			return store.Events(ctx, globalSequenceNumber)
		},
		ConsumeRecord: consumeRecord,
	}
}

// AggregateTypesConfig returns a configuration to subscribe to events by aggregate types.
func AggregateTypesConfig(ctx context.Context, store rangedb.Store, broadcaster broadcast.Broadcaster, bufferLength int, aggregateTypes []string, consumeRecord ConsumeRecordFunc) Config {
	return Config{
		BufferSize: bufferLength,
		DoneChan:   ctx.Done(),
		Subscribe: func(subscriber broadcast.RecordSubscriber) {
			broadcaster.SubscribeAggregateTypes(subscriber, aggregateTypes...)
		},
		Unsubscribe: func(subscriber broadcast.RecordSubscriber) {
			broadcaster.UnsubscribeAggregateTypes(subscriber, aggregateTypes...)
		},
		GetRecords: func(globalSequenceNumber uint64) rangedb.RecordIterator {
			return store.EventsByAggregateTypes(ctx, globalSequenceNumber, aggregateTypes...)
		},
		ConsumeRecord: consumeRecord,
	}
}
