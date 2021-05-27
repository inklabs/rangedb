package rangedbtest

import (
	"context"
	"fmt"

	"github.com/inklabs/rangedb"
)

type failingEventStore struct{}

// NewFailingEventStore constructs a failing event store for testing.
func NewFailingEventStore() *failingEventStore {
	return &failingEventStore{}
}

func (f failingEventStore) Bind(_ ...rangedb.Event) {}

func (f failingEventStore) Events(_ context.Context, _ uint64) rangedb.RecordIterator {
	return getFailingIterator("Events")
}

func (f failingEventStore) EventsByAggregateTypes(_ context.Context, _ uint64, _ ...string) rangedb.RecordIterator {
	return getFailingIterator("EventsByAggregateTypes")
}

func (f failingEventStore) EventsByStream(_ context.Context, _ uint64, _ string) rangedb.RecordIterator {
	return getFailingIterator("EventsByStream")
}

func (f failingEventStore) OptimisticDeleteStream(_ context.Context, _ uint64, _ string) error {
	return fmt.Errorf("failingEventStore.OptimisitDeleteStream")
}

func (f failingEventStore) OptimisticSave(_ context.Context, _ uint64, _ ...*rangedb.EventRecord) (uint64, error) {
	return 0, fmt.Errorf("failingEventStore.OptimisticSave")
}

func (f failingEventStore) Save(_ context.Context, _ ...*rangedb.EventRecord) (uint64, error) {
	return 0, fmt.Errorf("failingEventStore.Save")
}

func (f failingEventStore) AllEventsSubscription(_ context.Context, _ int, _ rangedb.RecordSubscriber) rangedb.RecordSubscription {
	return &dummyRecordSubscription{}
}

func (f failingEventStore) AggregateTypesSubscription(_ context.Context, _ int, _ rangedb.RecordSubscriber, _ ...string) rangedb.RecordSubscription {
	return &dummyRecordSubscription{}
}

func (f failingEventStore) TotalEventsInStream(_ context.Context, _ string) (uint64, error) {
	return 0, fmt.Errorf("failingEventStore.TotalEventsInStream")
}

func getFailingIterator(name string) rangedb.RecordIterator {
	recordResults := make(chan rangedb.ResultRecord, 1)
	recordResults <- rangedb.ResultRecord{
		Record: nil,
		Err:    fmt.Errorf("failingEventStore.%s", name),
	}
	close(recordResults)
	return rangedb.NewRecordIterator(recordResults)
}
