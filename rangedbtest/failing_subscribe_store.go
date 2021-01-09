package rangedbtest

import (
	"context"
	"fmt"

	"github.com/inklabs/rangedb"
)

type failingSubscribeEventStore struct{}

// NewFailingSubscribeEventStore constructs a failing event store for testing.
func NewFailingSubscribeEventStore() *failingSubscribeEventStore {
	return &failingSubscribeEventStore{}
}

func (f failingSubscribeEventStore) Bind(_ ...rangedb.Event) {}

func (f failingSubscribeEventStore) Events(_ context.Context, _ uint64) rangedb.RecordIterator {
	return getEmptyIterator()
}

func (f failingSubscribeEventStore) EventsByAggregateTypes(_ context.Context, _ uint64, _ ...string) rangedb.RecordIterator {
	return getEmptyIterator()
}

func (f failingSubscribeEventStore) EventsByStream(_ context.Context, _ uint64, _ string) rangedb.RecordIterator {
	return getEmptyIterator()
}

func (f failingSubscribeEventStore) OptimisticSave(_ context.Context, _ uint64, _ ...*rangedb.EventRecord) (uint64, error) {
	return 0, nil
}

func (f failingSubscribeEventStore) Save(_ context.Context, _ ...*rangedb.EventRecord) (uint64, error) {
	return 0, nil
}

func (f failingSubscribeEventStore) Subscribe(_ context.Context, _ ...rangedb.RecordSubscriber) error {
	return fmt.Errorf("failingSubscribeEventStore.Subscribe")
}

func (f failingSubscribeEventStore) TotalEventsInStream(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func getEmptyIterator() rangedb.RecordIterator {
	recordResults := make(chan rangedb.ResultRecord)
	return rangedb.NewRecordIterator(recordResults)
}
