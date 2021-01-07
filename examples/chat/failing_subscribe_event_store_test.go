package chat_test

import (
	"context"
	"fmt"

	"github.com/inklabs/rangedb"
)

type failingSubscribeEventStore struct{}

func newFailingSubscribeEventStore() *failingSubscribeEventStore {
	return &failingSubscribeEventStore{}
}

func (f failingSubscribeEventStore) Bind(_ ...rangedb.Event) {}

func (f failingSubscribeEventStore) EventsStartingWith(_ context.Context, _ uint64) rangedb.RecordIterator {
	return getEmptyIterator()
}

func (f failingSubscribeEventStore) EventsByAggregateTypesStartingWith(_ context.Context, _ uint64, _ ...string) rangedb.RecordIterator {
	return getEmptyIterator()
}

func (f failingSubscribeEventStore) EventsByStreamStartingWith(_ context.Context, _ uint64, _ string) rangedb.RecordIterator {
	return getEmptyIterator()
}

func (f failingSubscribeEventStore) OptimisticSave(_ context.Context, _ uint64, _ ...*rangedb.EventRecord) error {
	return nil
}

func (f failingSubscribeEventStore) Save(_ context.Context, _ ...*rangedb.EventRecord) error {
	return nil
}

func (f failingSubscribeEventStore) Subscribe(_ context.Context, _ ...rangedb.RecordSubscriber) error {
	return fmt.Errorf("failingSubscribeEventStore.Subscribe")
}

func (f failingSubscribeEventStore) SubscribeStartingWith(_ context.Context, _ uint64, _ ...rangedb.RecordSubscriber) error {
	return nil
}

func (f failingSubscribeEventStore) TotalEventsInStream(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func getEmptyIterator() rangedb.RecordIterator {
	recordResults := make(chan rangedb.ResultRecord)
	return rangedb.NewRecordIterator(recordResults)
}
