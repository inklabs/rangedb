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

func (f failingSubscribeEventStore) OptimisticDeleteStream(_ context.Context, _ uint64, _ string) error {
	return nil
}

func (f failingSubscribeEventStore) OptimisticSave(_ context.Context, _ uint64, _ string, _ ...*rangedb.EventRecord) (uint64, error) {
	return 0, nil
}

func (f failingSubscribeEventStore) Save(_ context.Context, _ string, _ ...*rangedb.EventRecord) (uint64, error) {
	return 0, nil
}

func (f failingSubscribeEventStore) AllEventsSubscription(_ context.Context, _ int, _ rangedb.RecordSubscriber) rangedb.RecordSubscription {
	return &failingRecordSubscription{}
}

func (f failingSubscribeEventStore) AggregateTypesSubscription(_ context.Context, _ int, _ rangedb.RecordSubscriber, _ ...string) rangedb.RecordSubscription {
	return &failingRecordSubscription{}
}

func (f failingSubscribeEventStore) TotalEventsInStream(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func getEmptyIterator() rangedb.RecordIterator {
	recordResults := make(chan rangedb.ResultRecord)
	return rangedb.NewRecordIterator(recordResults)
}

type failingRecordSubscription struct{}

func (f failingRecordSubscription) Start() error {
	return fmt.Errorf("failingRecordSubscription.Start")
}

func (f failingRecordSubscription) StartFrom(_ uint64) error {
	return fmt.Errorf("failingRecordSubscription.StartFrom")
}

func (f failingRecordSubscription) Stop() {}

type dummyRecordSubscription struct{}

func (f dummyRecordSubscription) Start() error {
	return nil
}

func (f dummyRecordSubscription) StartFrom(_ uint64) error {
	return nil
}

func (f dummyRecordSubscription) Stop() {}
