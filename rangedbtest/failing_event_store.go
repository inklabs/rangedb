package rangedbtest

import (
	"context"
	"fmt"

	"github.com/inklabs/rangedb"
)

type failingEventStore struct{}

func NewFailingEventStore() *failingEventStore {
	return &failingEventStore{}
}

func (f failingEventStore) Bind(_ ...rangedb.Event) {}

func (f failingEventStore) EventsStartingWith(_ context.Context, _ uint64) <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) EventsByAggregateTypesStartingWith(_ context.Context, _ uint64, _ ...string) <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) EventsByStreamStartingWith(_ context.Context, _ uint64, _ string) <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) Save(_ rangedb.Event, _ interface{}) error {
	return fmt.Errorf("failingEventStore.Save")
}

func (f failingEventStore) SaveEvent(_, _, _, _ string, _ *uint64, _, _ interface{}) error {
	return fmt.Errorf("failingEventStore.SaveEvent")
}

func (f failingEventStore) OptimisticSave(_ uint64, _ rangedb.Event, _ interface{}) error {
	return fmt.Errorf("failingEventStore.OptimisticSave")
}

func (f failingEventStore) Subscribe(_ ...rangedb.RecordSubscriber) {}

func (f failingEventStore) SubscribeStartingWith(_ context.Context, _ uint64, _ ...rangedb.RecordSubscriber) {
}

func (f failingEventStore) TotalEventsInStream(_ string) uint64 {
	return 0
}

func getClosedChannel() <-chan *rangedb.Record {
	channel := make(chan *rangedb.Record)
	close(channel)
	return channel
}
