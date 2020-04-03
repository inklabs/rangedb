package rangedbapi_test

import (
	"fmt"
	"github.com/inklabs/rangedb/pkg/paging"

	"github.com/inklabs/rangedb"
)

type failingEventStore struct{}

func NewFailingEventStore() *failingEventStore {
	return &failingEventStore{}
}

func (f failingEventStore) AllEvents() <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) AllEventsByAggregateType(_ string) <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) AllEventsByAggregateTypes(_ ...string) <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) AllEventsByStream(_ string) <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) EventsByAggregateType(_ paging.Pagination, _ string) <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) EventsByAggregateTypeStartingWith(_ string, _ uint64) <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) EventsByStreamStartingWith(_ string, _ uint64) <-chan *rangedb.Record {
	return getClosedChannel()
}

func (f failingEventStore) Save(_ rangedb.Event, _ interface{}) error {
	return fmt.Errorf("failingEventStore.Save")
}

func (f failingEventStore) SaveEvent(_, _, _, _ string, _, _ interface{}) error {
	return fmt.Errorf("failingEventStore.SaveEvent")
}

func (f failingEventStore) Subscribe(_ ...rangedb.RecordSubscriber) {}

func (f failingEventStore) SubscribeAndReplay(_ ...rangedb.RecordSubscriber) {}

func getClosedChannel() <-chan *rangedb.Record {
	channel := make(chan *rangedb.Record)
	close(channel)
	return channel
}
