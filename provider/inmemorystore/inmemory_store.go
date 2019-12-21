package inmemorystore

import (
	"sync"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

type inMemoryStore struct {
	clock clock.Clock

	mux                    *sync.RWMutex
	allRecords             []*rangedb.Record
	recordsByStream        map[string][]*rangedb.Record
	recordsByAggregateType map[string][]*rangedb.Record
	nextIndexByStream      map[string]uint64
	globalSequenceNumber   uint64
}

type Option func(*inMemoryStore)

func WithClock(clock clock.Clock) Option {
	return func(store *inMemoryStore) {
		store.clock = clock
	}
}

func New(options ...Option) *inMemoryStore {
	s := &inMemoryStore{
		clock:                  systemclock.New(),
		mux:                    &sync.RWMutex{},
		recordsByStream:        make(map[string][]*rangedb.Record),
		recordsByAggregateType: make(map[string][]*rangedb.Record),
		nextIndexByStream:      make(map[string]uint64),
	}

	for _, option := range options {
		option(s)
	}

	return s
}

func (s *inMemoryStore) AllEvents() <-chan *rangedb.Record {
	s.mux.RLock()

	records := make(chan *rangedb.Record)

	go func() {
		defer s.mux.RUnlock()
		defer close(records)

		for _, record := range s.allRecords {
			records <- record
		}
	}()

	return records
}

func (s *inMemoryStore) EventsByStream(stream string) <-chan *rangedb.Record {
	return s.EventsByStreamStartingWith(stream, 0)
}

func (s *inMemoryStore) EventsByStreamStartingWith(stream string, eventNumber uint64) <-chan *rangedb.Record {
	s.mux.RLock()

	records := make(chan *rangedb.Record)

	go func() {
		defer s.mux.RUnlock()

		count := uint64(0)
		for _, record := range s.recordsByStream[stream] {
			if count >= eventNumber {
				records <- record
			}
			count++
		}
		close(records)
	}()

	return records
}

func (s *inMemoryStore) EventsByAggregateType(aggregateType string) <-chan *rangedb.Record {
	return s.EventsByAggregateTypeStartingWith(aggregateType, 0)
}

func (s *inMemoryStore) EventsByAggregateTypes(aggregateTypes ...string) <-chan *rangedb.Record {
	var channels []<-chan *rangedb.Record
	for _, aggregateType := range aggregateTypes {
		channels = append(channels, s.EventsByAggregateType(aggregateType))
	}

	return rangedb.MergeRecordChannelsInOrder(channels)
}

func (s *inMemoryStore) EventsByAggregateTypeStartingWith(aggregateType string, eventNumber uint64) <-chan *rangedb.Record {
	s.mux.RLock()

	records := make(chan *rangedb.Record)

	go func() {
		defer s.mux.RUnlock()

		count := uint64(0)
		for _, record := range s.recordsByAggregateType[aggregateType] {
			if count >= eventNumber {
				records <- record
			}
			count++
		}
		close(records)
	}()

	return records
}

func (s *inMemoryStore) Save(event rangedb.Event, metadata interface{}) error {
	return s.SaveEvent(
		event.AggregateType(),
		event.AggregateId(),
		event.EventType(),
		shortuuid.New().String(),
		event,
		metadata,
	)
}

func (s *inMemoryStore) SaveEvent(aggregateType, aggregateId, eventType, eventId string, event, metadata interface{}) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if eventId == "" {
		eventId = shortuuid.New().String()
	}

	stream := rangedb.GetStream(aggregateType, aggregateId)
	record := &rangedb.Record{
		AggregateType:        aggregateType,
		AggregateId:          aggregateId,
		GlobalSequenceNumber: s.globalSequenceNumber,
		StreamSequenceNumber: s.nextIndexByStream[stream],
		EventType:            eventType,
		EventId:              eventId,
		InsertTimestamp:      uint64(s.clock.Now().Unix()),
		Data:                 event,
		Metadata:             metadata,
	}

	s.allRecords = append(s.allRecords, record)
	s.recordsByStream[stream] = append(s.recordsByStream[stream], record)
	s.recordsByAggregateType[aggregateType] = append(s.recordsByAggregateType[aggregateType], record)
	s.nextIndexByStream[stream]++
	s.globalSequenceNumber++

	return nil
}
