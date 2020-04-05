package inmemorystore

import (
	"io/ioutil"
	"log"
	"sync"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/paging"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

type inMemoryStore struct {
	clock      clock.Clock
	serializer rangedb.RecordSerializer
	logger     *log.Logger

	subscriberMux sync.RWMutex
	subscribers   []rangedb.RecordSubscriber

	mux                    sync.RWMutex
	allRecords             [][]byte
	recordsByStream        map[string][][]byte
	recordsByAggregateType map[string][][]byte
}

// Option defines functional option parameters for inMemoryStore.
type Option func(*inMemoryStore)

// WithClock is a functional option to inject a clock.Clock.
func WithClock(clock clock.Clock) Option {
	return func(store *inMemoryStore) {
		store.clock = clock
	}
}

// WithSerializer is a functional option to inject a RecordSerializer.
func WithSerializer(serializer rangedb.RecordSerializer) Option {
	return func(store *inMemoryStore) {
		store.serializer = serializer
	}
}

// WithLogger is a functional option to inject a Logger.
func WithLogger(logger *log.Logger) Option {
	return func(store *inMemoryStore) {
		store.logger = logger
	}
}

// New constructs an inMemoryStore.
func New(options ...Option) *inMemoryStore {
	s := &inMemoryStore{
		clock:                  systemclock.New(),
		serializer:             jsonrecordserializer.New(),
		logger:                 log.New(ioutil.Discard, "", 0),
		recordsByStream:        make(map[string][][]byte),
		recordsByAggregateType: make(map[string][][]byte),
	}

	for _, option := range options {
		option(s)
	}

	return s
}

func (s *inMemoryStore) AllEvents() <-chan *rangedb.Record {
	return s.recordsStartingWith(s.allRecords, 0)
}

func (s *inMemoryStore) AllEventsByAggregateType(aggregateType string) <-chan *rangedb.Record {
	return s.EventsByAggregateTypeStartingWith(aggregateType, 0)
}

func (s *inMemoryStore) AllEventsByAggregateTypes(aggregateTypes ...string) <-chan *rangedb.Record {
	channels := rangedb.GetAllEventsByAggregateTypes(s, aggregateTypes...)
	return rangedb.MergeRecordChannelsInOrder(channels)
}

func (s *inMemoryStore) AllEventsByStream(stream string) <-chan *rangedb.Record {
	return s.EventsByStreamStartingWith(stream, 0)
}

func (s *inMemoryStore) EventsByAggregateType(pagination paging.Pagination, aggregateType string) <-chan *rangedb.Record {
	return s.paginateRecords(pagination, s.recordsByAggregateType[aggregateType])
}

func (s *inMemoryStore) EventsByAggregateTypeStartingWith(aggregateType string, eventNumber uint64) <-chan *rangedb.Record {
	return s.recordsStartingWith(s.recordsByAggregateType[aggregateType], eventNumber)
}

func (s *inMemoryStore) EventsByStream(pagination paging.Pagination, streamName string) <-chan *rangedb.Record {
	return s.paginateRecords(pagination, s.recordsByStream[streamName])
}

func (s *inMemoryStore) EventsByStreamStartingWith(stream string, eventNumber uint64) <-chan *rangedb.Record {
	return s.recordsStartingWith(s.recordsByStream[stream], eventNumber)
}

func (s *inMemoryStore) recordsStartingWith(serializedRecords [][]byte, eventNumber uint64) <-chan *rangedb.Record {
	s.mux.RLock()

	records := make(chan *rangedb.Record)

	go func() {
		defer s.mux.RUnlock()

		count := uint64(0)
		for _, data := range serializedRecords {
			if count >= eventNumber {
				record, err := s.serializer.Deserialize(data)
				if err != nil {
					s.logger.Printf("failed to deserialize record: %v", err)
					continue
				}

				records <- record
			}
			count++
		}
		close(records)
	}()

	return records
}

func (s *inMemoryStore) paginateRecords(pagination paging.Pagination, serializedRecords [][]byte) <-chan *rangedb.Record {
	s.mux.RLock()

	records := make(chan *rangedb.Record)

	go func() {
		defer s.mux.RUnlock()

		firstEventNumber := (pagination.Page - 1) * pagination.ItemsPerPage
		count := 0
		for _, data := range serializedRecords {
			count++
			if count <= firstEventNumber {
				continue
			}

			record, err := s.serializer.Deserialize(data)
			if err != nil {
				s.logger.Printf("failed to deserialize record: %v", err)
				continue
			}

			records <- record

			if count-firstEventNumber >= pagination.ItemsPerPage {
				break
			}
		}
		close(records)
	}()

	return records
}

func (s *inMemoryStore) Save(event rangedb.Event, metadata interface{}) error {
	return s.SaveEvent(
		event.AggregateType(),
		event.AggregateID(),
		event.EventType(),
		shortuuid.New().String(),
		event,
		metadata,
	)
}

func (s *inMemoryStore) SaveEvent(aggregateType, aggregateID, eventType, eventID string, event, metadata interface{}) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if eventID == "" {
		eventID = shortuuid.New().String()
	}

	stream := rangedb.GetStream(aggregateType, aggregateID)
	record := &rangedb.Record{
		AggregateType:        aggregateType,
		AggregateID:          aggregateID,
		GlobalSequenceNumber: uint64(len(s.allRecords)),
		StreamSequenceNumber: uint64(len(s.recordsByStream[stream])),
		EventType:            eventType,
		EventID:              eventID,
		InsertTimestamp:      uint64(s.clock.Now().Unix()),
		Data:                 event,
		Metadata:             metadata,
	}

	data, err := s.serializer.Serialize(record)
	if err != nil {
		return err
	}

	s.allRecords = append(s.allRecords, data)
	s.recordsByStream[stream] = append(s.recordsByStream[stream], data)
	s.recordsByAggregateType[aggregateType] = append(s.recordsByAggregateType[aggregateType], data)

	s.notifySubscribers(record)

	return nil
}

func (s *inMemoryStore) SubscribeAndReplay(subscribers ...rangedb.RecordSubscriber) {
	rangedb.ReplayEvents(s, subscribers...)

	s.mux.Lock()
	s.Subscribe(subscribers...)
	defer s.mux.Unlock()
}

func (s *inMemoryStore) Subscribe(subscribers ...rangedb.RecordSubscriber) {
	s.subscriberMux.Lock()
	defer s.subscriberMux.Unlock()

	s.subscribers = append(s.subscribers, subscribers...)
}

func (s *inMemoryStore) TotalEventsInStream(streamName string) uint64 {
	return uint64(len(s.recordsByStream[streamName]))
}

func (s *inMemoryStore) notifySubscribers(record *rangedb.Record) {
	s.subscriberMux.RLock()
	defer s.subscriberMux.RUnlock()

	for _, subscriber := range s.subscribers {
		subscriber.Accept(record)
	}
}
