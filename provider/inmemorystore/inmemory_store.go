package inmemorystore

import (
	"context"
	"io/ioutil"
	"log"
	"sync"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
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

func (s *inMemoryStore) Bind(events ...rangedb.Event) {
	s.serializer.Bind(events...)
}

func (s *inMemoryStore) EventsStartingWith(ctx context.Context, eventNumber uint64) <-chan *rangedb.Record {
	s.mux.RLock()
	return s.recordsStartingWith(ctx, eventNumber, s.allRecords)
}

func (s *inMemoryStore) EventsByAggregateTypesStartingWith(ctx context.Context, eventNumber uint64, aggregateTypes ...string) <-chan *rangedb.Record {
	if len(aggregateTypes) == 1 {
		s.mux.RLock()
		return s.recordsStartingWith(ctx, eventNumber, s.recordsByAggregateType[aggregateTypes[0]])
	}

	var channels []<-chan *rangedb.Record
	for _, aggregateType := range aggregateTypes {
		s.mux.RLock()
		channels = append(channels, s.recordsStartingWith(ctx, 0, s.recordsByAggregateType[aggregateType]))
	}

	return rangedb.MergeRecordChannelsInOrder(channels, eventNumber)
}

func (s *inMemoryStore) EventsByStreamStartingWith(ctx context.Context, eventNumber uint64, stream string) <-chan *rangedb.Record {
	s.mux.RLock()
	return s.recordsStartingWith(ctx, eventNumber, s.recordsByStream[stream])
}

func (s *inMemoryStore) recordsStartingWith(ctx context.Context, eventNumber uint64, serializedRecords [][]byte) <-chan *rangedb.Record {
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

				select {
				case <-ctx.Done():
					break
				case records <- record:
				}
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
		event.AggregateID(),
		event.EventType(),
		shortuuid.New().String(),
		event,
		metadata,
	)
}

func (s *inMemoryStore) SaveEvent(aggregateType, aggregateID, eventType, eventID string, event, metadata interface{}) error {
	s.mux.Lock()

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
		s.mux.Unlock()
		return err
	}

	s.allRecords = append(s.allRecords, data)
	s.recordsByStream[stream] = append(s.recordsByStream[stream], data)
	s.recordsByAggregateType[aggregateType] = append(s.recordsByAggregateType[aggregateType], data)

	s.mux.Unlock()

	deSerializedRecord, _ := s.serializer.Deserialize(data)
	s.notifySubscribers(deSerializedRecord)

	return nil
}

func (s *inMemoryStore) SubscribeStartingWith(ctx context.Context, eventNumber uint64, subscribers ...rangedb.RecordSubscriber) {
	rangedb.ReplayEvents(s, eventNumber, subscribers...)

	select {
	case <-ctx.Done():
		return
	default:
		s.Subscribe(subscribers...)
	}
}

func (s *inMemoryStore) Subscribe(subscribers ...rangedb.RecordSubscriber) {
	s.subscriberMux.Lock()
	s.subscribers = append(s.subscribers, subscribers...)
	s.subscriberMux.Unlock()
}

func (s *inMemoryStore) TotalEventsInStream(streamName string) uint64 {
	return uint64(len(s.recordsByStream[streamName]))
}

func (s *inMemoryStore) notifySubscribers(record *rangedb.Record) {
	s.subscriberMux.RLock()

	for _, subscriber := range s.subscribers {
		subscriber.Accept(record)
	}

	s.subscriberMux.RUnlock()
}
