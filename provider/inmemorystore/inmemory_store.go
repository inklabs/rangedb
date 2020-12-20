package inmemorystore

import (
	"context"
	"fmt"
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

func (s *inMemoryStore) OptimisticSave(expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(&expectedStreamSequenceNumber, eventRecords...)
}

func (s *inMemoryStore) Save(eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(nil, eventRecords...)
}

//saveEvents persists one or more events inside a locked mutex, and notifies subscribers.
func (s *inMemoryStore) saveEvents(expectedStreamSequenceNumber *uint64, eventRecords ...*rangedb.EventRecord) error {
	nextExpectedStreamSequenceNumber := expectedStreamSequenceNumber

	var pendingEventsData [][]byte
	var totalSavedEvents int
	var aggregateType, aggregateID string

	s.mux.Lock()
	for _, eventRecord := range eventRecords {
		if aggregateType != "" && aggregateType != eventRecord.Event.AggregateType() {
			s.mux.Unlock()
			return fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != "" && aggregateID != eventRecord.Event.AggregateID() {
			s.mux.Unlock()
			return fmt.Errorf("unmatched aggregate ID")
		}

		aggregateType = eventRecord.Event.AggregateType()
		aggregateID = eventRecord.Event.AggregateID()

		data, err := s.saveEvent(
			aggregateType,
			aggregateID,
			eventRecord.Event.EventType(),
			shortuuid.New().String(),
			nextExpectedStreamSequenceNumber,
			eventRecord.Event,
			eventRecord.Metadata,
		)
		if err != nil {
			s.removeEvents(totalSavedEvents, eventRecord.Event.AggregateType(), eventRecord.Event.AggregateID())
			s.mux.Unlock()
			return err
		}

		totalSavedEvents++
		pendingEventsData = append(pendingEventsData, data)

		if nextExpectedStreamSequenceNumber != nil {
			*nextExpectedStreamSequenceNumber++
		}
	}
	s.mux.Unlock()

	for _, data := range pendingEventsData {
		deSerializedRecord, _ := s.serializer.Deserialize(data)
		s.notifySubscribers(deSerializedRecord)
	}

	return nil
}

//saveEvent persists a single event without locking the mutex, or notifying subscribers.
func (s *inMemoryStore) saveEvent(
	aggregateType, aggregateID, eventType, eventID string,
	expectedStreamSequenceNumber *uint64,
	event interface{}, metadata interface{}) ([]byte, error) {

	stream := rangedb.GetStream(aggregateType, aggregateID)
	nextSequenceNumber := s.getNextStreamSequenceNumber(stream)

	if expectedStreamSequenceNumber != nil && *expectedStreamSequenceNumber != nextSequenceNumber {
		return nil, &rangedb.UnexpectedSequenceNumber{
			Expected:           *expectedStreamSequenceNumber,
			NextSequenceNumber: nextSequenceNumber,
		}
	}

	record := &rangedb.Record{
		AggregateType:        aggregateType,
		AggregateID:          aggregateID,
		GlobalSequenceNumber: s.getNextGlobalSequenceNumber(),
		StreamSequenceNumber: nextSequenceNumber,
		EventType:            eventType,
		EventID:              eventID,
		InsertTimestamp:      uint64(s.clock.Now().Unix()),
		Data:                 event,
		Metadata:             metadata,
	}

	data, err := s.serializer.Serialize(record)
	if err != nil {
		return nil, err
	}

	s.allRecords = append(s.allRecords, data)
	s.recordsByStream[stream] = append(s.recordsByStream[stream], data)
	s.recordsByAggregateType[aggregateType] = append(s.recordsByAggregateType[aggregateType], data)

	return data, nil
}

func (s *inMemoryStore) removeEvents(total int, aggregateType, aggregateID string) {
	stream := rangedb.GetStream(aggregateType, aggregateID)
	s.allRecords = rTrimFromByteSlice(s.allRecords, total)
	s.recordsByStream[stream] = rTrimFromByteSlice(s.recordsByStream[stream], total)
	s.recordsByAggregateType[aggregateType] = rTrimFromByteSlice(s.recordsByAggregateType[aggregateType], total)
}

func rTrimFromByteSlice(slice [][]byte, total int) [][]byte {
	return slice[:len(slice)-total]
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
	s.mux.RLock()
	defer s.mux.RUnlock()
	return uint64(len(s.recordsByStream[streamName]))
}

func (s *inMemoryStore) getNextStreamSequenceNumber(stream string) uint64 {
	return uint64(len(s.recordsByStream[stream]))
}

func (s *inMemoryStore) getNextGlobalSequenceNumber() uint64 {
	return uint64(len(s.allRecords))
}

func (s *inMemoryStore) notifySubscribers(record *rangedb.Record) {
	s.subscriberMux.RLock()

	for _, subscriber := range s.subscribers {
		subscriber.Accept(record)
	}

	s.subscriberMux.RUnlock()
}
