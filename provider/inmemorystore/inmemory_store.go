package inmemorystore

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/rangedberror"
	"github.com/inklabs/rangedb/pkg/recordsubscriber"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

const broadcastRecordBuffSize = 100

type inMemoryStore struct {
	clock       clock.Clock
	serializer  rangedb.RecordSerializer
	logger      *log.Logger
	broadcaster broadcast.Broadcaster

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
		broadcaster:            broadcast.New(broadcastRecordBuffSize, broadcast.DefaultTimeout),
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

func (s *inMemoryStore) Events(ctx context.Context, globalSequenceNumber uint64) rangedb.RecordIterator {
	s.mux.RLock()
	return s.recordsToIterator(ctx, s.allRecords, func(record *rangedb.Record) bool {
		return record.GlobalSequenceNumber >= globalSequenceNumber
	})
}

func (s *inMemoryStore) EventsByAggregateTypes(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) rangedb.RecordIterator {
	if len(aggregateTypes) == 1 {
		s.mux.RLock()
		return s.recordsToIterator(ctx, s.recordsByAggregateType[aggregateTypes[0]], compareByGlobalSequenceNumber(globalSequenceNumber))
	}

	var recordIterators []rangedb.RecordIterator
	for _, aggregateType := range aggregateTypes {
		s.mux.RLock()
		recordIterators = append(recordIterators, s.recordsToIterator(ctx, s.recordsByAggregateType[aggregateType], compareByGlobalSequenceNumber(globalSequenceNumber)))
	}

	return rangedb.MergeRecordIteratorsInOrder(recordIterators)
}

func compareByGlobalSequenceNumber(globalSequenceNumber uint64) func(record *rangedb.Record) bool {
	return func(record *rangedb.Record) bool {
		return record.GlobalSequenceNumber >= globalSequenceNumber
	}
}

func (s *inMemoryStore) EventsByStream(ctx context.Context, streamSequenceNumber uint64, stream string) rangedb.RecordIterator {
	s.mux.RLock()

	if _, ok := s.recordsByStream[stream]; !ok || len(s.recordsByStream[stream]) == 0 {
		s.mux.RUnlock()
		return rangedb.NewRecordIteratorWithError(rangedb.ErrStreamNotFound)
	}

	return s.recordsToIterator(ctx, s.recordsByStream[stream], func(record *rangedb.Record) bool {
		return record.StreamSequenceNumber >= streamSequenceNumber
	})
}

func (s *inMemoryStore) recordsToIterator(ctx context.Context, serializedRecords [][]byte, compare func(record *rangedb.Record) bool) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer s.mux.RUnlock()
		defer close(resultRecords)

		for _, data := range serializedRecords {
			record, err := s.serializer.Deserialize(data)
			if err != nil {
				deserializeErr := fmt.Errorf("failed to deserialize record: %v", err)
				s.logger.Printf(deserializeErr.Error())
				resultRecords <- rangedb.ResultRecord{Err: deserializeErr}
				return
			}

			if compare(record) {
				if !rangedb.PublishRecordOrCancel(ctx, resultRecords, record, time.Second) {
					return
				}
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *inMemoryStore) OptimisticDeleteStream(ctx context.Context, expectedStreamSequenceNumber uint64, streamName string) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if _, ok := s.recordsByStream[streamName]; !ok {
		return rangedb.ErrStreamNotFound
	}

	currentStreamSequenceNumber := s.getStreamSequenceNumber(streamName)
	if expectedStreamSequenceNumber != currentStreamSequenceNumber {
		return &rangedberror.UnexpectedSequenceNumber{
			Expected:             expectedStreamSequenceNumber,
			ActualSequenceNumber: currentStreamSequenceNumber,
		}
	}

	delete(s.recordsByStream, streamName)

	return nil
}

func (s *inMemoryStore) OptimisticSave(ctx context.Context, expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.saveEvents(ctx, &expectedStreamSequenceNumber, eventRecords...)
}

func (s *inMemoryStore) Save(ctx context.Context, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.saveEvents(ctx, nil, eventRecords...)
}

// saveEvents persists one or more events inside a locked mutex, and notifies subscribers.
func (s *inMemoryStore) saveEvents(ctx context.Context, expectedStreamSequenceNumber *uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	if len(eventRecords) < 1 {
		return 0, fmt.Errorf("missing events")
	}

	var pendingEventsData [][]byte
	var totalSavedEvents int
	var aggregateType, aggregateID string
	var lastStreamSequenceNumber uint64

	s.mux.Lock()
	for _, eventRecord := range eventRecords {
		if aggregateType != "" && aggregateType != eventRecord.Event.AggregateType() {
			s.mux.Unlock()
			return 0, fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != "" && aggregateID != eventRecord.Event.AggregateID() {
			s.mux.Unlock()
			return 0, fmt.Errorf("unmatched aggregate ID")
		}

		aggregateType = eventRecord.Event.AggregateType()
		aggregateID = eventRecord.Event.AggregateID()

		select {
		case <-ctx.Done():
			s.mux.Unlock()
			return 0, context.Canceled

		default:
		}

		var data []byte
		var err error
		data, lastStreamSequenceNumber, err = s.saveEvent(
			aggregateType,
			aggregateID,
			eventRecord.Event.EventType(),
			shortuuid.New().String(),
			expectedStreamSequenceNumber,
			eventRecord.Event,
			eventRecord.Metadata,
		)
		if err != nil {
			s.removeEvents(totalSavedEvents, eventRecord.Event.AggregateType(), eventRecord.Event.AggregateID())
			s.mux.Unlock()
			return 0, err
		}

		totalSavedEvents++
		pendingEventsData = append(pendingEventsData, data)

		if expectedStreamSequenceNumber != nil {
			*expectedStreamSequenceNumber++
		}
	}
	s.mux.Unlock()

	for _, data := range pendingEventsData {
		deSerializedRecord, err := s.serializer.Deserialize(data)
		if err == nil {
			s.broadcaster.Accept(deSerializedRecord)
		} else {
			s.logger.Print(err)
		}
	}

	return lastStreamSequenceNumber, nil
}

// saveEvent persists a single event without locking the mutex, or notifying subscribers.
func (s *inMemoryStore) saveEvent(
	aggregateType, aggregateID, eventType, eventID string,
	expectedStreamSequenceNumber *uint64,
	event interface{}, metadata interface{}) ([]byte, uint64, error) {

	stream := rangedb.GetStream(aggregateType, aggregateID)
	streamSequenceNumber := s.getStreamSequenceNumber(stream)

	if expectedStreamSequenceNumber != nil && *expectedStreamSequenceNumber != streamSequenceNumber {
		return nil, 0, &rangedberror.UnexpectedSequenceNumber{
			Expected:             *expectedStreamSequenceNumber,
			ActualSequenceNumber: streamSequenceNumber,
		}
	}

	record := &rangedb.Record{
		AggregateType:        aggregateType,
		AggregateID:          aggregateID,
		GlobalSequenceNumber: s.getGlobalSequenceNumber() + 1,
		StreamSequenceNumber: streamSequenceNumber + 1,
		EventType:            eventType,
		EventID:              eventID,
		InsertTimestamp:      uint64(s.clock.Now().Unix()),
		Data:                 event,
		Metadata:             metadata,
	}

	data, err := s.serializer.Serialize(record)
	if err != nil {
		return nil, 0, err
	}

	s.allRecords = append(s.allRecords, data)
	s.recordsByStream[stream] = append(s.recordsByStream[stream], data)
	s.recordsByAggregateType[aggregateType] = append(s.recordsByAggregateType[aggregateType], data)

	return data, record.StreamSequenceNumber, nil
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

func (s *inMemoryStore) AllEventsSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AllEventsConfig(ctx, s, s.broadcaster, bufferSize,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *inMemoryStore) AggregateTypesSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber, aggregateTypes ...string) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AggregateTypesConfig(ctx, s, s.broadcaster, bufferSize,
			aggregateTypes,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *inMemoryStore) TotalEventsInStream(ctx context.Context, streamName string) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, context.Canceled

	default:
	}

	s.mux.RLock()
	defer s.mux.RUnlock()
	return uint64(len(s.recordsByStream[streamName])), nil
}

func (s *inMemoryStore) getStreamSequenceNumber(stream string) uint64 {
	return uint64(len(s.recordsByStream[stream]))
}

func (s *inMemoryStore) getGlobalSequenceNumber() uint64 {
	return uint64(len(s.allRecords))
}
