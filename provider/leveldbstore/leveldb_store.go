package leveldbstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/rangedberror"
	"github.com/inklabs/rangedb/pkg/recordsubscriber"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

const (
	broadcastRecordBuffSize = 100
	separator               = "!"
	allEventsPrefix         = "$all$" + separator
)

type levelDbStore struct {
	clock       clock.Clock
	serializer  rangedb.RecordSerializer
	broadcaster broadcast.Broadcaster
	logger      *log.Logger

	mux sync.RWMutex
	db  *leveldb.DB
}

// Option defines functional option parameters for levelDbStore.
type Option func(*levelDbStore)

// WithClock is a functional option to inject a Clock.
func WithClock(clock clock.Clock) Option {
	return func(store *levelDbStore) {
		store.clock = clock
	}
}

// WithSerializer is a functional option to inject a RecordSerializer.
func WithSerializer(serializer rangedb.RecordSerializer) Option {
	return func(store *levelDbStore) {
		store.serializer = serializer
	}
}

// WithLogger is a functional option to inject a Logger.
func WithLogger(logger *log.Logger) Option {
	return func(store *levelDbStore) {
		store.logger = logger
	}
}

// New constructs a levelDbStore.
func New(dbFilePath string, options ...Option) (*levelDbStore, error) {
	db, err := leveldb.OpenFile(dbFilePath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed opening db: %v", err)
	}

	s := &levelDbStore{
		clock:       systemclock.New(),
		serializer:  jsonrecordserializer.New(),
		logger:      log.New(ioutil.Discard, "", 0),
		broadcaster: broadcast.New(broadcastRecordBuffSize, broadcast.DefaultTimeout),
		db:          db,
	}

	for _, option := range options {
		option(s)
	}

	return s, nil
}

func (s *levelDbStore) Stop() error {
	return s.db.Close()
}

func (s *levelDbStore) Bind(events ...rangedb.Event) {
	s.serializer.Bind(events...)
}

func (s *levelDbStore) Events(ctx context.Context, globalSequenceNumber uint64) rangedb.RecordIterator {
	return s.getEventsByLookup(ctx, allEventsPrefix, globalSequenceNumber)
}

func (s *levelDbStore) EventsByAggregateTypes(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) rangedb.RecordIterator {
	if len(aggregateTypes) == 1 {
		return s.getEventsByLookup(ctx, getAggregateTypeKeyPrefix(aggregateTypes[0]), globalSequenceNumber)
	}

	var recordIterators []rangedb.RecordIterator
	for _, aggregateType := range aggregateTypes {
		recordIterators = append(recordIterators, s.getEventsByLookup(ctx, getAggregateTypeKeyPrefix(aggregateType), globalSequenceNumber))
	}

	return rangedb.MergeRecordIteratorsInOrder(recordIterators)
}

func (s *levelDbStore) EventsByStream(ctx context.Context, streamSequenceNumber uint64, stream string) rangedb.RecordIterator {
	return s.getEventsByPrefix(ctx, stream, streamSequenceNumber)
}

func (s *levelDbStore) OptimisticSave(ctx context.Context, expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.saveEvents(ctx, &expectedStreamSequenceNumber, eventRecords...)
}

func (s *levelDbStore) Save(ctx context.Context, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.saveEvents(ctx, nil, eventRecords...)
}

func (s *levelDbStore) saveEvents(ctx context.Context, expectedStreamSequenceNumber *uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	if len(eventRecords) < 1 {
		return 0, fmt.Errorf("missing events")
	}

	nextExpectedStreamSequenceNumber := expectedStreamSequenceNumber

	var pendingEventsData [][]byte
	var aggregateType, aggregateID string
	var lastStreamSequenceNumber uint64

	s.mux.Lock()
	transaction, err := s.db.OpenTransaction()
	if err != nil {
		return 0, err
	}

	for _, eventRecord := range eventRecords {
		if aggregateType != "" && aggregateType != eventRecord.Event.AggregateType() {
			transaction.Discard()
			s.mux.Unlock()
			return 0, fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != "" && aggregateID != eventRecord.Event.AggregateID() {
			transaction.Discard()
			s.mux.Unlock()
			return 0, fmt.Errorf("unmatched aggregate ID")
		}

		aggregateType = eventRecord.Event.AggregateType()
		aggregateID = eventRecord.Event.AggregateID()

		var data []byte
		var err error
		data, lastStreamSequenceNumber, err = s.saveEvent(
			ctx,
			transaction,
			aggregateType,
			aggregateID,
			eventRecord.Event.EventType(),
			shortuuid.New().String(),
			nextExpectedStreamSequenceNumber,
			eventRecord.Event,
			eventRecord.Metadata,
		)
		if err != nil {
			transaction.Discard()
			s.mux.Unlock()
			return 0, err
		}

		pendingEventsData = append(pendingEventsData, data)

		if nextExpectedStreamSequenceNumber != nil {
			*nextExpectedStreamSequenceNumber++
		}
	}
	err = transaction.Commit()
	if err != nil {
		return 0, err
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
func (s *levelDbStore) saveEvent(ctx context.Context, transaction *leveldb.Transaction,
	aggregateType, aggregateID, eventType, eventID string,
	expectedStreamSequenceNumber *uint64,
	event, metadata interface{}) ([]byte, uint64, error) {

	select {
	case <-ctx.Done():
		return nil, 0, context.Canceled

	default:
	}

	stream := rangedb.GetStream(aggregateType, aggregateID)
	nextSequenceNumber := s.getNextStreamSequenceNumber(transaction, stream)

	if expectedStreamSequenceNumber != nil && *expectedStreamSequenceNumber != nextSequenceNumber {
		return nil, 0, &rangedberror.UnexpectedSequenceNumber{
			Expected:           *expectedStreamSequenceNumber,
			NextSequenceNumber: nextSequenceNumber,
		}
	}

	record := &rangedb.Record{
		AggregateType:        aggregateType,
		AggregateID:          aggregateID,
		GlobalSequenceNumber: s.getNextGlobalSequenceNumber(transaction),
		StreamSequenceNumber: nextSequenceNumber,
		EventType:            eventType,
		EventID:              eventID,
		InsertTimestamp:      uint64(s.clock.Now().Unix()),
		Data:                 event,
		Metadata:             metadata,
	}

	batch := new(leveldb.Batch)
	data, err := s.serializer.Serialize(record)
	if err != nil {
		return nil, 0, err
	}

	streamKey := getKeyWithNumber(stream+separator, record.StreamSequenceNumber)
	batch.Put(streamKey, data)

	allAggregateTypeKey := getKeyWithNumber(getAggregateTypeKeyPrefix(aggregateType), record.GlobalSequenceNumber)
	batch.Put(allAggregateTypeKey, streamKey)

	allEventsKey := getKeyWithNumber(allEventsPrefix, record.GlobalSequenceNumber)
	batch.Put(allEventsKey, streamKey)

	err = transaction.Write(batch, nil)
	if err != nil {
		return nil, 0, err
	}

	return data, record.StreamSequenceNumber, nil
}

func (s *levelDbStore) AllEventsSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AllEventsConfig(ctx, s, s.broadcaster, bufferSize,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *levelDbStore) AggregateTypesSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber, aggregateTypes ...string) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AggregateTypesConfig(ctx, s, s.broadcaster, bufferSize,
			aggregateTypes,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *levelDbStore) TotalEventsInStream(ctx context.Context, streamName string) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, context.Canceled

	default:
	}

	return s.getNextStreamSequenceNumber(s.db, streamName), nil
}

func (s *levelDbStore) getEventsByPrefix(ctx context.Context, prefix string, streamSequenceNumber uint64) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)
	s.mux.RLock()

	go func() {
		defer s.mux.RUnlock()
		defer close(resultRecords)

		iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
		defer iter.Release()

		for iter.Next() {
			record, err := s.getRecordByValue(iter.Value())
			if err != nil {
				resultRecords <- rangedb.ResultRecord{Err: err}
				return
			}

			if record.StreamSequenceNumber < streamSequenceNumber {
				continue
			}

			if !rangedb.PublishRecordOrCancel(ctx, resultRecords, record, time.Second) {
				return
			}
		}

		_ = iter.Error()
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *levelDbStore) getEventsByLookup(ctx context.Context, key string, globalSequenceNumber uint64) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)
	s.mux.RLock()

	go func() {
		defer s.mux.RUnlock()
		defer close(resultRecords)

		iter := s.db.NewIterator(util.BytesPrefix([]byte(key)), nil)
		defer iter.Release()

		if globalSequenceNumber > 0 {
			seekKey := getKeyWithNumber(key, globalSequenceNumber)
			found := iter.Seek(seekKey)
			if found {
				iter.Prev()
			}
		}

		for iter.Next() {
			targetKey := iter.Value()

			record, err := s.getRecordByLookup(targetKey, iter)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{Err: err}
				return
			}

			if record.GlobalSequenceNumber < globalSequenceNumber {
				continue
			}

			if !rangedb.PublishRecordOrCancel(ctx, resultRecords, record, time.Second) {
				return
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *levelDbStore) getRecordByLookup(targetKey []byte, iter iterator.Iterator) (*rangedb.Record, error) {
	data, err := s.db.Get(targetKey, nil)
	if err != nil {
		s.logger.Printf("unable to find lookup record %s for %s: %v", targetKey, iter.Key(), err)
		return nil, err
	}

	return s.getRecordByValue(data)
}

func (s *levelDbStore) getRecordByValue(value []byte) (*rangedb.Record, error) {
	record, err := s.serializer.Deserialize(value)
	if err != nil {
		s.logger.Printf("failed to deserialize record: %v", err)
		return nil, err
	}

	return record, nil
}

func getAggregateTypeKeyPrefix(aggregateType string) string {
	return fmt.Sprintf("$%s$%s", aggregateType, separator)
}

func getKeyWithNumber(inputKey string, number uint64) []byte {
	return append([]byte(inputKey), uint64ToBytes(number)...)
}

func uint64ToBytes(number uint64) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, number)
	return buf.Bytes()
}

func bytesToUint64(input []byte) uint64 {
	var number uint64
	_ = binary.Read(bytes.NewReader(input), binary.BigEndian, &number)
	return number
}

type dbNewIterable interface {
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

func (s *levelDbStore) getNextGlobalSequenceNumber(iterable dbNewIterable) uint64 {
	return s.getNextSequenceNumber(iterable, allEventsPrefix)
}

func (s *levelDbStore) getNextStreamSequenceNumber(iterable dbNewIterable, stream string) uint64 {
	return s.getNextSequenceNumber(iterable, stream+separator)
}

func (s *levelDbStore) getNextSequenceNumber(iterable dbNewIterable, key string) uint64 {
	iter := iterable.NewIterator(util.BytesPrefix([]byte(key)), nil)
	iter.Last()

	keySize := len(key)
	if len(iter.Key()) > keySize {
		lastSequenceNumber := bytesToUint64(iter.Key()[keySize:])
		return lastSequenceNumber + 1
	}

	return 0
}
