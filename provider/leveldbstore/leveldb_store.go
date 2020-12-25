package leveldbstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/rangedberror"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

const (
	separator       = "!"
	allEventsPrefix = "$all$" + separator
)

type levelDbStore struct {
	clock      clock.Clock
	serializer rangedb.RecordSerializer
	logger     *log.Logger

	subscriberMux sync.RWMutex
	subscribers   []rangedb.RecordSubscriber

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
		clock:      systemclock.New(),
		serializer: jsonrecordserializer.New(),
		logger:     log.New(ioutil.Discard, "", 0),
		db:         db,
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

func (s *levelDbStore) EventsStartingWith(ctx context.Context, globalSequenceNumber uint64) <-chan *rangedb.Record {
	return s.getEventsByLookup(ctx, allEventsPrefix, globalSequenceNumber)
}

func (s *levelDbStore) EventsByAggregateTypesStartingWith(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) <-chan *rangedb.Record {
	if len(aggregateTypes) == 1 {
		return s.getEventsByLookup(ctx, getAggregateTypeKeyPrefix(aggregateTypes[0]), globalSequenceNumber)
	}

	var channels []<-chan *rangedb.Record
	for _, aggregateType := range aggregateTypes {
		channels = append(channels, s.getEventsByLookup(ctx, getAggregateTypeKeyPrefix(aggregateType), globalSequenceNumber))
	}

	return rangedb.MergeRecordChannelsInOrder(channels)
}

func (s *levelDbStore) EventsByStreamStartingWith(ctx context.Context, streamSequenceNumber uint64, stream string) <-chan *rangedb.Record {
	return s.getEventsByPrefixStartingWith(ctx, stream, streamSequenceNumber)
}

func (s *levelDbStore) OptimisticSave(expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(&expectedStreamSequenceNumber, eventRecords...)
}

func (s *levelDbStore) Save(eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(nil, eventRecords...)
}

func (s *levelDbStore) saveEvents(expectedStreamSequenceNumber *uint64, eventRecords ...*rangedb.EventRecord) error {
	nextExpectedStreamSequenceNumber := expectedStreamSequenceNumber

	var pendingEventsData [][]byte
	var aggregateType, aggregateID string

	s.mux.Lock()
	transaction, err := s.db.OpenTransaction()
	if err != nil {
		return err
	}

	for _, eventRecord := range eventRecords {
		if aggregateType != "" && aggregateType != eventRecord.Event.AggregateType() {
			transaction.Discard()
			s.mux.Unlock()
			return fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != "" && aggregateID != eventRecord.Event.AggregateID() {
			transaction.Discard()
			s.mux.Unlock()
			return fmt.Errorf("unmatched aggregate ID")
		}

		aggregateType = eventRecord.Event.AggregateType()
		aggregateID = eventRecord.Event.AggregateID()

		data, err := s.saveEvent(
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
			return err
		}

		pendingEventsData = append(pendingEventsData, data)

		if nextExpectedStreamSequenceNumber != nil {
			*nextExpectedStreamSequenceNumber++
		}
	}
	err = transaction.Commit()
	if err != nil {
		return err
	}
	s.mux.Unlock()

	for _, data := range pendingEventsData {
		deSerializedRecord, _ := s.serializer.Deserialize(data)
		s.notifySubscribers(deSerializedRecord)
	}

	return nil
}

//saveEvent persists a single event without locking the mutex, or notifying subscribers.
func (s *levelDbStore) saveEvent(transaction *leveldb.Transaction,
	aggregateType, aggregateID, eventType, eventID string,
	expectedStreamSequenceNumber *uint64,
	event, metadata interface{}) ([]byte, error) {

	stream := rangedb.GetStream(aggregateType, aggregateID)
	nextSequenceNumber := s.getNextStreamSequenceNumber(transaction, stream)

	if expectedStreamSequenceNumber != nil && *expectedStreamSequenceNumber != nextSequenceNumber {
		return nil, &rangedberror.UnexpectedSequenceNumber{
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
		return nil, err
	}

	streamKey := getKeyWithNumber(stream+separator, record.StreamSequenceNumber)
	batch.Put(streamKey, data)

	allAggregateTypeKey := getKeyWithNumber(getAggregateTypeKeyPrefix(aggregateType), record.GlobalSequenceNumber)
	batch.Put(allAggregateTypeKey, streamKey)

	allEventsKey := getKeyWithNumber(allEventsPrefix, record.GlobalSequenceNumber)
	batch.Put(allEventsKey, streamKey)

	err = transaction.Write(batch, nil)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *levelDbStore) SubscribeStartingWith(ctx context.Context, globalSequenceNumber uint64, subscribers ...rangedb.RecordSubscriber) {
	rangedb.ReplayEvents(s, globalSequenceNumber, subscribers...)

	select {
	case <-ctx.Done():
		return
	default:
		s.Subscribe(subscribers...)
	}
}

func (s *levelDbStore) Subscribe(subscribers ...rangedb.RecordSubscriber) {
	s.subscriberMux.Lock()
	s.subscribers = append(s.subscribers, subscribers...)
	s.subscriberMux.Unlock()
}

func (s *levelDbStore) TotalEventsInStream(streamName string) uint64 {
	return s.getNextStreamSequenceNumber(s.db, streamName)
}

func (s *levelDbStore) notifySubscribers(record *rangedb.Record) {
	s.subscriberMux.RLock()
	defer s.subscriberMux.RUnlock()

	for _, subscriber := range s.subscribers {
		subscriber.Accept(record)
	}
}

func (s *levelDbStore) getEventsByPrefixStartingWith(ctx context.Context, prefix string, streamSequenceNumber uint64) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)
	s.mux.RLock()

	go func() {
		defer s.mux.RUnlock()

		iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
		for iter.Next() {
			record, err := s.getRecordByValue(iter.Value())
			if err != nil {
				continue
			}

			if record.StreamSequenceNumber >= streamSequenceNumber {
				select {
				case <-ctx.Done():
					break
				case records <- record:
				}
			}
		}
		iter.Release()

		_ = iter.Error()
		close(records)
	}()

	return records
}

func (s *levelDbStore) getEventsByLookup(ctx context.Context, key string, globalSequenceNumber uint64) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)
	s.mux.RLock()

	go func() {
		defer s.mux.RUnlock()

		iter := s.db.NewIterator(util.BytesPrefix([]byte(key)), nil)

		for iter.Next() {
			targetKey := iter.Value()

			record, err := s.getRecordByLookup(targetKey, iter)
			if err != nil {
				continue
			}

			if record.GlobalSequenceNumber >= globalSequenceNumber {
				select {
				case <-ctx.Done():
					break
				case records <- record:
				}
			}
		}
		iter.Release()
		close(records)
	}()

	return records
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
