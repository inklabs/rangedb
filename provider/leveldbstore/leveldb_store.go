package leveldbstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/paging"
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

	mux sync.Mutex
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

func (s *levelDbStore) Bind(events ...rangedb.Event) {
	s.serializer.Bind(events...)
}

func (s *levelDbStore) AllEvents() <-chan *rangedb.Record {
	return s.getEventsByLookup(allEventsPrefix)
}

func (s *levelDbStore) AllEventsByAggregateType(aggregateType string) <-chan *rangedb.Record {
	return s.getEventsByLookup(getAggregateTypeKeyPrefix(aggregateType))
}

func (s *levelDbStore) AllEventsByAggregateTypes(aggregateTypes ...string) <-chan *rangedb.Record {
	channels := rangedb.GetAllEventsByAggregateTypes(s, aggregateTypes...)
	return rangedb.MergeRecordChannelsInOrder(channels)
}

func (s *levelDbStore) AllEventsByStream(stream string) <-chan *rangedb.Record {
	return s.getEventsByPrefixStartingWith(stream, 0)
}

func (s *levelDbStore) EventsByAggregateType(pagination paging.Pagination, aggregateType string) <-chan *rangedb.Record {
	return s.paginateEventsByLookup(pagination, getAggregateTypeKeyPrefix(aggregateType))
}

func (s *levelDbStore) EventsByAggregateTypeStartingWith(aggregateType string, eventNumber uint64) <-chan *rangedb.Record {
	return s.getEventsByPrefixStartingWith(aggregateType, eventNumber)
}

func (s *levelDbStore) EventsByStream(pagination paging.Pagination, streamName string) <-chan *rangedb.Record {
	return s.paginateEventsByPrefix(pagination, streamName)
}

func (s *levelDbStore) EventsByStreamStartingWith(stream string, eventNumber uint64) <-chan *rangedb.Record {
	return s.getEventsByPrefixStartingWith(stream, eventNumber)
}

func (s *levelDbStore) Save(event rangedb.Event, metadata interface{}) error {
	return s.SaveEvent(
		event.AggregateType(),
		event.AggregateID(),
		event.EventType(),
		shortuuid.New().String(),
		event,
		metadata,
	)
}

func (s *levelDbStore) SaveEvent(aggregateType, aggregateID, eventType, eventID string, event, metadata interface{}) error {
	s.mux.Lock()

	if eventID == "" {
		eventID = shortuuid.New().String()
	}

	stream := rangedb.GetStream(aggregateType, aggregateID)
	record := &rangedb.Record{
		AggregateType:        aggregateType,
		AggregateID:          aggregateID,
		GlobalSequenceNumber: s.getNextGlobalSequenceNumber(),
		StreamSequenceNumber: s.getNextStreamSequenceNumber(stream),
		EventType:            eventType,
		EventID:              eventID,
		InsertTimestamp:      uint64(s.clock.Now().Unix()),
		Data:                 event,
		Metadata:             metadata,
	}

	batch := new(leveldb.Batch)
	data, err := s.serializer.Serialize(record)
	if err != nil {
		s.mux.Unlock()
		return err
	}

	streamKey := getKeyWithNumber(stream+separator, record.StreamSequenceNumber)
	batch.Put(streamKey, data)

	allAggregateTypeKey := getKeyWithNumber(getAggregateTypeKeyPrefix(aggregateType), record.GlobalSequenceNumber)
	batch.Put(allAggregateTypeKey, streamKey)

	allEventsKey := getKeyWithNumber(allEventsPrefix, record.GlobalSequenceNumber)
	batch.Put(allEventsKey, streamKey)

	err = s.db.Write(batch, nil)
	s.mux.Unlock()

	if err == nil {
		deSerializedRecord, _ := s.serializer.Deserialize(data)
		s.notifySubscribers(deSerializedRecord)
	}

	return err
}

func (s *levelDbStore) SubscribeAndReplay(subscribers ...rangedb.RecordSubscriber) {
	rangedb.ReplayEvents(s, subscribers...)

	s.mux.Lock()
	s.Subscribe(subscribers...)
	defer s.mux.Unlock()
}

func (s *levelDbStore) Subscribe(subscribers ...rangedb.RecordSubscriber) {
	s.subscriberMux.Lock()
	defer s.subscriberMux.Unlock()

	s.subscribers = append(s.subscribers, subscribers...)
}

func (s *levelDbStore) TotalEventsInStream(streamName string) uint64 {
	return s.getNextStreamSequenceNumber(streamName)
}

func (s *levelDbStore) notifySubscribers(record *rangedb.Record) {
	s.subscriberMux.RLock()
	defer s.subscriberMux.RUnlock()

	for _, subscriber := range s.subscribers {
		subscriber.Accept(record)
	}
}

func (s *levelDbStore) getEventsByPrefixStartingWith(prefix string, eventNumber uint64) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
		count := uint64(0)
		for iter.Next() {
			if count >= eventNumber {
				record, err := s.getRecordByValue(iter.Value())
				if err != nil {
					continue
				}

				records <- record
			}
			count++
		}
		iter.Release()

		_ = iter.Error()
		close(records)
	}()

	return records
}

func (s *levelDbStore) paginateEventsByPrefix(pagination paging.Pagination, prefix string) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		firstEventNumber := (pagination.Page - 1) * pagination.ItemsPerPage
		count := 0

		iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
		for iter.Next() {
			count++
			if count <= firstEventNumber {
				continue
			}

			record, err := s.getRecordByValue(iter.Value())
			if err != nil {
				continue
			}

			records <- record

			if count-firstEventNumber >= pagination.ItemsPerPage {
				break
			}
		}
		iter.Release()

		_ = iter.Error()
		close(records)
	}()

	return records
}

func (s *levelDbStore) getEventsByLookup(key string) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		iter := s.db.NewIterator(util.BytesPrefix([]byte(key)), nil)
		for iter.Next() {
			targetKey := iter.Value()

			record, err := s.getRecordByLookup(targetKey, iter)
			if err != nil {
				continue
			}

			records <- record
		}
		iter.Release()

		_ = iter.Error()
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

func (s *levelDbStore) paginateEventsByLookup(pagination paging.Pagination, key string) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		firstEventNumber := (pagination.Page - 1) * pagination.ItemsPerPage
		count := 0

		iter := s.db.NewIterator(util.BytesPrefix([]byte(key)), nil)
		for iter.Next() {
			count++
			if count <= firstEventNumber {
				continue
			}

			targetKey := iter.Value()

			record, err := s.getRecordByLookup(targetKey, iter)
			if err != nil {
				continue
			}

			records <- record

			if count-firstEventNumber >= pagination.ItemsPerPage {
				break
			}
		}
		iter.Release()

		_ = iter.Error()
		close(records)
	}()

	return records
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

func (s *levelDbStore) getNextGlobalSequenceNumber() uint64 {
	return s.getNextSequenceNumber(allEventsPrefix)
}

func (s *levelDbStore) getNextStreamSequenceNumber(stream string) uint64 {
	return s.getNextSequenceNumber(stream + separator)
}

func (s *levelDbStore) getNextSequenceNumber(key string) uint64 {
	iter := s.db.NewIterator(util.BytesPrefix([]byte(key)), nil)
	iter.Last()

	keySize := len(key)
	if len(iter.Key()) > keySize {
		lastSequenceNumber := bytesToUint64(iter.Key()[keySize:])
		return lastSequenceNumber + 1
	}

	return 0
}
