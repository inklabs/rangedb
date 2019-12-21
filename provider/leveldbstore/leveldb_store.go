package leveldbstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

const (
	separator       = "!"
	allEventsPrefix = "$all$" + separator
)

type levelDbStore struct {
	clock       clock.Clock
	serializer  rangedb.RecordSerializer
	logger      *log.Logger
	db          *leveldb.DB
	subscribers []rangedb.RecordSubscriber
}

type Option func(*levelDbStore)

func WithClock(clock clock.Clock) Option {
	return func(store *levelDbStore) {
		store.clock = clock
	}
}

func WithSerializer(serializer rangedb.RecordSerializer) Option {
	return func(store *levelDbStore) {
		store.serializer = serializer
	}
}

func WithLogger(logger *log.Logger) Option {
	return func(store *levelDbStore) {
		store.logger = logger
	}
}

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

func (s *levelDbStore) AllEvents() <-chan *rangedb.Record {
	return s.getEventsByLookup(allEventsPrefix)
}

func (s *levelDbStore) EventsByStream(stream string) <-chan *rangedb.Record {
	return s.getEventsByPrefixStartingWith(stream, 0)
}

func (s *levelDbStore) EventsByStreamStartingWith(stream string, eventNumber uint64) <-chan *rangedb.Record {
	return s.getEventsByPrefixStartingWith(stream, eventNumber)
}

func (s *levelDbStore) EventsByAggregateType(aggregateType string) <-chan *rangedb.Record {
	return s.getEventsByLookup(getAggregateTypeKeyPrefix(aggregateType))
}

func (s *levelDbStore) EventsByAggregateTypes(aggregateTypes ...string) <-chan *rangedb.Record {
	channels := rangedb.GetEventsByAggregateTypes(s, aggregateTypes...)
	return rangedb.MergeRecordChannelsInOrder(channels)
}

func (s *levelDbStore) EventsByAggregateTypeStartingWith(aggregateType string, eventNumber uint64) <-chan *rangedb.Record {
	return s.getEventsByPrefixStartingWith(aggregateType, eventNumber)
}

func (s *levelDbStore) Save(event rangedb.Event, metadata interface{}) error {
	return s.SaveEvent(
		event.AggregateType(),
		event.AggregateId(),
		event.EventType(),
		shortuuid.New().String(),
		event,
		metadata,
	)
}

func (s *levelDbStore) SaveEvent(aggregateType, aggregateId, eventType, eventId string, event, metadata interface{}) error {
	stream := rangedb.GetStream(aggregateType, aggregateId)

	if eventId == "" {
		eventId = shortuuid.New().String()
	}

	record := &rangedb.Record{
		AggregateType:        aggregateType,
		AggregateId:          aggregateId,
		GlobalSequenceNumber: s.getNextGlobalSequenceNumber(),
		StreamSequenceNumber: s.getNextStreamSequenceNumber(stream),
		EventType:            eventType,
		EventId:              eventId,
		InsertTimestamp:      uint64(s.clock.Now().Unix()),
		Data:                 event,
		Metadata:             metadata,
	}

	batch := new(leveldb.Batch)
	data, err := s.serializer.Serialize(record)
	if err != nil {
		return err
	}

	streamKey := getKeyWithNumber(stream+separator, record.StreamSequenceNumber)
	batch.Put(streamKey, data)

	allAggregateTypeKey := getKeyWithNumber(getAggregateTypeKeyPrefix(aggregateType), record.GlobalSequenceNumber)
	batch.Put(allAggregateTypeKey, streamKey)

	allEventsKey := getKeyWithNumber(allEventsPrefix, record.GlobalSequenceNumber)
	batch.Put(allEventsKey, streamKey)

	err = s.db.Write(batch, nil)
	if err == nil {
		s.notifySubscribers(record)
	}

	return err
}

func (s *levelDbStore) Subscribe(subscribers ...rangedb.RecordSubscriber) {
	s.subscribers = append(s.subscribers, subscribers...)
}

func (s *levelDbStore) getEventsByPrefixStartingWith(prefix string, eventNumber uint64) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
		count := uint64(0)
		for iter.Next() {
			if count >= eventNumber {
				record, err := s.serializer.Deserialize(iter.Value())
				if err != nil {
					s.logger.Printf("failed to deserialize record for prefix (%v): %v", prefix, err)
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

func (s *levelDbStore) getEventsByLookup(key string) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		iter := s.db.NewIterator(util.BytesPrefix([]byte(key)), nil)
		for iter.Next() {
			targetKey := iter.Value()

			data, err := s.db.Get(targetKey, nil)
			if err != nil {
				s.logger.Printf("unable to find lookup record %s for %s: %v", targetKey, iter.Key(), err)
				continue
			}

			record, err := s.serializer.Deserialize(data)
			if err != nil {
				s.logger.Printf("failed to deserialize record %s for %s: %v", targetKey, iter.Key(), err)
			}

			records <- record
		}
		iter.Release()

		_ = iter.Error()
		close(records)
	}()

	return records
}

func (s *levelDbStore) notifySubscribers(record *rangedb.Record) {
	for _, subscriber := range s.subscribers {
		subscriber.Accept(record)
	}
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
