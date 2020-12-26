package postgresstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/lib/pq"
	_ "github.com/lib/pq"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/rangedberror"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

type JsonSerializer interface {
	rangedb.EventBinder
	rangedb.EventTypeIdentifier
}

type postgresStore struct {
	clock      clock.Clock
	db         *sql.DB
	serializer JsonSerializer

	subscriberMux sync.RWMutex
	subscribers   []rangedb.RecordSubscriber
}

// Option defines functional option parameters for postgresStore.
type Option func(*postgresStore)

// WithClock is a functional option to inject a clock.Clock.
func WithClock(clock clock.Clock) Option {
	return func(store *postgresStore) {
		store.clock = clock
	}
}

// New constructs an postgresStore.
func New(config Config, options ...Option) (*postgresStore, error) {
	s := &postgresStore{
		clock:      systemclock.New(),
		serializer: jsonrecordserializer.New(),
	}

	err := s.connectToDB(config)
	if err != nil {
		return nil, err
	}
	s.initDB()

	for _, option := range options {
		option(s)
	}

	return s, nil
}

// CloseDB closes the postgres DB connection.
func (s *postgresStore) CloseDB() error {
	return s.db.Close()
}

func (s *postgresStore) Bind(events ...rangedb.Event) {
	s.serializer.Bind(events...)
}

func (s *postgresStore) EventsStartingWith(ctx context.Context, globalSequenceNumber uint64) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		rows, err := s.db.QueryContext(ctx, "SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE GlobalSequenceNumber > $1 ORDER BY GlobalSequenceNumber",
			globalSequenceNumber)
		if err != nil {
			panic(err) // TODO: test this error path
		}
		defer rows.Close()
		s.readRecords(rows, records)

		close(records)
	}()

	return records
}

func (s *postgresStore) EventsByAggregateTypesStartingWith(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		rows, err := s.db.QueryContext(ctx, "SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE AggregateType = ANY($1) AND GlobalSequenceNumber > $2 ORDER BY GlobalSequenceNumber",
			pq.Array(aggregateTypes), globalSequenceNumber)
		if err != nil {
			panic(err) // TODO: test this error path
		}
		defer rows.Close()
		s.readRecords(rows, records)

		close(records)
	}()

	return records
}

func (s *postgresStore) EventsByStreamStartingWith(ctx context.Context, streamSequenceNumber uint64, streamName string) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		aggregateType, aggregateID := rangedb.ParseStream(streamName)
		rows, err := s.db.QueryContext(ctx, "SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE AggregateType = $1 AND AggregateID = $2 AND StreamSequenceNumber >= $3 ORDER BY GlobalSequenceNumber",
			aggregateType, aggregateID, streamSequenceNumber)
		if err != nil {
			panic(err) // TODO: test this error path
		}
		defer rows.Close()
		s.readRecords(rows, records)

		close(records)
	}()

	return records
}

func (s *postgresStore) OptimisticSave(expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(&expectedStreamSequenceNumber, eventRecords...)
}

func (s *postgresStore) Save(eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(nil, eventRecords...)
}

//saveEvents persists one or more events inside a locked mutex, and notifies subscribers.
func (s *postgresStore) saveEvents(expectedStreamSequenceNumber *uint64, eventRecords ...*rangedb.EventRecord) error {
	transaction, err := s.db.Begin()
	if err != nil {
		return err
	}

	aggregateType := eventRecords[0].Event.AggregateType()
	aggregateID := eventRecords[0].Event.AggregateID()
	nextStreamSequenceNumber := s.getNextStreamSequenceNumber(transaction, aggregateType, aggregateID)

	if expectedStreamSequenceNumber != nil && nextStreamSequenceNumber != *expectedStreamSequenceNumber {
		_ = transaction.Rollback()
		return &rangedberror.UnexpectedSequenceNumber{
			Expected:           *expectedStreamSequenceNumber,
			NextSequenceNumber: nextStreamSequenceNumber,
		}
	}

	type batchEvent struct {
		StreamSequenceNumber uint64
		EventID              string
		EventType            string
		Data                 string
		Metadata             string
		InsertTimestamp      uint64
	}

	var batchEvents []*batchEvent

	cnt := uint64(0)
	for _, eventRecord := range eventRecords {
		if aggregateType != eventRecord.Event.AggregateType() {
			_ = transaction.Rollback()
			return fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != eventRecord.Event.AggregateID() {
			_ = transaction.Rollback()
			return fmt.Errorf("unmatched aggregate ID")
		}

		jsonData, err := json.Marshal(eventRecord.Event)
		if err != nil {
			_ = transaction.Rollback()
			return err
		}

		jsonMetadata, err := json.Marshal(eventRecord.Metadata)
		if err != nil {
			_ = transaction.Rollback()
			return err
		}

		eventID := shortuuid.New().String()

		batchEvents = append(batchEvents, &batchEvent{
			StreamSequenceNumber: nextStreamSequenceNumber + cnt,
			EventID:              eventID,
			EventType:            eventRecord.Event.EventType(),
			Data:                 string(jsonData),
			Metadata:             string(jsonMetadata),
			InsertTimestamp:      uint64(s.clock.Now().Unix()),
		})
		cnt++
	}

	valueStrings := make([]string, 0, len(eventRecords))
	valueArgs := make([]interface{}, 0, len(eventRecords)*8)

	i := 0
	for _, batchEvent := range batchEvents {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*8+1, i*8+2, i*8+3, i*8+4, i*8+5, i*8+6, i*8+7, i*8+8))
		valueArgs = append(valueArgs,
			aggregateType,
			aggregateID,
			batchEvent.StreamSequenceNumber,
			batchEvent.InsertTimestamp,
			batchEvent.EventID,
			batchEvent.EventType,
			batchEvent.Data,
			batchEvent.Metadata,
		)
		i++
	}

	sqlStatement := fmt.Sprintf(
		"INSERT INTO record (AggregateType,AggregateID,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata) VALUES %s RETURNING GlobalSequenceNumber;",
		strings.Join(valueStrings, ","))

	globalSequenceNumber := uint64(0)
	err = transaction.QueryRow(sqlStatement, valueArgs...).Scan(&globalSequenceNumber)
	if err != nil {
		err = transaction.Rollback()
		return err
	}

	err = transaction.Commit()
	if err != nil {
		return err
	}

	cnt = globalSequenceNumber - uint64(len(batchEvents))
	for _, batchEvent := range batchEvents {
		record := &rangedb.Record{
			AggregateType:        aggregateType,
			AggregateID:          aggregateID,
			GlobalSequenceNumber: cnt,
			StreamSequenceNumber: batchEvent.StreamSequenceNumber,
			EventType:            batchEvent.EventType,
			EventID:              batchEvent.EventID,
			InsertTimestamp:      batchEvent.InsertTimestamp,
			Data:                 batchEvent.Data,
			Metadata:             batchEvent.Metadata,
		}
		cnt++

		deSerializedData, err := jsonrecordserializer.DecodeJsonData(
			batchEvent.EventType,
			strings.NewReader(batchEvent.Data),
			s.serializer,
		)
		if err != nil {
			panic(err) // TODO: test this error path
		}

		var metadata interface{}
		if batchEvent.Metadata != "null" {
			err = json.Unmarshal([]byte(batchEvent.Metadata), metadata)
			if err != nil {
				panic(err) // TODO: test this error path
			}
		}

		record.Data = deSerializedData
		record.Metadata = metadata
		s.notifySubscribers(record)
	}

	return nil
}

func (s *postgresStore) Subscribe(subscribers ...rangedb.RecordSubscriber) {
	s.subscriberMux.Lock()
	s.subscribers = append(s.subscribers, subscribers...)
	s.subscriberMux.Unlock()
}

func (s *postgresStore) SubscribeStartingWith(ctx context.Context, globalSequenceNumber uint64, subscribers ...rangedb.RecordSubscriber) {
	rangedb.ReplayEvents(s, globalSequenceNumber, subscribers...)

	select {
	case <-ctx.Done():
		return
	default:
		s.Subscribe(subscribers...)
	}
}

func (s *postgresStore) TotalEventsInStream(streamName string) uint64 {
	aggregateType, aggregateID := rangedb.ParseStream(streamName)
	return s.getNextStreamSequenceNumber(s.db, aggregateType, aggregateID)
}

func (s *postgresStore) notifySubscribers(record *rangedb.Record) {
	s.subscriberMux.RLock()
	defer s.subscriberMux.RUnlock()

	for _, subscriber := range s.subscribers {
		subscriber.Accept(record)
	}
}

func (s *postgresStore) connectToDB(config Config) error {
	db, err := sql.Open("postgres", config.DataSourceName())
	if err != nil {
		return fmt.Errorf("unable to open DB connection: %v", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("unable to connect to DB: %v", err)
	}

	s.db = db

	return nil
}

func (s *postgresStore) initDB() {
	createSQL := `CREATE TABLE IF NOT EXISTS record (
		AggregateType TEXT,
		AggregateID TEXT,
		GlobalSequenceNumber BIGSERIAL PRIMARY KEY,
		StreamSequenceNumber BIGINT,
		InsertTimestamp BIGINT,
		EventID TEXT,
		EventType TEXT,
		Data TEXT,
		Metadata TEXT
	);`

	_, err := s.db.Exec(createSQL)
	if err != nil {
		panic(err) // TODO: test this error path
	}
}

type dbRowQueryable interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}

func (s *postgresStore) getNextStreamSequenceNumber(queryable dbRowQueryable, aggregateType string, aggregateID string) uint64 {
	var lastStreamSequenceNumber uint64

	err := queryable.QueryRow("SELECT MAX(StreamSequenceNumber) FROM record WHERE AggregateType = $1 AND AggregateID = $2 GROUP BY AggregateType, AggregateID",
		aggregateType,
		aggregateID,
	).Scan(&lastStreamSequenceNumber)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0
		}
		panic(err) // TODO: test this error path
	}

	return lastStreamSequenceNumber + 1
}

func (s *postgresStore) readRecords(rows *sql.Rows, records chan *rangedb.Record) {
	for rows.Next() {
		var (
			aggregateType        string
			aggregateID          string
			globalSequenceNumber uint64
			streamSequenceNumber uint64
			insertTimestamp      uint64
			eventID              string
			eventType            string
			serializedData       string
			serializedMetadata   string
		)
		err := rows.Scan(&aggregateType, &aggregateID, &globalSequenceNumber, &streamSequenceNumber, &insertTimestamp, &eventID, &eventType, &serializedData, &serializedMetadata)
		if err != nil {
			panic(err) // TODO: test this error path
		}

		data, err := jsonrecordserializer.DecodeJsonData(
			eventType,
			strings.NewReader(serializedData),
			s.serializer,
		)
		if err != nil {
			panic(err) // TODO: test this error path
		}

		var metadata interface{}
		if serializedMetadata != "null" {
			err = json.Unmarshal([]byte(serializedMetadata), metadata)
			if err != nil {
				panic(err) // TODO: test this error path
			}
		}

		record := &rangedb.Record{
			AggregateType:        aggregateType,
			AggregateID:          aggregateID,
			GlobalSequenceNumber: globalSequenceNumber - 1,
			StreamSequenceNumber: streamSequenceNumber,
			InsertTimestamp:      insertTimestamp,
			EventID:              eventID,
			EventType:            eventType,
			Data:                 data,
			Metadata:             metadata,
		}

		records <- record
	}

	err := rows.Err()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		panic(err) // TODO: test this error path
	}
}
