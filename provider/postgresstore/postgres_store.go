package postgresstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/lib/pq"
	_ "github.com/lib/pq"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/errors"
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

func (s *postgresStore) StreamsStartingWith(ctx context.Context, globalSequenceNumber uint64) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		rows, err := s.db.Query("SELECT DISTINCT(AggregateID)FROM record WHERE GlobalSequenceNumber >= $1 ORDER BY GlobalSequenceNumber",
			globalSequenceNumber)
		if err != nil {
			panic(err) // TODO: test this error path
		}
		defer rows.Close()
		s.readRecords(ctx, rows, records)

		close(records)
	}()

	return records
}

func (s *postgresStore) EventsStartingWith(ctx context.Context, globalSequenceNumber uint64) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		rows, err := s.db.Query("SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE globalSequenceNumber >= $1 ORDER BY GlobalSequenceNumber",
			int64(globalSequenceNumber))
		if err != nil {
			panic(err) // TODO: test this error path
		}
		defer rows.Close()
		s.readRecords(ctx, rows, records)

		close(records)
	}()

	return records
}

func (s *postgresStore) EventsByAggregateTypesStartingWith(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		rows, err := s.db.Query("SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE AggregateType = ANY($1) AND GlobalSequenceNumber >= $2 ORDER BY GlobalSequenceNumber, StreamSequenceNumber",
			pq.Array(aggregateTypes), int64(globalSequenceNumber))
		if err != nil {
			panic(err) // TODO: test this error path
		}
		defer rows.Close()
		s.readRecords(ctx, rows, records)

		close(records)
	}()

	return records
}

func (s *postgresStore) EventsByStreamStartingWith(ctx context.Context, eventNumber uint64, streamName string) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		aggregateType, aggregateID := rangedb.ParseStream(streamName)
		fmt.Printf("SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE AggregateType = %s AND AggregateID = %s AND StreamSequenceNumber >= %d ORDER BY GlobalSequenceNumber\n",
			aggregateType, aggregateID, int64(eventNumber))
		rows, err := s.db.Query("SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE AggregateType = $1 AND AggregateID = $2 AND StreamSequenceNumber >= $3 ORDER BY GlobalSequenceNumber",
			aggregateType, aggregateID, int64(eventNumber))
		if err != nil {
			panic(err) // TODO: test this error path
		}
		defer rows.Close()
		s.readRecords(ctx, rows, records)
		close(records)
	}()

	return records
}

func (s *postgresStore) Save(event rangedb.Event, expectedStreamSequenceNumber *uint64, metadata interface{}) error {
	return s.SaveEvent(
		event.AggregateType(),
		event.AggregateID(),
		event.EventType(),
		shortuuid.New().String(),
		expectedStreamSequenceNumber,
		event,
		metadata,
	)
}

func (s *postgresStore) SaveEvent(aggregateType, aggregateID, eventType, eventID string, expectedStreamSequenceNumber *uint64, event, metadata interface{}) error {
	if eventID == "" {
		eventID = shortuuid.New().String()
	}

	streamSequenceNumber := s.getNextStreamSequenceNumber(aggregateType, aggregateID)

	if expectedStreamSequenceNumber != nil && streamSequenceNumber != *expectedStreamSequenceNumber {
		return errors.ErrUnexpectedVersionError{
			ExpectedVersion: streamSequenceNumber,
			EventVersion:    *expectedStreamSequenceNumber,
		}
	}

	jsonData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	jsonMetadata, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	sqlStatement := `INSERT INTO record (AggregateType,AggregateID,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING GlobalSequenceNumber;`
	globalSequenceNumber := uint64(0)
	insertTimestamp := uint64(s.clock.Now().Unix())
	err = s.db.QueryRow(sqlStatement,
		aggregateType,
		aggregateID,
		streamSequenceNumber,
		insertTimestamp,
		eventID,
		eventType,
		string(jsonData),
		string(jsonMetadata),
	).Scan(&globalSequenceNumber)
	if err != nil {
		return err
	}

	record := &rangedb.Record{
		AggregateType:        aggregateType,
		AggregateID:          aggregateID,
		GlobalSequenceNumber: globalSequenceNumber,
		StreamSequenceNumber: streamSequenceNumber,
		EventType:            eventType,
		EventID:              eventID,
		InsertTimestamp:      insertTimestamp,
		Metadata:             metadata,
	}

	deSerializedData, _ := jsonrecordserializer.DecodeJsonData(eventType, bytes.NewReader(jsonData), s.serializer)
	record.Data = deSerializedData
	s.notifySubscribers(record)

	return nil
}

func (s *postgresStore) Subscribe(subscribers ...rangedb.RecordSubscriber) {
	s.subscriberMux.Lock()
	s.subscribers = append(s.subscribers, subscribers...)
	s.subscriberMux.Unlock()
}

func (s *postgresStore) SubscribeStartingWith(ctx context.Context, eventNumber uint64, subscribers ...rangedb.RecordSubscriber) {
	rangedb.ReplayEvents(s, eventNumber, subscribers...)

	select {
	case <-ctx.Done():
		return
	default:
		s.Subscribe(subscribers...)
	}
}

func (s *postgresStore) TotalEventsInStream(streamName string) uint64 {
	return s.getNextStreamSequenceNumber(rangedb.ParseStream(streamName))
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

func (s *postgresStore) getNextStreamSequenceNumber(aggregateType string, aggregateID string) uint64 {
	var lastStreamSequenceNumber uint64
	err := s.db.QueryRow("SELECT MAX(StreamSequenceNumber) FROM record WHERE AggregateType = $1 AND AggregateID = $2 GROUP BY AggregateType, AggregateID",
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

func (s *postgresStore) readRecords(ctx context.Context, rows *sql.Rows, records chan *rangedb.Record) {
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
			err = json.Unmarshal([]byte(serializedMetadata), &metadata)
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
		panic(err) // TODO: test this error path
	}
}
