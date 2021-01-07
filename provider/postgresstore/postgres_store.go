package postgresstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/rangedberror"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

const streamAdvisoryLockTimeout = 2 * time.Second

type batchEvent struct {
	AggregateType        string
	AggregateID          string
	StreamSequenceNumber uint64
	EventID              string
	EventType            string
	Data                 string
	Metadata             string
	InsertTimestamp      uint64
}

// JsonSerializer defines the interface to bind events and identify event types.
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
func New(config *Config, options ...Option) (*postgresStore, error) {
	s := &postgresStore{
		clock:      systemclock.New(),
		serializer: jsonrecordserializer.New(),
	}

	err := s.connectToDB(config)
	if err != nil {
		return nil, err
	}
	err = s.initDB()
	if err != nil {
		return nil, err
	}

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

func (s *postgresStore) EventsStartingWith(ctx context.Context, globalSequenceNumber uint64) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		rows, err := s.db.QueryContext(ctx, "SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE GlobalSequenceNumber >= $1 ORDER BY GlobalSequenceNumber",
			globalSequenceNumber)
		if err != nil {
			resultRecords <- rangedb.ResultRecord{Err: err}
			return
		}
		defer ignoreClose(rows)
		s.readResultRecords(ctx, rows, resultRecords)
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func ignoreClose(closer io.Closer) {
	_ = closer.Close()
}

func (s *postgresStore) EventsByAggregateTypesStartingWith(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)
		rows, err := s.db.QueryContext(ctx, "SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE AggregateType = ANY($1) AND GlobalSequenceNumber >= $2 ORDER BY GlobalSequenceNumber",
			pq.Array(aggregateTypes), globalSequenceNumber)
		if err != nil {
			resultRecords <- rangedb.ResultRecord{Err: err}
			return
		}
		defer ignoreClose(rows)
		s.readResultRecords(ctx, rows, resultRecords)
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *postgresStore) EventsByStreamStartingWith(ctx context.Context, streamSequenceNumber uint64, streamName string) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)
		aggregateType, aggregateID := rangedb.ParseStream(streamName)
		rows, err := s.db.QueryContext(ctx, "SELECT AggregateType,AggregateID,GlobalSequenceNumber,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata FROM record WHERE AggregateType = $1 AND AggregateID = $2 AND StreamSequenceNumber >= $3 ORDER BY GlobalSequenceNumber",
			aggregateType, aggregateID, streamSequenceNumber)
		if err != nil {
			resultRecords <- rangedb.ResultRecord{Err: err}
			return
		}
		defer ignoreClose(rows)
		s.readResultRecords(ctx, rows, resultRecords)
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *postgresStore) OptimisticSave(ctx context.Context, expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(ctx, &expectedStreamSequenceNumber, eventRecords...)
}

func (s *postgresStore) Save(ctx context.Context, eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(ctx, nil, eventRecords...)
}

// saveEvents persists one or more events inside a locked mutex, and notifies subscribers.
func (s *postgresStore) saveEvents(ctx context.Context, expectedStreamSequenceNumber *uint64, eventRecords ...*rangedb.EventRecord) error {
	if len(eventRecords) < 1 {
		return fmt.Errorf("missing events")
	}

	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	aggregateType := eventRecords[0].Event.AggregateType()
	aggregateID := eventRecords[0].Event.AggregateID()

	err = s.lockStream(ctx, transaction, aggregateID)
	if err != nil {
		return err
	}

	nextStreamSequenceNumber, err := s.validateNextStreamSequenceNumber(ctx, transaction, expectedStreamSequenceNumber, aggregateType, aggregateID)
	if err != nil {
		_ = transaction.Rollback()
		return err
	}

	batchEvents, err := s.eventRecordsToBatchEvents(eventRecords, nextStreamSequenceNumber)
	if err != nil {
		_ = transaction.Rollback()
		return err
	}

	globalSequenceNumbers, err := s.batchInsert(ctx, transaction, batchEvents)
	if err != nil {
		_ = transaction.Rollback()
		return err
	}

	err = transaction.Commit()
	if err != nil {
		return err
	}

	return s.batchNotifySubscribers(batchEvents, globalSequenceNumbers)
}

func (s *postgresStore) SubscribeStartingWith(ctx context.Context, globalSequenceNumber uint64, subscribers ...rangedb.RecordSubscriber) error {
	rangedb.ReplayEvents(ctx, s, globalSequenceNumber, subscribers...)
	return s.Subscribe(ctx, subscribers...)
}

func (s *postgresStore) Subscribe(ctx context.Context, subscribers ...rangedb.RecordSubscriber) error {
	select {
	case <-ctx.Done():
		return context.Canceled

	default:
	}

	s.subscriberMux.Lock()
	s.subscribers = append(s.subscribers, subscribers...)
	s.subscriberMux.Unlock()

	return nil
}

func (s *postgresStore) TotalEventsInStream(ctx context.Context, streamName string) (uint64, error) {
	aggregateType, aggregateID := rangedb.ParseStream(streamName)
	return s.getNextStreamSequenceNumber(ctx, s.db, aggregateType, aggregateID)
}

func (s *postgresStore) validateNextStreamSequenceNumber(ctx context.Context, transaction *sql.Tx, expectedStreamSequenceNumber *uint64, aggregateType string, aggregateID string) (uint64, error) {
	nextStreamSequenceNumber, err := s.getNextStreamSequenceNumber(ctx, transaction, aggregateType, aggregateID)
	if err != nil {
		return 0, err
	}

	if expectedStreamSequenceNumber != nil && nextStreamSequenceNumber != *expectedStreamSequenceNumber {
		return 0, &rangedberror.UnexpectedSequenceNumber{
			Expected:           *expectedStreamSequenceNumber,
			NextSequenceNumber: nextStreamSequenceNumber,
		}
	}

	return nextStreamSequenceNumber, nil
}

func (s *postgresStore) batchNotifySubscribers(batchEvents []*batchEvent, globalSequenceNumbers []uint64) error {
	for i, batchEvent := range batchEvents {
		record := &rangedb.Record{
			AggregateType:        batchEvent.AggregateType,
			AggregateID:          batchEvent.AggregateID,
			GlobalSequenceNumber: globalSequenceNumbers[i],
			StreamSequenceNumber: batchEvent.StreamSequenceNumber,
			EventType:            batchEvent.EventType,
			EventID:              batchEvent.EventID,
			InsertTimestamp:      batchEvent.InsertTimestamp,
			Data:                 batchEvent.Data,
			Metadata:             batchEvent.Metadata,
		}

		deSerializedData, err := jsonrecordserializer.DecodeJsonData(
			batchEvent.EventType,
			strings.NewReader(batchEvent.Data),
			s.serializer,
		)
		if err != nil {
			return fmt.Errorf("unable to decode data: %v", err)
		}

		var metadata interface{}
		if batchEvent.Metadata != "null" {
			err = json.Unmarshal([]byte(batchEvent.Metadata), metadata)
			if err != nil {
				return fmt.Errorf("unable to unmarshal metadata: %v", err)
			}
		}

		record.Data = deSerializedData
		record.Metadata = metadata
		s.notifySubscribers(record)
	}

	return nil
}

func (s *postgresStore) eventRecordsToBatchEvents(eventRecords []*rangedb.EventRecord, nextStreamSequenceNumber uint64) ([]*batchEvent, error) {
	aggregateType := eventRecords[0].Event.AggregateType()
	aggregateID := eventRecords[0].Event.AggregateID()

	var batchEvents []*batchEvent

	cnt := uint64(0)
	for _, eventRecord := range eventRecords {
		if aggregateType != eventRecord.Event.AggregateType() {
			return nil, fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != eventRecord.Event.AggregateID() {
			return nil, fmt.Errorf("unmatched aggregate ID")
		}

		jsonData, err := json.Marshal(eventRecord.Event)
		if err != nil {
			return nil, err
		}

		jsonMetadata, err := json.Marshal(eventRecord.Metadata)
		if err != nil {
			return nil, err
		}

		eventID := shortuuid.New().String()

		batchEvents = append(batchEvents, &batchEvent{
			AggregateType:        aggregateType,
			AggregateID:          aggregateID,
			StreamSequenceNumber: nextStreamSequenceNumber + cnt,
			EventID:              eventID,
			EventType:            eventRecord.Event.EventType(),
			Data:                 string(jsonData),
			Metadata:             string(jsonMetadata),
			InsertTimestamp:      uint64(s.clock.Now().Unix()),
		})
		cnt++
	}

	return batchEvents, nil
}

func (s *postgresStore) batchInsert(ctx context.Context, transaction DBQueryable, batchEvents []*batchEvent) ([]uint64, error) {
	valueStrings := make([]string, 0, len(batchEvents))
	valueArgs := make([]interface{}, 0, len(batchEvents)*8)

	i := 0
	for _, batchEvent := range batchEvents {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*8+1, i*8+2, i*8+3, i*8+4, i*8+5, i*8+6, i*8+7, i*8+8))
		valueArgs = append(valueArgs,
			batchEvent.AggregateType,
			batchEvent.AggregateID,
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

	rows, err := transaction.QueryContext(ctx, sqlStatement, valueArgs...)
	if err != nil {
		return nil, fmt.Errorf("unable to insert: %v", err)
	}

	globalSequenceNumbers := make([]uint64, 0)
	for rows.Next() {
		var globalSequenceNumber uint64
		err := rows.Scan(&globalSequenceNumber)
		if err != nil {
			return nil, fmt.Errorf("unable to get global sequence number: %v", err)
		}

		globalSequenceNumbers = append(globalSequenceNumbers, globalSequenceNumber)
	}

	return globalSequenceNumbers, nil
}

func (s *postgresStore) lockStream(ctx context.Context, transaction DBExecAble, aggregateID string) error {
	lockCtx, cancel := context.WithTimeout(ctx, streamAdvisoryLockTimeout)
	defer cancel()

	h := fnv.New32a()
	_, _ = h.Write([]byte(aggregateID))
	lockKey := h.Sum32()

	_, err := transaction.ExecContext(lockCtx, "SELECT pg_advisory_xact_lock($1)", lockKey)
	if err != nil {
		return fmt.Errorf("unable to obtain lock: %v", err)
	}
	return nil
}

func (s *postgresStore) notifySubscribers(record *rangedb.Record) {
	s.subscriberMux.RLock()
	defer s.subscriberMux.RUnlock()

	for _, subscriber := range s.subscribers {
		subscriber.Accept(record)
	}
}

func (s *postgresStore) connectToDB(config *Config) error {
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

func (s *postgresStore) initDB() error {
	sqlStatements := []string{
		`CREATE SEQUENCE IF NOT EXISTS global_sequence_number INCREMENT 1 MINVALUE 0 START 0;`,
		`CREATE TABLE IF NOT EXISTS record (
		AggregateType TEXT,
		AggregateID TEXT,
		GlobalSequenceNumber BIGINT DEFAULT NEXTVAL('global_sequence_number') PRIMARY KEY,
		StreamSequenceNumber BIGINT,
		InsertTimestamp BIGINT,
		EventID TEXT,
		EventType TEXT,
		Data TEXT,
		Metadata TEXT
	);`,
		`CREATE INDEX IF NOT EXISTS record_idx_aggregate_type ON record USING HASH (
		AggregateType
	);`,
		`CREATE INDEX IF NOT EXISTS record_idx_aggregate_id ON record USING HASH (
		AggregateID
	);`,
	}

	for _, statement := range sqlStatements {
		_, err := s.db.Exec(statement)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *postgresStore) getNextStreamSequenceNumber(ctx context.Context, queryable dbRowQueryable, aggregateType, aggregateID string) (uint64, error) {
	var lastStreamSequenceNumber uint64

	err := queryable.QueryRowContext(ctx, "SELECT MAX(StreamSequenceNumber) FROM record WHERE AggregateType = $1 AND AggregateID = $2 GROUP BY AggregateType, AggregateID",
		aggregateType,
		aggregateID,
	).Scan(&lastStreamSequenceNumber)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}

		return 0, err
	}

	return lastStreamSequenceNumber + 1, nil
}

func (s *postgresStore) readResultRecords(ctx context.Context, rows *sql.Rows, resultRecords chan rangedb.ResultRecord) {
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
			resultRecords <- rangedb.ResultRecord{Err: err}
			return
		}

		data, err := jsonrecordserializer.DecodeJsonData(
			eventType,
			strings.NewReader(serializedData),
			s.serializer,
		)
		if err != nil {
			resultRecords <- rangedb.ResultRecord{Err: fmt.Errorf("unable to decode data: %v", err)}
			return
		}

		var metadata interface{}
		if serializedMetadata != "null" {
			err = json.Unmarshal([]byte(serializedMetadata), metadata)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{Err: fmt.Errorf("unable to unmarshal metadata: %v", err)}
				return
			}
		}

		record := &rangedb.Record{
			AggregateType:        aggregateType,
			AggregateID:          aggregateID,
			GlobalSequenceNumber: globalSequenceNumber,
			StreamSequenceNumber: streamSequenceNumber,
			InsertTimestamp:      insertTimestamp,
			EventID:              eventID,
			EventType:            eventType,
			Data:                 data,
			Metadata:             metadata,
		}

		if !rangedb.PublishRecordOrCancel(ctx, resultRecords, record, time.Second) {
			return
		}
	}

	err := rows.Err()
	if err != nil {
		resultRecords <- rangedb.ResultRecord{Err: err}
		return
	}
}

type DBQueryable interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type dbRowQueryable interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type DBExecAble interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
