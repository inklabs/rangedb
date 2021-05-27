package postgresstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"strings"
	"time"

	"github.com/lib/pq"

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
	broadcastRecordBuffSize   = 100
	streamAdvisoryLockTimeout = 2 * time.Second
)

type batchSQLRecord struct {
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
	config            *Config
	db                *sql.DB
	clock             clock.Clock
	serializer        JsonSerializer
	broadcaster       broadcast.Broadcaster
	pgNotifyIsEnabled bool
}

// Option defines functional option parameters for postgresStore.
type Option func(*postgresStore)

// WithClock is a functional option to inject a clock.Clock.
func WithClock(clock clock.Clock) Option {
	return func(store *postgresStore) {
		store.clock = clock
	}
}

// WithPgNotify enables pg_notify() for notifying subscribers.
func WithPgNotify() Option {
	return func(store *postgresStore) {
		store.pgNotifyIsEnabled = true
	}
}

// New constructs an postgresStore.
func New(config *Config, options ...Option) (*postgresStore, error) {
	s := &postgresStore{
		config:      config,
		clock:       systemclock.New(),
		serializer:  jsonrecordserializer.New(),
		broadcaster: broadcast.New(broadcastRecordBuffSize, broadcast.DefaultTimeout),
	}

	for _, option := range options {
		option(s)
	}

	err := s.connectToDB()
	if err != nil {
		return nil, err
	}

	if s.pgNotifyIsEnabled {
		err = s.startPQListener()
		if err != nil {
			return nil, err
		}
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

func (s *postgresStore) Events(ctx context.Context, globalSequenceNumber uint64) rangedb.RecordIterator {
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
		_, _ = s.readResultRecords(ctx, rows, resultRecords)
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func ignoreClose(closer io.Closer) {
	_ = closer.Close()
}

func (s *postgresStore) EventsByAggregateTypes(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) rangedb.RecordIterator {
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
		_, _ = s.readResultRecords(ctx, rows, resultRecords)
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *postgresStore) EventsByStream(ctx context.Context, streamSequenceNumber uint64, streamName string) rangedb.RecordIterator {
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

		recordsRead, err := s.readResultRecords(ctx, rows, resultRecords)
		if err == nil && recordsRead == 0 {
			resultRecords <- rangedb.ResultRecord{Err: rangedb.ErrStreamNotFound}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *postgresStore) OptimisticDeleteStream(ctx context.Context, expectedStreamSequenceNumber uint64, streamName string) error {
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	aggregateType, aggregateID := rangedb.ParseStream(streamName)
	streamSequenceNumber, err := s.getStreamSequenceNumber(ctx, transaction, aggregateType, aggregateID)
	if err != nil {
		return err
	}

	if streamSequenceNumber == 0 {
		return rangedb.ErrStreamNotFound
	}

	if streamSequenceNumber != expectedStreamSequenceNumber {
		return &rangedberror.UnexpectedSequenceNumber{
			Expected:             expectedStreamSequenceNumber,
			ActualSequenceNumber: streamSequenceNumber,
		}
	}

	_, err = transaction.ExecContext(ctx, "DELETE FROM record WHERE AggregateType = $1 AND AggregateID = $2", aggregateType, aggregateID)
	if err != nil {
		_ = transaction.Rollback()
		return err
	}

	err = transaction.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *postgresStore) OptimisticSave(ctx context.Context, expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.transactionalSaveEvents(ctx, &expectedStreamSequenceNumber, eventRecords...)
}

func (s *postgresStore) Save(ctx context.Context, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.transactionalSaveEvents(ctx, nil, eventRecords...)
}

type saveResults struct {
	LastStreamSequenceNumber uint64
	GlobalSequenceNumbers    []uint64
	BatchRecords             []*batchSQLRecord
}

func (s *postgresStore) transactionalSaveEvents(ctx context.Context, expectedStreamSequenceNumber *uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}

	saveResult, err := s.saveEvents(ctx, transaction, expectedStreamSequenceNumber, eventRecords)
	if err != nil {
		_ = transaction.Rollback()
		return 0, err
	}

	err = transaction.Commit()
	if err != nil {
		return 0, err
	}

	if !s.pgNotifyIsEnabled {
		err = s.batchNotifySubscribers(saveResult)
	}

	return saveResult.LastStreamSequenceNumber, nil
}

func (s *postgresStore) saveEvents(ctx context.Context, transaction *sql.Tx, expectedStreamSequenceNumber *uint64, eventRecords []*rangedb.EventRecord) (*saveResults, error) {
	if len(eventRecords) < 1 {
		return nil, fmt.Errorf("missing events")
	}

	aggregateType := eventRecords[0].Event.AggregateType()
	aggregateID := eventRecords[0].Event.AggregateID()

	err := s.lockStream(ctx, transaction, aggregateID)
	if err != nil {
		return nil, err
	}

	streamSequenceNumber, err := s.validateStreamSequenceNumber(ctx, transaction, expectedStreamSequenceNumber, aggregateType, aggregateID)
	if err != nil {
		return nil, err
	}

	batchRecords, err := s.eventRecordsToBatchRecords(eventRecords, streamSequenceNumber)
	if err != nil {
		return nil, err
	}

	globalSequenceNumbers, lastStreamSequenceNumber, err := s.batchInsert(ctx, transaction, batchRecords)
	if err != nil {
		return nil, err
	}

	return &saveResults{
		LastStreamSequenceNumber: lastStreamSequenceNumber,
		GlobalSequenceNumbers:    globalSequenceNumbers,
		BatchRecords:             batchRecords,
	}, nil
}

func (s *postgresStore) AllEventsSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AllEventsConfig(ctx, s, s.broadcaster, bufferSize,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *postgresStore) AggregateTypesSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber, aggregateTypes ...string) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AggregateTypesConfig(ctx, s, s.broadcaster, bufferSize,
			aggregateTypes,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *postgresStore) TotalEventsInStream(ctx context.Context, streamName string) (uint64, error) {
	aggregateType, aggregateID := rangedb.ParseStream(streamName)
	return s.getStreamSequenceNumber(ctx, s.db, aggregateType, aggregateID)
}

func (s *postgresStore) validateStreamSequenceNumber(ctx context.Context, queryable dbRowQueryable, expectedStreamSequenceNumber *uint64, aggregateType string, aggregateID string) (uint64, error) {
	streamSequenceNumber, err := s.getStreamSequenceNumber(ctx, queryable, aggregateType, aggregateID)
	if err != nil {
		return 0, err
	}

	if expectedStreamSequenceNumber != nil && streamSequenceNumber != *expectedStreamSequenceNumber {
		return 0, &rangedberror.UnexpectedSequenceNumber{
			Expected:             *expectedStreamSequenceNumber,
			ActualSequenceNumber: streamSequenceNumber,
		}
	}

	return streamSequenceNumber, nil
}

func (s *postgresStore) batchNotifySubscribers(saveResult *saveResults) error {
	for i, batchRecord := range saveResult.BatchRecords {
		record := &rangedb.Record{
			AggregateType:        batchRecord.AggregateType,
			AggregateID:          batchRecord.AggregateID,
			GlobalSequenceNumber: saveResult.GlobalSequenceNumbers[i],
			StreamSequenceNumber: batchRecord.StreamSequenceNumber,
			EventType:            batchRecord.EventType,
			EventID:              batchRecord.EventID,
			InsertTimestamp:      batchRecord.InsertTimestamp,
			Data:                 batchRecord.Data,
			Metadata:             batchRecord.Metadata,
		}

		deSerializedData, err := jsonrecordserializer.DecodeJsonData(
			batchRecord.EventType,
			strings.NewReader(batchRecord.Data),
			s.serializer,
		)
		if err != nil {
			return fmt.Errorf("unable to decode data: %v", err)
		}

		var metadata interface{}
		if batchRecord.Metadata != "null" {
			err = json.Unmarshal([]byte(batchRecord.Metadata), metadata)
			if err != nil {
				return fmt.Errorf("unable to unmarshal metadata: %v", err)
			}
		}

		record.Data = deSerializedData
		record.Metadata = metadata
		s.broadcaster.Accept(record)
	}

	return nil
}

func (s *postgresStore) eventRecordsToBatchRecords(eventRecords []*rangedb.EventRecord, streamSequenceNumber uint64) ([]*batchSQLRecord, error) {
	aggregateType := eventRecords[0].Event.AggregateType()
	aggregateID := eventRecords[0].Event.AggregateID()

	var batchEvents []*batchSQLRecord

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

		streamSequenceNumber++
		batchEvents = append(batchEvents, &batchSQLRecord{
			AggregateType:        aggregateType,
			AggregateID:          aggregateID,
			StreamSequenceNumber: streamSequenceNumber,
			EventID:              eventID,
			EventType:            eventRecord.Event.EventType(),
			Data:                 string(jsonData),
			Metadata:             string(jsonMetadata),
			InsertTimestamp:      uint64(s.clock.Now().Unix()),
		})
	}

	return batchEvents, nil
}

func (s *postgresStore) batchInsert(ctx context.Context, transaction dbQueryable, batchRecords []*batchSQLRecord) ([]uint64, uint64, error) {
	valueStrings := make([]string, 0, len(batchRecords))
	valueArgs := make([]interface{}, 0, len(batchRecords)*8)
	var lastStreamSequenceNumber uint64

	i := 0
	for _, batchRecord := range batchRecords {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*8+1, i*8+2, i*8+3, i*8+4, i*8+5, i*8+6, i*8+7, i*8+8))
		valueArgs = append(valueArgs,
			batchRecord.AggregateType,
			batchRecord.AggregateID,
			batchRecord.StreamSequenceNumber,
			batchRecord.InsertTimestamp,
			batchRecord.EventID,
			batchRecord.EventType,
			batchRecord.Data,
			batchRecord.Metadata,
		)
		i++

		lastStreamSequenceNumber = batchRecord.StreamSequenceNumber
	}

	sqlStatement := fmt.Sprintf(
		"INSERT INTO record (AggregateType,AggregateID,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata) VALUES %s",
		strings.Join(valueStrings, ","))

	if !s.pgNotifyIsEnabled {
		sqlStatement += " RETURNING GlobalSequenceNumber"
	}

	rows, err := transaction.QueryContext(ctx, sqlStatement, valueArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to insert: %v", err)
	}
	defer ignoreClose(rows)

	var globalSequenceNumbers []uint64
	if !s.pgNotifyIsEnabled {
		for rows.Next() {
			var globalSequenceNumber uint64
			err := rows.Scan(&globalSequenceNumber)
			if err != nil {
				return nil, 0, fmt.Errorf("unable to get global sequence number: %v", err)
			}

			globalSequenceNumbers = append(globalSequenceNumbers, globalSequenceNumber)
		}
	}

	return globalSequenceNumbers, lastStreamSequenceNumber, nil
}

func (s *postgresStore) lockStream(ctx context.Context, transaction dbExecAble, aggregateID string) error {
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

func (s *postgresStore) InitDB() error {
	sqlStatements := []string{
		`CREATE TABLE IF NOT EXISTS record (
			AggregateType TEXT,
			AggregateID TEXT,
			GlobalSequenceNumber SERIAL PRIMARY KEY,
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

	if s.pgNotifyIsEnabled {
		sqlStatements = append(sqlStatements,
			`CREATE OR REPLACE FUNCTION rangedb_notify_record() RETURNS TRIGGER AS
		$$
			BEGIN
				PERFORM pg_notify('records', row_to_json(NEW)::text); 
				RETURN NULL; 
			END;
		$$ LANGUAGE plpgsql;`,
			`DROP TRIGGER IF EXISTS rangedb_trigger_notify_record ON record;`,
			`CREATE TRIGGER rangedb_trigger_notify_record AFTER INSERT ON record FOR EACH ROW EXECUTE PROCEDURE rangedb_notify_record();`,
		)
	}

	for _, statement := range sqlStatements {
		_, err := s.db.Exec(statement)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *postgresStore) getStreamSequenceNumber(ctx context.Context, queryable dbRowQueryable, aggregateType, aggregateID string) (uint64, error) {
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

	return lastStreamSequenceNumber, nil
}

func (s *postgresStore) readResultRecords(ctx context.Context, rows *sql.Rows, resultRecords chan rangedb.ResultRecord) (int, error) {
	recordsRead := 0
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
			return 0, err
		}

		data, err := jsonrecordserializer.DecodeJsonData(
			eventType,
			strings.NewReader(serializedData),
			s.serializer,
		)
		if err != nil {
			decodeErr := fmt.Errorf("unable to decode data: %v", err)
			resultRecords <- rangedb.ResultRecord{Err: decodeErr}
			return 0, decodeErr
		}

		var metadata interface{}
		if serializedMetadata != "null" {
			err = json.Unmarshal([]byte(serializedMetadata), metadata)
			if err != nil {
				unmarshalErr := fmt.Errorf("unable to unmarshal metadata: %v", err)
				resultRecords <- rangedb.ResultRecord{Err: unmarshalErr}
				return 0, unmarshalErr
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

		recordsRead++

		if !rangedb.PublishRecordOrCancel(ctx, resultRecords, record, time.Second) {
			return 0, fmt.Errorf("publish record was canceled")
		}
	}

	err := rows.Err()
	if err != nil {
		resultRecords <- rangedb.ResultRecord{Err: err}
		return 0, err
	}

	return recordsRead, nil
}

func (s *postgresStore) connectToDB() error {
	db, err := sql.Open("postgres", s.config.DataSourceName())
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

func (s *postgresStore) startPQListener() error {
	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	listener := pq.NewListener(s.config.DataSourceName(), 10*time.Second, time.Minute, reportProblem)
	err := listener.Listen("records")
	if err != nil {
		return err
	}

	go s.listen(listener)

	return nil
}

// PostgresJsonRecord holds a JSON record sent via pg_notify.
type PostgresJsonRecord struct {
	AggregateType        string `json:"aggregatetype"`
	AggregateID          string `json:"aggregateid"`
	GlobalSequenceNumber uint64 `json:"globalsequencenumber"`
	StreamSequenceNumber uint64 `json:"streamsequencenumber"`
	InsertTimestamp      uint64 `json:"inserttimestamp"`
	EventID              string `json:"eventid"`
	EventType            string `json:"eventtype"`
	Data                 string `json:"data"`
	Metadata             string `json:"metadata"`
}

func (s *postgresStore) listen(listener *pq.Listener) {
	for {
		select {
		case n := <-listener.Notify:
			var jsonRecord PostgresJsonRecord
			err := json.Unmarshal([]byte(n.Extra), &jsonRecord)
			if err != nil {
				log.Printf("invalid json request body: %v", err)
				return
			}

			data, err := jsonrecordserializer.DecodeJsonData(
				jsonRecord.EventType,
				strings.NewReader(jsonRecord.Data),
				s.serializer,
			)
			if err != nil {
				log.Printf("unable to decode data: %v", err)
				return
			}

			var metadata interface{}
			if jsonRecord.Metadata != "null" {
				err = json.Unmarshal([]byte(jsonRecord.Metadata), metadata)
				if err != nil {
					log.Printf("unable to unmarshal metadata: %v", err)
					return
				}
			}

			record := &rangedb.Record{
				AggregateType:        jsonRecord.AggregateType,
				AggregateID:          jsonRecord.AggregateID,
				GlobalSequenceNumber: jsonRecord.GlobalSequenceNumber,
				StreamSequenceNumber: jsonRecord.StreamSequenceNumber,
				InsertTimestamp:      jsonRecord.InsertTimestamp,
				EventID:              jsonRecord.EventID,
				EventType:            jsonRecord.EventType,
				Data:                 data,
				Metadata:             metadata,
			}

			s.broadcaster.Accept(record)

		case <-time.After(90 * time.Second):
			go func() {
				err := listener.Ping()
				if err != nil {
					log.Print(err)
				}
			}()
		}
	}
}

type dbQueryable interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type dbRowQueryable interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type dbExecAble interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
