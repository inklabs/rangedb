package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	esclient "github.com/EventStore/EventStore-Client-Go/client"
	clienterrors "github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/streamrevision"
	"github.com/gofrs/uuid"

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
	rpcErrContextCanceled             = "Canceled desc = context canceled"
	rpcErrEventStreamIsDeleted        = " is deleted."
	streamNotFound                    = "Failed to perform read because the stream was not found"
	rpcErrWrongExpectedStreamRevision = "WrongExpectedStreamRevision"
	broadcastRecordBuffSize           = 100
)

type StreamPrefixer interface {
	WithPrefix(name string) string
	GetPrefix() string
}

type eventStore struct {
	client         *esclient.Client
	clock          clock.Clock
	streamPrefixer StreamPrefixer
	serializer     rangedb.RecordSerializer
	uuidGenerator  shortuuid.Generator
	broadcaster    broadcast.Broadcaster
	ipAddr         string
	username       string
	password       string

	sync           sync.RWMutex
	savedStreams   map[string]struct{}
	deletedStreams map[string]struct{}
}

// Option defines functional option parameters for eventStore.
type Option func(*eventStore)

// WithClock is a function option to inject a clock.Clock
func WithClock(clock clock.Clock) Option {
	return func(store *eventStore) {
		store.clock = clock
	}
}

// WithUUIDGenerator is a functional option to inject a shortuuid.Generator.
func WithUUIDGenerator(uuidGenerator shortuuid.Generator) Option {
	return func(store *eventStore) {
		store.uuidGenerator = uuidGenerator
	}
}

// WithStreamPrefix is a functional option to inject a stream prefix.
func WithStreamPrefix(streamPrefixer StreamPrefixer) Option {
	return func(store *eventStore) {
		store.streamPrefixer = streamPrefixer
	}
}

// WithSerializer is a functional option to inject a rangedb.RecordSerializer.
func WithSerializer(serializer rangedb.RecordSerializer) Option {
	return func(store *eventStore) {
		store.serializer = serializer
	}
}

// New constructs an eventStore. Experimental: Use at your own risk.
func New(ipAddr, username, password string, options ...Option) (*eventStore, error) {
	s := &eventStore{
		clock:          systemclock.New(),
		serializer:     jsonrecordserializer.New(),
		broadcaster:    broadcast.New(broadcastRecordBuffSize, broadcast.DefaultTimeout),
		savedStreams:   make(map[string]struct{}),
		deletedStreams: make(map[string]struct{}),
		ipAddr:         ipAddr,
		username:       username,
		password:       password,
	}

	for _, option := range options {
		option(s)
	}

	err := s.setupClient()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *eventStore) Close() error {
	return s.client.Close()
}

func (s *eventStore) setupClient() error {
	config, err := esclient.ParseConnectionString(fmt.Sprintf("esdb://%s:%s@%s", s.username, s.password, s.ipAddr))
	if err != nil {
		return fmt.Errorf("unexpected configuration error: %s", err.Error())
	}

	config.DisableTLS = true
	config.SkipCertificateVerification = true
	client, err := esclient.NewClient(config)
	if err != nil {
		return fmt.Errorf("unable to create client: %s", err.Error())
	}

	s.client = client

	return nil
}

func (s *eventStore) streamName(name string) string {
	if s.streamPrefixer == nil {
		return name
	}

	return s.streamPrefixer.WithPrefix(name)
}

func (s *eventStore) Bind(events ...rangedb.Event) {
	s.serializer.Bind(events...)
}

func (s *eventStore) Events(ctx context.Context, globalSequenceNumber uint64) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		readStream, err := s.client.ReadAllEvents(ctx, esclient.ReadAllEventsOptions{}, ^uint64(0))
		if err != nil {
			if strings.Contains(err.Error(), rpcErrContextCanceled) {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return
			}

			if strings.Contains(err.Error(), streamNotFound) {
				return
			}

			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("unexpected failure ReadStreamEvents: %v", err),
			}
			return
		}

		for {
			resolvedEvent, err := readStream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}

				if strings.Contains(err.Error(), rpcErrContextCanceled) {
					resultRecords <- rangedb.ResultRecord{
						Record: nil,
						Err:    context.Canceled,
					}
					return
				}

				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to receive event: %v", err),
				}
				return
			}

			if !s.inCurrentVersion(resolvedEvent) {
				continue
			}

			record, err := s.serializer.Deserialize(resolvedEvent.Event.Data)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to deserialize: %v", err),
				}
				return
			}

			select {
			case <-ctx.Done():
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return

			default:
				if record.GlobalSequenceNumber < globalSequenceNumber {
					continue
				}

				resultRecords <- rangedb.ResultRecord{
					Record: record,
					Err:    nil,
				}
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *eventStore) EventsByAggregateTypes(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)
		aggregateTypesMap := make(map[string]struct{})

		for _, aggregateType := range aggregateTypes {
			aggregateTypesMap[aggregateType] = struct{}{}
		}

		readStream, err := s.client.ReadAllEvents(ctx, esclient.ReadAllEventsOptions{}, ^uint64(0))
		if err != nil {
			if strings.Contains(err.Error(), rpcErrContextCanceled) {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return
			}

			if strings.Contains(err.Error(), streamNotFound) {
				return
			}

			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("unexpected failure ReadStreamEvents: %v", err),
			}
			return
		}
		for {
			resolvedEvent, err := readStream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}

				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to receive event: %v", err),
				}
				return
			}

			if !s.inCurrentVersion(resolvedEvent) {
				continue
			}

			record, err := s.serializer.Deserialize(resolvedEvent.Event.Data)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to deserialize: %v", err),
				}
				return
			}

			select {
			case <-ctx.Done():
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return

			default:
				if _, ok := aggregateTypesMap[record.AggregateType]; !ok {
					continue
				}

				if record.GlobalSequenceNumber < globalSequenceNumber {
					continue
				}

				resultRecords <- rangedb.ResultRecord{
					Record: record,
					Err:    nil,
				}
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *eventStore) EventsByStream(ctx context.Context, streamSequenceNumber uint64, streamName string) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		readStreamOptions := esclient.ReadStreamEventsOptions{}
		readStreamOptions.SetFromRevision(zeroBasedStreamSequenceNumber(streamSequenceNumber))
		readStream, err := s.client.ReadStreamEvents(ctx, s.streamName(streamName), readStreamOptions, ^uint64(0))
		if err != nil {
			if strings.Contains(err.Error(), rpcErrContextCanceled) {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return
			}

			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("unexpected failure ReadStreamEvents: %v", err),
			}
			return
		}

		for {
			resolvedEvent, err := readStream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}

				if strings.Contains(err.Error(), rpcErrContextCanceled) {
					resultRecords <- rangedb.ResultRecord{
						Record: nil,
						Err:    context.Canceled,
					}
					return
				}

				if strings.Contains(err.Error(), streamNotFound) || strings.HasSuffix(err.Error(), rpcErrEventStreamIsDeleted) {
					resultRecords <- rangedb.ResultRecord{
						Record: nil,
						Err:    rangedb.ErrStreamNotFound,
					}
					return
				}

				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to receive event: %v", err),
				}
				return
			}

			record, err := s.serializer.Deserialize(resolvedEvent.Event.Data)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to deserialize: %v", err),
				}
				return
			}

			record.GlobalSequenceNumber = resolvedEvent.OriginalEvent().Position.Commit
			log.Printf("resolvedEvent.Commit: %v", resolvedEvent.Commit)
			log.Printf("resolvedEvent Position.Commit: %v", resolvedEvent.OriginalEvent().Position.Commit)
			log.Printf("resolvedEvent Position.Prepare: %v", resolvedEvent.OriginalEvent().Position.Prepare)
			log.Printf("resolvedEvent systemmetadata: %v", resolvedEvent.OriginalEvent().SystemMetadata)

			log.Printf("%v", resolvedEvent.OriginalEvent().Position)

			select {
			case <-ctx.Done():
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return

			default:
				resultRecords <- rangedb.ResultRecord{
					Record: record,
					Err:    nil,
				}
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func zeroBasedStreamSequenceNumber(streamSequenceNumber uint64) uint64 {
	if streamSequenceNumber > 0 {
		streamSequenceNumber--
	}
	return streamSequenceNumber
}

func (s *eventStore) OptimisticDeleteStream(ctx context.Context, expectedStreamSequenceNumber uint64, streamName string) error {
	// We have to manually obtain the current stream sequence number
	// client.DeleteStream does not return an error for stream not found
	actualSequenceNumber, err := s.getStreamSequenceNumber(ctx, streamName)
	if err != nil {
		return err
	}
	if actualSequenceNumber == 0 {
		return rangedb.ErrStreamNotFound
	}

	versionedStreamName := s.streamName(streamName)
	tombstoneStreamOptions := esclient.TombstoneStreamOptions{}
	tombstoneStreamOptions.SetExpectedRevision(streamrevision.Exact(expectedStreamSequenceNumber - 1))

	_, err = s.client.TombstoneStream(ctx, versionedStreamName, tombstoneStreamOptions)
	if err != nil {
		log.Printf("%T\n%#v", err, err)
		if strings.Contains(err.Error(), rpcErrWrongExpectedStreamRevision) {
			// We have to manually obtain the current stream sequence number
			// err does not contain "Actual version" and must be a bug in the EventStoreDB gRPC API.
			return &rangedberror.UnexpectedSequenceNumber{
				Expected:             expectedStreamSequenceNumber,
				ActualSequenceNumber: actualSequenceNumber,
			}
		}

		return err
	}

	s.sync.Lock()
	s.deletedStreams[versionedStreamName] = struct{}{}
	s.sync.Unlock()

	return nil
}

func (s *eventStore) OptimisticSave(ctx context.Context, expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.saveEvents(ctx, &expectedStreamSequenceNumber, eventRecords...)
}

func (s *eventStore) Save(ctx context.Context, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.saveEvents(ctx, nil, eventRecords...)
}

func (s *eventStore) AllEventsSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AllEventsConfig(ctx, s, s.broadcaster, bufferSize,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *eventStore) AggregateTypesSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber, aggregateTypes ...string) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AggregateTypesConfig(ctx, s, s.broadcaster, bufferSize,
			aggregateTypes,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *eventStore) TotalEventsInStream(ctx context.Context, streamName string) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, context.Canceled

	default:
	}

	iter := s.EventsByStream(ctx, 0, streamName)
	total := uint64(0)
	for iter.Next() {
		if iter.Err() != nil {
			break
		}
		total++
	}
	return total, nil
}

func (s *eventStore) saveEvents(ctx context.Context, expectedStreamSequenceNumber *uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	if len(eventRecords) < 1 {
		return 0, fmt.Errorf("missing events")
	}

	aggregateType := eventRecords[0].Event.AggregateType()
	aggregateID := eventRecords[0].Event.AggregateID()

	var pendingEventsData [][]byte
	var lastStreamSequenceNumber uint64

	stream := rangedb.GetStream(aggregateType, aggregateID)
	streamSequenceNumber, _ := s.getStreamSequenceNumber(ctx, stream)

	if expectedStreamSequenceNumber != nil && *expectedStreamSequenceNumber != streamSequenceNumber {
		return 0, &rangedberror.UnexpectedSequenceNumber{
			Expected:             *expectedStreamSequenceNumber,
			ActualSequenceNumber: streamSequenceNumber,
		}
	}

	for _, eventRecord := range eventRecords {
		if aggregateType != "" && aggregateType != eventRecord.Event.AggregateType() {
			return 0, fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != "" && aggregateID != eventRecord.Event.AggregateID() {
			return 0, fmt.Errorf("unmatched aggregate ID")
		}

		aggregateType = eventRecord.Event.AggregateType()
		aggregateID = eventRecord.Event.AggregateID()

		streamSequenceNumber++

		const placeholderGlobalSequenceNumber = 0
		record := &rangedb.Record{
			AggregateType:        aggregateType,
			AggregateID:          aggregateID,
			GlobalSequenceNumber: placeholderGlobalSequenceNumber,
			StreamSequenceNumber: streamSequenceNumber,
			EventType:            eventRecord.Event.EventType(),
			EventID:              s.uuidGenerator.New(),
			InsertTimestamp:      uint64(s.clock.Now().Unix()),
			Data:                 eventRecord.Event,
			Metadata:             eventRecord.Metadata,
		}

		recordData, err := s.serializer.Serialize(record)
		if err != nil {
			return 0, err
		}

		var eventMetadata []byte
		if eventRecord.Metadata != nil {
			eventMetadata, err = json.Marshal(eventRecord.Metadata)
			if err != nil {
				return 0, err
			}
		}

		eventUUID, _ := uuid.FromString(record.EventID)
		proposedEvent := messages.ProposedEvent{}
		proposedEvent.SetEventID(eventUUID)
		proposedEvent.SetEventType(record.EventType)
		proposedEvent.SetContentType("application/json")
		proposedEvent.SetData(recordData)
		proposedEvent.SetMetadata(eventMetadata)

		streamRevision := streamrevision.Any()
		if expectedStreamSequenceNumber != nil {
			streamRevision = streamrevision.Exact(*expectedStreamSequenceNumber - 1)
		}
		streamName := s.streamName(stream)

		appendToStreamRevisionOptions := esclient.AppendToStreamOptions{}
		appendToStreamRevisionOptions.SetExpectedRevision(streamRevision)
		writeResult, err := s.client.AppendToStream(ctx, streamName, appendToStreamRevisionOptions, proposedEvent)
		if err != nil {
			if errors.Is(err, clienterrors.ErrWrongExpectedStreamRevision) {
				return 0, &rangedberror.UnexpectedSequenceNumber{
					Expected:             *expectedStreamSequenceNumber,
					ActualSequenceNumber: 0,
				}
			}

			if strings.Contains(err.Error(), rpcErrContextCanceled) {
				return 0, context.Canceled
			}

			return 0, err
		}

		// commit/prepare are returned when calling AppendToStream()
		// commit/prepare are not available when calling ReadStreamEvents() and subsequent calls to Recv()

		record.GlobalSequenceNumber = writeResult.CommitPosition
		log.Printf("writeResult commit: %v", writeResult.CommitPosition)
		log.Printf("writeResult prepare: %v", writeResult.PreparePosition)

		recordData, err = s.serializer.Serialize(record)
		if err != nil {
			return 0, err
		}

		lastStreamSequenceNumber = record.StreamSequenceNumber
		pendingEventsData = append(pendingEventsData, recordData)
	}

	for _, data := range pendingEventsData {
		deSerializedRecord, err := s.serializer.Deserialize(data)
		if err == nil {
			s.broadcaster.Accept(deSerializedRecord)
		} else {
			log.Print(err)
		}
	}

	return lastStreamSequenceNumber, nil
}

func (s *eventStore) inCurrentVersion(event *messages.ResolvedEvent) bool {
	s.sync.RLock()
	defer s.sync.RUnlock()
	if _, ok := s.deletedStreams[event.Event.StreamID]; ok {
		return false
	}

	if s.streamPrefixer == nil {
		return true
	}

	return strings.HasPrefix(event.Event.StreamID, s.streamPrefixer.GetPrefix())
}

func (s *eventStore) Ping() error {
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	iter := s.EventsByStream(ctx, 0, "no-stream")
	iter.Next()
	if iter.Err() != nil && strings.Contains(iter.Err().Error(), "connection refused") {
		return iter.Err()
	}

	return nil
}

func (s *eventStore) getStreamSequenceNumber(ctx context.Context, stream string) (uint64, error) {
	iter := s.EventsByStream(ctx, 0, stream)

	lastStreamSequenceNumber := uint64(0)
	for iter.Next() {
		if iter.Err() != nil {
			return 0, iter.Err()
		}

		lastStreamSequenceNumber = iter.Record().StreamSequenceNumber
	}

	if iter.Err() != nil {
		return 0, iter.Err()
	}

	return lastStreamSequenceNumber, nil
}
