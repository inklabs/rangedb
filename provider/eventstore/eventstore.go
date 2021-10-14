package eventstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
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

type RangeDBMetadata struct {
	AggregateType        string `json:"aggregateType"`
	AggregateID          string `json:"aggregateID"`
	StreamSequenceNumber uint64 `json:"streamSequenceNumber"`
	InsertTimestamp      uint64 `json:"insertTimestamp"`
	EventType            string `json:"eventType"`
	EventID              string `json:"eventID"`
}

type ESDBMetadata struct {
	RangeDBMetadata RangeDBMetadata `json:"rangeDBMetadata"`
	EventMetadata   interface{}     `json:"eventMetadata"`
}

type StreamPrefixer interface {
	WithPrefix(name string) string
	GetPrefix() string
}

type eventStore struct {
	client              *esclient.Client
	clock               clock.Clock
	streamPrefixer      StreamPrefixer
	uuidGenerator       shortuuid.Generator
	broadcaster         broadcast.Broadcaster
	eventTypeIdentifier rangedb.EventTypeIdentifier
	ipAddr              string
	username            string
	password            string
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

// New constructs an eventStore. Experimental: Use at your own risk.
func New(ipAddr, username, password string, options ...Option) (*eventStore, error) {
	s := &eventStore{
		clock:               systemclock.New(),
		broadcaster:         broadcast.New(broadcastRecordBuffSize, broadcast.DefaultTimeout),
		eventTypeIdentifier: rangedb.NewEventIdentifier(),
		ipAddr:              ipAddr,
		username:            username,
		password:            password,
	}

	for _, option := range options {
		option(s)
	}

	err := s.setupClient()
	if err != nil {
		return nil, err
	}

	go s.startSubscription()

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
	s.eventTypeIdentifier.Bind(events...)
}

func (s *eventStore) Events(ctx context.Context, globalSequenceNumber uint64) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		readStreamOptions := esclient.ReadStreamEventsOptions{}
		readStreamOptions.SetFromRevision(globalSequenceNumber)
		readStreamOptions.SetResolveLinks()
		readStream, err := s.client.ReadStreamEvents(ctx, "LogData", readStreamOptions, ^uint64(0))
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
					return
				}

				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to receive event: %v", err),
				}
				return
			}

			if resolvedEvent.Event == nil {
				continue
			}

			record, err := s.recordFromLinkedEvent(resolvedEvent)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to deserialize resolved event: %v", err),
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

		readStreamOptions := esclient.ReadStreamEventsOptions{}
		readStreamOptions.SetFromRevision(zeroBasedSequenceNumber(globalSequenceNumber))
		readStreamOptions.SetResolveLinks()
		readStream, err := s.client.ReadStreamEvents(ctx, "LogData", readStreamOptions, ^uint64(0))
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

				if strings.Contains(err.Error(), streamNotFound) || strings.HasSuffix(err.Error(), rpcErrEventStreamIsDeleted) {
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

			record, err := s.recordFromLinkedEvent(resolvedEvent)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to deserialize resolved event: %v", err),
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

		aggregateType, aggregateID := rangedb.ParseStream(streamName)

		readStreamOptions := esclient.ReadStreamEventsOptions{}
		readStreamOptions.SetFromRevision(0)
		readStreamOptions.SetResolveLinks()
		readStream, err := s.client.ReadStreamEvents(ctx, "LogData", readStreamOptions, ^uint64(0))
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

		totalRecords := 0
		for {
			resolvedEvent, err := readStream.Recv()
			if err != nil {
				if err == io.EOF {
					if totalRecords == 0 {
						resultRecords <- rangedb.ResultRecord{
							Record: nil,
							Err:    rangedb.ErrStreamNotFound,
						}
						return
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

			if !s.inCurrentVersion(resolvedEvent) {
				continue
			}

			record, err := s.recordFromLinkedEvent(resolvedEvent)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to deserialize resolved event: %v", err),
				}
				return
			}

			if record.AggregateType != aggregateType || record.AggregateID != aggregateID {
				continue
			}

			if record.StreamSequenceNumber < streamSequenceNumber {
				continue
			}

			select {
			case <-ctx.Done():
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return

			case resultRecords <- rangedb.ResultRecord{
				Record: record,
				Err:    nil,
			}:
				totalRecords++
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func zeroBasedSequenceNumber(sequenceNumber uint64) uint64 {
	if sequenceNumber > 0 {
		sequenceNumber--
	}
	return sequenceNumber
}

func oneBasedSequenceNumber(sequenceNumber uint64) uint64 {
	return sequenceNumber + 1
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

	stream := rangedb.GetStream(aggregateType, aggregateID)
	streamSequenceNumber, _ := s.getStreamSequenceNumber(ctx, stream)

	if expectedStreamSequenceNumber != nil && *expectedStreamSequenceNumber != streamSequenceNumber {
		return 0, &rangedberror.UnexpectedSequenceNumber{
			Expected:             *expectedStreamSequenceNumber,
			ActualSequenceNumber: streamSequenceNumber,
		}
	}

	var proposedEvents []messages.ProposedEvent

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

		eventID := s.uuidGenerator.New()
		esDBMetadata := ESDBMetadata{
			RangeDBMetadata: RangeDBMetadata{
				AggregateType:        aggregateType,
				AggregateID:          aggregateID,
				StreamSequenceNumber: streamSequenceNumber,
				InsertTimestamp:      uint64(s.clock.Now().Unix()),
				EventType:            eventRecord.Event.EventType(),
				EventID:              eventID,
			},
			EventMetadata: eventRecord.Metadata,
		}

		var eventMetadata []byte
		eventMetadata, err := json.Marshal(esDBMetadata)
		if err != nil {
			return 0, err
		}

		eventData, err := json.Marshal(eventRecord.Event)
		if err != nil {
			return 0, err
		}

		eventUUID, _ := uuid.FromString(eventID)
		proposedEvent := messages.ProposedEvent{}
		proposedEvent.SetEventID(eventUUID)
		proposedEvent.SetEventType(eventRecord.Event.EventType())
		proposedEvent.SetContentType("application/json")
		proposedEvent.SetData(eventData)
		proposedEvent.SetMetadata(eventMetadata)

		proposedEvents = append(proposedEvents, proposedEvent)
	}

	streamRevision := streamrevision.Any()
	if expectedStreamSequenceNumber != nil && *expectedStreamSequenceNumber > 0 {
		streamRevision = streamrevision.Exact(*expectedStreamSequenceNumber - 1)
	}

	streamName := s.streamName(stream)
	appendToStreamRevisionOptions := esclient.AppendToStreamOptions{}
	appendToStreamRevisionOptions.SetExpectedRevision(streamRevision)
	_, err := s.client.AppendToStream(ctx, streamName, appendToStreamRevisionOptions, proposedEvents...)
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

	s.waitForLogDataProjectionToCatchUp()

	return streamSequenceNumber, nil
}

func (s *eventStore) waitForLogDataProjectionToCatchUp() {
	time.Sleep(time.Millisecond * 100)
}

func (s *eventStore) inCurrentVersion(event *messages.ResolvedEvent) bool {
	if event.Event == nil {
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
	iter := s.EventsByStream(ctx, 0, "no!stream")
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

func (s *eventStore) recordFromLinkedEvent(resolvedEvent *messages.ResolvedEvent) (*rangedb.Record, error) {
	if resolvedEvent.Event == nil {
		return nil, fmt.Errorf("not found")
	}

	var eventStoreDBMetadata ESDBMetadata
	err := json.Unmarshal(resolvedEvent.Event.UserMetadata, &eventStoreDBMetadata)
	if err != nil {
		return nil, err
	}

	globalSequenceNumber := ^uint64(0)
	if resolvedEvent.Link != nil {
		globalSequenceNumber = resolvedEvent.Link.EventNumber
	}

	record := &rangedb.Record{
		AggregateType:        eventStoreDBMetadata.RangeDBMetadata.AggregateType,
		AggregateID:          eventStoreDBMetadata.RangeDBMetadata.AggregateID,
		GlobalSequenceNumber: globalSequenceNumber,
		StreamSequenceNumber: oneBasedSequenceNumber(resolvedEvent.Event.EventNumber),
		InsertTimestamp:      eventStoreDBMetadata.RangeDBMetadata.InsertTimestamp,
		EventID:              eventStoreDBMetadata.RangeDBMetadata.EventID,
		EventType:            eventStoreDBMetadata.RangeDBMetadata.EventType,
		Metadata:             eventStoreDBMetadata.EventMetadata,
	}

	event, err := jsonrecordserializer.DecodeJsonData(record.EventType, bytes.NewReader(resolvedEvent.Event.Data), s.eventTypeIdentifier)
	if err != nil {
		return nil, err
	}

	record.Data = event

	return record, nil
}

func (s *eventStore) startSubscription() {
	ctx := context.Background()
	opts := esclient.SubscribeToStreamOptions{}
	opts.SetResolveLinks()
	opts.SetFromStart()
	subscription, err := s.client.SubscribeToStream(ctx, "LogData", opts)
	if err != nil {
		return
	}
	defer func() {
		err := subscription.Close()
		if err != nil {
			log.Printf("subscription: unable to close")
			return
		}
	}()

	for {
		subscriptionEvent := subscription.Recv()
		if err != nil {
			if err == io.EOF {
				log.Printf("subscription: io.EOF")
				return
			}

			if strings.Contains(err.Error(), rpcErrContextCanceled) {
				log.Printf("subscription: context canceled")
				return
			}

			if strings.Contains(err.Error(), streamNotFound) || strings.HasSuffix(err.Error(), rpcErrEventStreamIsDeleted) {
				log.Printf("subscription: stream not found")
				return
			}

			log.Printf("subscription: unable to receive event: %v", err)
			return
		}

		if subscriptionEvent.EventAppeared == nil || subscriptionEvent.EventAppeared.Event == nil {
			continue
		}

		record, err := s.recordFromLinkedEvent(subscriptionEvent.EventAppeared)
		if err != nil {
			log.Printf("subscription: unable deserialize resolved event: %v", err)
			return
		}

		select {
		case <-ctx.Done():
			log.Printf("subscription: context canceled")
			return

		default:
			s.broadcaster.Accept(record)
		}
	}
}
