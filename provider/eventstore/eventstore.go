package eventstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
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
	rpcErrContextCanceled   = "Canceled desc = context canceled"
	broadcastRecordBuffSize = 100
)

type RangeDBMetadata struct {
	StreamName      string `json:"streamName"`
	AggregateType   string `json:"aggregateType"`
	AggregateID     string `json:"aggregateID"`
	InsertTimestamp uint64 `json:"insertTimestamp"`
	EventType       string `json:"eventType"`
	EventID         string `json:"eventID"`
}

type ESDBMetadata struct {
	RangeDBMetadata RangeDBMetadata `json:"rangeDBMetadata"`
	EventMetadata   interface{}     `json:"eventMetadata"`
}

type StreamPrefixer interface {
	WithPrefix(name string) string
	GetPrefix() string
}

type Config struct {
	IPAddr   string
	Username string
	Password string
}

func (c Config) ConnectionString() string {
	return fmt.Sprintf("esdb://%s:%s@%s", c.Username, c.Password, c.IPAddr)
}

type eventStore struct {
	client              *esdb.Client
	clock               clock.Clock
	streamPrefixer      StreamPrefixer
	uuidGenerator       shortuuid.Generator
	broadcaster         broadcast.Broadcaster
	eventTypeIdentifier rangedb.EventTypeIdentifier
	config              Config

	password       string
	mu             sync.RWMutex
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

// New constructs an eventStore. Experimental: Use at your own risk!
func New(config Config, options ...Option) (*eventStore, error) {
	s := &eventStore{
		clock:               systemclock.New(),
		broadcaster:         broadcast.New(broadcastRecordBuffSize, broadcast.DefaultTimeout),
		eventTypeIdentifier: rangedb.NewEventIdentifier(),
		config:              config,
		deletedStreams:      make(map[string]struct{}),
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
	config, err := esdb.ParseConnectionString(s.config.ConnectionString())
	if err != nil {
		return fmt.Errorf("unexpected configuration error: %s", err.Error())
	}

	config.DisableTLS = true
	config.SkipCertificateVerification = true
	client, err := esdb.NewClient(config)
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

		readAllOptions := esdb.ReadAllOptions{
			From: esdb.Position{Commit: globalSequenceNumber},
		}
		readStream, err := s.client.ReadAll(ctx, readAllOptions, ^uint64(0))
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			if errors.Is(err, esdb.ErrStreamNotFound) {
				return
			}

			if strings.HasSuffix(err.Error(), rpcErrContextCanceled) {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return
			}

			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("unexpected failure ReadStreamEvents: %w", err),
			}
			return
		}

		for {
			resolvedEvent, err := readStream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}

				if strings.HasSuffix(err.Error(), rpcErrContextCanceled) {
					resultRecords <- rangedb.ResultRecord{
						Record: nil,
						Err:    context.Canceled,
					}
					return
				}

				if errors.Is(err, esdb.ErrStreamNotFound) {
					return
				}

				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to receive event: %w", err),
				}
				return
			}

			if resolvedEvent.Event == nil {
				continue
			}

			if !s.inCurrentVersion(resolvedEvent) {
				continue
			}

			if s.inDeletedStreams(resolvedEvent) {
				continue
			}

			record, err := s.recordFromLinkedEvent(resolvedEvent)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to deserialize resolved event: %w", err),
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

		readAllOptions := esdb.ReadAllOptions{
			From: esdb.Position{Commit: globalSequenceNumber},
		}
		readStream, err := s.client.ReadAll(ctx, readAllOptions, ^uint64(0))
		if err != nil {
			if strings.HasSuffix(err.Error(), rpcErrContextCanceled) {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return
			}

			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("unexpected failure ReadStreamEvents: %w", err),
			}
			return
		}
		for {
			resolvedEvent, err := readStream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}

				if errors.Is(err, esdb.ErrStreamNotFound) {
					return
				}

				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to receive event: %w", err),
				}
				return
			}

			if !s.inCurrentVersion(resolvedEvent) {
				continue
			}

			if s.inDeletedStreams(resolvedEvent) {
				continue
			}

			record, err := s.recordFromLinkedEvent(resolvedEvent)
			if err != nil {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to deserialize resolved event: %w", err),
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

		readStreamOptions := esdb.ReadStreamOptions{
			From:           esdb.Revision(zeroBasedSequenceNumber(streamSequenceNumber)),
			ResolveLinkTos: true,
		}
		readStream, err := s.client.ReadStream(ctx, s.streamName(streamName), readStreamOptions, ^uint64(0))
		if err != nil {
			if strings.HasSuffix(err.Error(), rpcErrContextCanceled) {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    context.Canceled,
				}
				return
			}

			var streamDeletedError *esdb.StreamDeletedError
			if errors.Is(err, esdb.ErrStreamNotFound) || errors.As(err, &streamDeletedError) {
				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    rangedb.ErrStreamNotFound,
				}
				return
			}

			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("unexpected failure ReadStream: %w", err),
			}
			return
		}

		totalRecords := 0
		for {
			resolvedEvent, err := readStream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					if totalRecords == 0 {
						resultRecords <- rangedb.ResultRecord{
							Record: nil,
							Err:    rangedb.ErrStreamNotFound,
						}
						return
					}

					return
				}

				if strings.HasSuffix(err.Error(), rpcErrContextCanceled) {
					resultRecords <- rangedb.ResultRecord{
						Record: nil,
						Err:    context.Canceled,
					}
					return
				}

				if errors.Is(err, esdb.ErrStreamNotFound) {
					resultRecords <- rangedb.ResultRecord{
						Record: nil,
						Err:    rangedb.ErrStreamNotFound,
					}
					return
				}

				resultRecords <- rangedb.ResultRecord{
					Record: nil,
					Err:    fmt.Errorf("unable to receive event: %w", err),
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
					Err:    fmt.Errorf("unable to deserialize resolved event: %w", err),
				}
				return
			}

			if record.StreamName != streamName {
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
	if sequenceNumber < 1 {
		return 0
	}

	return sequenceNumber - 1
}

func oneBasedSequenceNumber(sequenceNumber uint64) uint64 {
	return sequenceNumber + 1
}

func (s *eventStore) OptimisticDeleteStream(ctx context.Context, expectedStreamSequenceNumber uint64, streamName string) error {
	versionedStreamName := s.streamName(streamName)
	tombstoneStreamOptions := esdb.TombstoneStreamOptions{
		ExpectedRevision: esdb.Revision(zeroBasedSequenceNumber(expectedStreamSequenceNumber)),
		Authenticated:    nil,
	}

	_, err := s.client.TombstoneStream(ctx, versionedStreamName, tombstoneStreamOptions)
	if err != nil {
		var streamDeletedError *esdb.StreamDeletedError
		if errors.Is(err, esdb.ErrStreamNotFound) || errors.As(err, &streamDeletedError) {
			return rangedb.ErrStreamNotFound
		}

		if errors.Is(err, esdb.ErrWrongExpectedStreamRevision) {
			// We have to manually obtain the current stream sequence number
			// err does not contain "Actual version" and must be a bug in the EventStoreDB gRPC API.
			// re: https://github.com/EventStore/EventStore/issues/3226
			log.Printf("### Actual version missing: %#v", err)
			actualSequenceNumber, err := s.getStreamSequenceNumber(ctx, streamName)
			if err != nil {
				return err
			}

			return &rangedberror.UnexpectedSequenceNumber{
				Expected:             expectedStreamSequenceNumber,
				ActualSequenceNumber: actualSequenceNumber,
			}
		}

		if strings.HasSuffix(err.Error(), rpcErrContextCanceled) {
			return context.Canceled
		}

		return err
	}

	s.mu.Lock()
	s.deletedStreams[s.streamName(streamName)] = struct{}{}
	s.mu.Unlock()
	// s.waitForScavenge(ctx)

	return nil
}

func (s *eventStore) OptimisticSave(ctx context.Context, expectedStreamSequenceNumber uint64, streamName string, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.saveEvents(ctx, &expectedStreamSequenceNumber, streamName, eventRecords...)
}

func (s *eventStore) Save(ctx context.Context, streamName string, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	return s.saveEvents(ctx, nil, streamName, eventRecords...)
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

func (s *eventStore) saveEvents(ctx context.Context, expectedStreamSequenceNumber *uint64, streamName string, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	if len(eventRecords) < 1 {
		return 0, fmt.Errorf("missing events")
	}

	aggregateType := eventRecords[0].Event.AggregateType()
	aggregateID := eventRecords[0].Event.AggregateID()

	if expectedStreamSequenceNumber != nil {
		streamSequenceNumber, _ := s.getStreamSequenceNumber(ctx, streamName)
		if *expectedStreamSequenceNumber != streamSequenceNumber {
			return 0, &rangedberror.UnexpectedSequenceNumber{
				Expected:             *expectedStreamSequenceNumber,
				ActualSequenceNumber: streamSequenceNumber,
			}
		}
	}

	var proposedEvents []esdb.EventData

	for _, eventRecord := range eventRecords {
		if aggregateType != "" && aggregateType != eventRecord.Event.AggregateType() {
			return 0, fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != "" && aggregateID != eventRecord.Event.AggregateID() {
			return 0, fmt.Errorf("unmatched aggregate ID")
		}

		aggregateType = eventRecord.Event.AggregateType()
		aggregateID = eventRecord.Event.AggregateID()

		eventID := s.uuidGenerator.New()
		esDBMetadata := ESDBMetadata{
			RangeDBMetadata: RangeDBMetadata{
				StreamName:      streamName,
				AggregateType:   aggregateType,
				AggregateID:     aggregateID,
				InsertTimestamp: uint64(s.clock.Now().Unix()),
				EventType:       eventRecord.Event.EventType(),
				EventID:         eventID,
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
		proposedEvent := esdb.EventData{
			EventID:     eventUUID,
			EventType:   eventRecord.Event.EventType(),
			ContentType: esdb.JsonContentType,
			Data:        eventData,
			Metadata:    eventMetadata,
		}

		proposedEvents = append(proposedEvents, proposedEvent)
	}

	var streamRevision esdb.ExpectedRevision
	if expectedStreamSequenceNumber != nil && *expectedStreamSequenceNumber > 0 {
		streamRevision = esdb.Revision(zeroBasedSequenceNumber(*expectedStreamSequenceNumber))
	}

	appendToStreamRevisionOptions := esdb.AppendToStreamOptions{
		ExpectedRevision: streamRevision,
	}
	result, err := s.client.AppendToStream(ctx, s.streamName(streamName), appendToStreamRevisionOptions, proposedEvents...)
	if err != nil {
		if errors.Is(err, esdb.ErrWrongExpectedStreamRevision) {
			return 0, &rangedberror.UnexpectedSequenceNumber{
				Expected:             *expectedStreamSequenceNumber,
				ActualSequenceNumber: 0,
			}
		}

		if strings.HasSuffix(err.Error(), rpcErrContextCanceled) {
			return 0, context.Canceled
		}

		return 0, err
	}

	streamSequenceNumber := oneBasedSequenceNumber(result.NextExpectedVersion)

	return streamSequenceNumber, nil
}

func (s *eventStore) inCurrentVersion(event *esdb.ResolvedEvent) bool {
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

func (s *eventStore) recordFromLinkedEvent(resolvedEvent *esdb.ResolvedEvent) (*rangedb.Record, error) {
	if resolvedEvent.Event == nil {
		return nil, fmt.Errorf("not found")
	}

	var eventStoreDBMetadata ESDBMetadata
	err := json.Unmarshal(resolvedEvent.Event.UserMetadata, &eventStoreDBMetadata)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal ESDBMetadata err: %w", err)
	}

	globalSequenceNumber := uint64(0)
	if resolvedEvent.Commit != nil {
		globalSequenceNumber = *resolvedEvent.Commit
	}

	record := &rangedb.Record{
		StreamName:           eventStoreDBMetadata.RangeDBMetadata.StreamName,
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
	opts := esdb.SubscribeToAllOptions{
		From: esdb.Start{},
	}
	subscription, err := s.client.SubscribeToAll(ctx, opts)
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

			if strings.HasSuffix(err.Error(), rpcErrContextCanceled) {
				log.Printf("subscription: context canceled")
				return
			}

			if errors.Is(err, esdb.ErrStreamNotFound) {
				log.Printf("subscription: stream not found")
				return
			}

			log.Printf("subscription: unable to receive event: %v", err)
			return
		}

		if subscriptionEvent.EventAppeared == nil || subscriptionEvent.EventAppeared.Event == nil {
			continue
		}

		if !s.inCurrentVersion(subscriptionEvent.EventAppeared) {
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

func (s *eventStore) waitForScavenge(ctx context.Context) {
	log.Print("starting scavenge")
	uri := "http://0.0.0.0:2113/admin/scavenge"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, nil)
	if err != nil {
		log.Print(err)
		return
	}
	req.SetBasicAuth(s.config.Username, s.config.Password)
	req.Header.Add("Accept", "application/json")

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Print(err)
		return
	}

	if response.StatusCode != 200 {
		log.Print(response)
		return
	}

	log.Printf("response: %#v", response)
	log.Printf("headers: %v", response.Header)

	type ScavengeResponse struct {
		ScavengeID string `json:"scavengeId"`
	}

	var scavengeResponse ScavengeResponse
	err = json.NewDecoder(response.Body).Decode(&scavengeResponse)
	if err != nil {
		log.Print(err)
		return
	}

	scavengeStream := fmt.Sprintf("$scavenges-%s", scavengeResponse.ScavengeID)

	stream, err := s.client.SubscribeToStream(ctx, scavengeStream, esdb.SubscribeToStreamOptions{})
	if err != nil {
		log.Print(err)
		return
	}

	log.Print("Waiting for scavenge")
	for {
		log.Print(".")
		subscriptionEvent := stream.Recv()

		log.Print(subscriptionEvent.EventAppeared.Event)

		if subscriptionEvent.EventAppeared.Event.EventType == "$scavengeCompleted" {
			log.Print("Scavenge complete!")
			return
		}
	}
}

func (s *eventStore) inDeletedStreams(event *esdb.ResolvedEvent) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.deletedStreams[event.Event.StreamID]; ok {
		return true
	}

	return false
}
