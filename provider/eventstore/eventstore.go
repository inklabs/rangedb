package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	esclient "github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/direction"
	clienterrors "github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/streamrevision"
	"github.com/gofrs/uuid"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
	"github.com/inklabs/rangedb/pkg/rangedberror"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

const (
	rpcErrContextCanceled = "Canceled desc = context canceled"
	streamNotFound        = "Failed to perform read because the stream was not found"
)

type eventStore struct {
	clock      clock.Clock
	serializer rangedb.RecordSerializer
	ipAddr     string
	username   string
	password   string

	sync                     sync.RWMutex
	version                  int64
	nextGlobalSequenceNumber uint64
	savedStreams             map[string]struct{}
}

// Option defines functional option parameters for eventStore.
type Option func(*eventStore)

// WithClock is a function option to inject a clock.Clock
func WithClock(clock clock.Clock) Option {
	return func(store *eventStore) {
		store.clock = clock
	}
}

// WithSerializer is a functional option to inject a rangedb.RecordSerializer.
func WithSerializer(serializer rangedb.RecordSerializer) Option {
	return func(store *eventStore) {
		store.serializer = serializer
	}
}

// New constructs an eventStore. Experimental: Use at your own risk.
func New(ipAddr, username, password string, options ...Option) *eventStore {
	s := &eventStore{
		clock:        systemclock.New(),
		serializer:   jsonrecordserializer.New(),
		savedStreams: make(map[string]struct{}),
		ipAddr:       ipAddr,
		username:     username,
		password:     password,
	}

	for _, option := range options {
		option(s)
	}

	return s
}

func (s *eventStore) newClient() *esclient.Client {
	config, err := esclient.ParseConnectionString(fmt.Sprintf("esdb://%s:%s@%s", s.username, s.password, s.ipAddr))
	if err != nil {
		log.Fatalf("unexpected configuration error: %s", err.Error())
	}
	config.DisableTLS = true
	config.SkipCertificateVerification = true
	client, err := esclient.NewClient(config)
	if err != nil {
		log.Fatalf("unable to create client: %s", err.Error())
	}

	return client
}

func (s *eventStore) streamName(name string) string {
	s.sync.RLock()
	defer s.sync.RUnlock()
	return fmt.Sprintf("%d-%s", s.version, name)
}

func (s *eventStore) SetVersion(version int64) {
	s.sync.Lock()
	s.nextGlobalSequenceNumber = 0
	s.version = version
	s.sync.Unlock()
}

func (s *eventStore) Bind(events ...rangedb.Event) {
	s.serializer.Bind(events...)
}

func (s *eventStore) EventsStartingWith(ctx context.Context, globalSequenceNumber uint64) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		client := s.newClient()
		err := client.Connect()
		if err != nil {
			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("unexpected failure connecting: %v", err),
			}
			return
		}
		defer client.Close()

		events, err := client.ReadAllEvents(ctx, direction.Forwards, position.StartPosition, ^uint64(0), true)
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
		for _, event := range events {
			if !s.inCurrentVersion(event) {
				continue
			}

			record, err := s.serializer.Deserialize(event.Data)
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

				record.StreamSequenceNumber = event.EventNumber
				resultRecords <- rangedb.ResultRecord{
					Record: record,
					Err:    nil,
				}
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *eventStore) inCurrentVersion(event messages.RecordedEvent) bool {
	return strings.HasPrefix(event.StreamID, fmt.Sprintf("%d-", s.version))
}

func (s *eventStore) EventsByAggregateTypesStartingWith(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)
		aggregateTypesMap := make(map[string]struct{})

		for _, aggregateType := range aggregateTypes {
			aggregateTypesMap[aggregateType] = struct{}{}
		}

		client := s.newClient()
		err := client.Connect()
		if err != nil {
			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("unexpected failure connecting: %v", err),
			}
			return
		}
		defer client.Close()

		events, err := client.ReadAllEvents(ctx, direction.Forwards, position.StartPosition, ^uint64(0), true)
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
		for _, event := range events {
			if !s.inCurrentVersion(event) {
				continue
			}

			record, err := s.serializer.Deserialize(event.Data)
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

				record.StreamSequenceNumber = event.EventNumber
				resultRecords <- rangedb.ResultRecord{
					Record: record,
					Err:    nil,
				}
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *eventStore) EventsByStreamStartingWith(ctx context.Context, streamSequenceNumber uint64, streamName string) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		client := s.newClient()
		err := client.Connect()
		if err != nil {
			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("unexpected failure connecting: %v", err),
			}
			return
		}
		defer client.Close()

		events, err := client.ReadStreamEvents(ctx, direction.Forwards, s.streamName(streamName), streamSequenceNumber, ^uint64(0), true)
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
		for _, event := range events {
			record, err := s.serializer.Deserialize(event.Data)
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
				record.StreamSequenceNumber = event.EventNumber
				resultRecords <- rangedb.ResultRecord{
					Record: record,
					Err:    nil,
				}
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *eventStore) OptimisticSave(expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(&expectedStreamSequenceNumber, eventRecords...)
}

func (s *eventStore) Save(eventRecords ...*rangedb.EventRecord) error {
	return s.saveEvents(nil, eventRecords...)
}

func (s *eventStore) saveEvents(expectedStreamSequenceNumber *uint64, eventRecords ...*rangedb.EventRecord) error {
	var aggregateType, aggregateID string

	var events []messages.ProposedEvent

	s.sync.RLock()
	nextGlobalSequenceNumber := s.nextGlobalSequenceNumber
	s.sync.RUnlock()

	for _, eventRecord := range eventRecords {
		if aggregateType != "" && aggregateType != eventRecord.Event.AggregateType() {
			return fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != "" && aggregateID != eventRecord.Event.AggregateID() {
			return fmt.Errorf("unmatched aggregate ID")
		}

		aggregateType = eventRecord.Event.AggregateType()
		aggregateID = eventRecord.Event.AggregateID()

		record := &rangedb.Record{
			AggregateType:        aggregateType,
			AggregateID:          aggregateID,
			GlobalSequenceNumber: nextGlobalSequenceNumber,
			StreamSequenceNumber: 0,
			EventType:            eventRecord.Event.EventType(),
			EventID:              shortuuid.New().String(),
			InsertTimestamp:      uint64(s.clock.Now().Unix()),
			Data:                 eventRecord.Event,
			Metadata:             eventRecord.Metadata,
		}

		recordData, err := s.serializer.Serialize(record)
		if err != nil {
			return err
		}

		var eventMetadata []byte
		if eventRecord.Metadata != nil {
			eventMetadata, err = json.Marshal(eventRecord.Metadata)
			if err != nil {
				return err
			}
		}

		eventUUID, _ := uuid.FromString(record.EventID)
		events = append(events, messages.ProposedEvent{
			EventID:      eventUUID,
			EventType:    record.EventType,
			ContentType:  "application/json",
			Data:         recordData,
			UserMetadata: eventMetadata,
		})
		nextGlobalSequenceNumber++
	}

	client := s.newClient()
	err := client.Connect()
	if err != nil {
		return fmt.Errorf("unexpected failure connecting: %s", err.Error())
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamRevision := streamrevision.StreamRevisionAny
	if expectedStreamSequenceNumber != nil {
		streamRevision = streamrevision.NewStreamRevision(*expectedStreamSequenceNumber - 1)
	}
	streamName := s.streamName(rangedb.GetStream(aggregateType, aggregateID))
	_, err = client.AppendToStream(ctx, streamName, streamRevision, events)
	if err != nil {
		if errors.Is(err, clienterrors.ErrWrongExpectedStreamRevision) {
			return &rangedberror.UnexpectedSequenceNumber{
				Expected:           *expectedStreamSequenceNumber,
				NextSequenceNumber: 0,
			}
		}

		return err
	}

	s.sync.Lock()
	s.nextGlobalSequenceNumber = nextGlobalSequenceNumber
	s.savedStreams[streamName] = struct{}{}
	s.sync.Unlock()

	return nil
}

func (s *eventStore) Subscribe(subscribers ...rangedb.RecordSubscriber) {
	s.sync.RLock()
	globalSequenceNumber := s.nextGlobalSequenceNumber
	s.sync.RUnlock()

	ctx := context.Background()
	s.SubscribeStartingWith(ctx, globalSequenceNumber, subscribers...)

}

func (s *eventStore) SubscribeStartingWith(ctx context.Context, globalSequenceNumber uint64, subscribers ...rangedb.RecordSubscriber) {
	for i, subscriber := range subscribers {
		go func(subscriber rangedb.RecordSubscriber, i int) {
			client := s.newClient()
			err := client.Connect()
			if err != nil {
				log.Printf("unexpected failure connecting: %v", err)
				return
			}
			defer client.Close()

			subscription, err := client.SubscribeToAll(ctx,
				position.StartPosition,
				true,
				func(event messages.RecordedEvent) {
					if !s.inCurrentVersion(event) {
						return
					}

					record, err := s.serializer.Deserialize(event.Data)
					if err != nil {
						log.Print(err)
						ctx.Done()
						return
					}

					if record.GlobalSequenceNumber < globalSequenceNumber {
						return
					}

					select {
					case <-ctx.Done():
						return
					default:
						record.StreamSequenceNumber = event.EventNumber
						subscriber.Accept(record)
					}
				},
				func(p position.Position) {
				},
				func(reason string) {
					// log.Printf("subscription dropped: %s", reason)
					ctx.Done()
				},
			)
			if err != nil {
				if strings.Contains(err.Error(), rpcErrContextCanceled) {
					ctx.Done()
					return
				}

				log.Printf("failed SubscribeAll: %v", err)
				return
			}
			err = subscription.Start()
			if err != nil {
				log.Print(err)
				return
			}
			defer subscription.Stop()
			<-ctx.Done()
		}(subscriber, i)
	}
}

func (s *eventStore) TotalEventsInStream(streamName string) uint64 {
	// TODO: pass in context
	ctx := context.Background()
	iter := s.EventsByStreamStartingWith(ctx, 0, streamName)
	total := uint64(0)
	for iter.Next() {
		if iter.Err() != nil {
			break
		}
		total++
	}
	return total
}

func (s *eventStore) SavedStreams() map[string]struct{} {
	s.sync.RLock()
	defer s.sync.RUnlock()

	return s.savedStreams
}

func (s *eventStore) Ping() error {
	client := s.newClient()

	err := client.Connect()
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	iter := s.EventsByStreamStartingWith(ctx, 0, "no-stream")
	iter.Next()
	if iter.Err() != nil && strings.Contains(iter.Err().Error(), "connection refused") {
		return iter.Err()
	}

	return nil
}
