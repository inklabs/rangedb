package remotestore

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"sync"

	"google.golang.org/grpc"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

type JsonSerializer interface {
	rangedb.EventBinder
	rangedb.EventTypeIdentifier
}

type PbRecordReceiver interface {
	Recv() (*rangedbpb.Record, error)
}

type remoteStore struct {
	serializer JsonSerializer
	client     rangedbpb.RangeDBClient

	subscriberMux sync.RWMutex
	subscribers   []rangedb.RecordSubscriber
}

func New(conn *grpc.ClientConn) *remoteStore {
	client := rangedbpb.NewRangeDBClient(conn)
	return &remoteStore{
		serializer: jsonrecordserializer.New(),
		client:     client,
	}
}

func (s *remoteStore) Bind(events ...rangedb.Event) {
	s.serializer.Bind(events...)
}

func (s *remoteStore) EventsStartingWith(ctx context.Context, eventNumber uint64) <-chan *rangedb.Record {
	request := &rangedbpb.EventsRequest{
		StartingWithEventNumber: eventNumber,
	}

	events, err := s.client.Events(ctx, request)
	if err != nil {
		return closedChannel()
	}

	return s.readRecords(ctx, events)
}

func (s *remoteStore) EventsByAggregateTypesStartingWith(ctx context.Context, eventNumber uint64, aggregateTypes ...string) <-chan *rangedb.Record {
	request := &rangedbpb.EventsByAggregateTypeRequest{
		AggregateTypes:          aggregateTypes,
		StartingWithEventNumber: eventNumber,
	}

	events, err := s.client.EventsByAggregateType(ctx, request)
	if err != nil {
		return closedChannel()
	}

	return s.readRecords(ctx, events)

}

func (s *remoteStore) EventsByStreamStartingWith(ctx context.Context, eventNumber uint64, streamName string) <-chan *rangedb.Record {
	request := &rangedbpb.EventsByStreamRequest{
		StreamName:              streamName,
		StartingWithEventNumber: eventNumber,
	}

	events, err := s.client.EventsByStream(ctx, request)
	if err != nil {
		return closedChannel()
	}

	return s.readRecords(ctx, events)
}

func (s *remoteStore) Save(event rangedb.Event, expectedStreamSequenceNumber *uint64, metadata interface{}) error {
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

func (s *remoteStore) SaveEvent(aggregateType, aggregateID, eventType, eventID string, expectedStreamSequenceNumber *uint64, event, metadata interface{}) error {
	jsonData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	jsonMetadata, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	var expectedSSN *rangedbpb.Uint64Value
	if expectedStreamSequenceNumber != nil {
		expectedSSN = &rangedbpb.Uint64Value{Value: *expectedStreamSequenceNumber}
	}

	request := &rangedbpb.SaveEventsRequest{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Events: []*rangedbpb.Event{
			{
				ID:                           eventID,
				Type:                         eventType,
				Data:                         string(jsonData),
				Metadata:                     string(jsonMetadata),
				ExpectedStreamSequenceNumber: expectedSSN,
			},
		},
	}

	_, err = s.client.SaveEvents(context.Background(), request)
	if err != nil {
		return err
	}

	return nil
}

func (s *remoteStore) SubscribeStartingWith(ctx context.Context, eventNumber uint64, subscribers ...rangedb.RecordSubscriber) {
	rangedb.ReplayEvents(s, eventNumber, subscribers...)

	select {
	case <-ctx.Done():
		return
	default:
		s.Subscribe(subscribers...)
	}
}

func (s *remoteStore) TotalEventsInStream(streamName string) uint64 {
	request := &rangedbpb.TotalEventsInStreamRequest{
		StreamName: streamName,
	}

	response, err := s.client.TotalEventsInStream(context.Background(), request)
	if err != nil {
		return 0
	}

	return response.TotalEvents
}

func (s *remoteStore) readRecords(ctx context.Context, events PbRecordReceiver) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		for {
			pbRecord, err := events.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("failed to get record: %v", err)
				break
			}

			record, err := rangedbpb.ToRecord(pbRecord, s.serializer)
			if err != nil {
				log.Printf("failed converting to record: %v", err)
				continue
			}

			select {
			case <-ctx.Done():
				break
			case records <- record:
			}
		}

		close(records)
	}()

	return records
}

func (s *remoteStore) Subscribe(subscribers ...rangedb.RecordSubscriber) {
	s.subscriberMux.Lock()
	if len(s.subscribers) == 0 {
		s.listenForEvents()
	}
	s.subscribers = append(s.subscribers, subscribers...)
	s.subscriberMux.Unlock()
}

func (s *remoteStore) listenForEvents() {
	request := &rangedbpb.SubscribeToLiveEventsRequest{}

	ctx := context.Background()
	events, err := s.client.SubscribeToLiveEvents(ctx, request)
	if err != nil {
		log.Printf("failed to subscribe: %v", err)
		return
	}

	go func() {
		for record := range s.readRecords(ctx, events) {
			s.subscriberMux.RLock()
			for _, subscriber := range s.subscribers {
				subscriber.Accept(record)
			}
			s.subscriberMux.RUnlock()
		}
	}()
}

func closedChannel() <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)
	close(records)
	return records
}
