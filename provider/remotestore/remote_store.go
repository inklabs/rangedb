package remotestore

import (
	"context"
	"encoding/json"
	"io"
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

	sync        sync.RWMutex
	subscribers []rangedb.RecordSubscriber
}

func New(conn *grpc.ClientConn) *remoteStore {
	client := rangedbpb.NewRangeDBClient(conn)
	return &remoteStore{
		serializer: jsonrecordserializer.New(),
		client:     client,
	}
}

func (r *remoteStore) Bind(events ...rangedb.Event) {
	r.serializer.Bind(events...)
}

func (r *remoteStore) EventsStartingWith(ctx context.Context, eventNumber uint64) <-chan *rangedb.Record {
	request := &rangedbpb.EventsRequest{
		StartingWithEventNumber: eventNumber,
	}

	events, err := r.client.Events(context.Background(), request)
	if err != nil {
		return closedChannel()
	}

	return r.readRecords(ctx, events)
}

func (r *remoteStore) EventsByAggregateTypesStartingWith(ctx context.Context, eventNumber uint64, aggregateTypes ...string) <-chan *rangedb.Record {
	request := &rangedbpb.EventsByAggregateTypeRequest{
		AggregateTypes:          aggregateTypes,
		StartingWithEventNumber: eventNumber,
	}

	events, err := r.client.EventsByAggregateType(ctx, request)
	if err != nil {
		return closedChannel()
	}

	return r.readRecords(ctx, events)

}

func (r *remoteStore) EventsByStreamStartingWith(ctx context.Context, eventNumber uint64, streamName string) <-chan *rangedb.Record {
	request := &rangedbpb.EventsByStreamRequest{
		StreamName:              streamName,
		StartingWithEventNumber: eventNumber,
	}

	events, err := r.client.EventsByStream(ctx, request)
	if err != nil {
		return closedChannel()
	}

	return r.readRecords(ctx, events)
}

func (r *remoteStore) Save(event rangedb.Event, metadata interface{}) error {
	return r.SaveEvent(
		event.AggregateType(),
		event.AggregateID(),
		event.EventType(),
		shortuuid.New().String(),
		event,
		metadata,
	)
}

func (r *remoteStore) SaveEvent(aggregateType, aggregateID, eventType, eventID string, event, metadata interface{}) error {
	jsonData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	jsonMetadata, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	request := &rangedbpb.SaveEventsRequest{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Events: []*rangedbpb.Event{
			{
				ID:       eventID,
				Type:     eventType,
				Data:     string(jsonData),
				Metadata: string(jsonMetadata),
			},
		},
	}

	_, err = r.client.SaveEvents(context.Background(), request)
	if err != nil {
		return err
	}

	return nil
}

func (r *remoteStore) SubscribeStartingWith(eventNumber uint64, subscribers ...rangedb.RecordSubscriber) {
	r.sync.Lock()
	startSubscription := false
	if len(r.subscribers) == 0 {
		startSubscription = true
	}

	for _, subscriber := range subscribers {
		r.subscribers = append(r.subscribers, subscriber)
	}
	r.sync.Unlock()

	if startSubscription {
		go func() {
			request := &rangedbpb.SubscribeToEventsRequest{
				StartingWithEventNumber: eventNumber,
			}

			events, err := r.client.SubscribeToEvents(context.Background(), request)
			if err != nil {
				//r.logger.Printf("failed to subscribe: %v", err)
				return
			}

			for record := range r.readRecords(context.Background(), events) {
				r.sync.RLock()
				for _, subscriber := range r.subscribers {
					subscriber.Accept(record)
				}
				r.sync.RUnlock()
			}
		}()
	}
}

func (r *remoteStore) TotalEventsInStream(streamName string) uint64 {
	request := &rangedbpb.TotalEventsInStreamRequest{
		StreamName: streamName,
	}

	response, err := r.client.TotalEventsInStream(context.Background(), request)
	if err != nil {
		return 0
	}

	return response.TotalEvents
}

func (r *remoteStore) readRecords(ctx context.Context, events PbRecordReceiver) <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)

	go func() {
		for {
			pbRecord, err := events.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				//r.logger.Printf("failed to get record: %v", err)
				break
			}

			record, err := rangedbpb.ToRecord(pbRecord, r.serializer)
			if err != nil {
				//r.logger.Printf("failed converting to record: %v", err)
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

func closedChannel() <-chan *rangedb.Record {
	records := make(chan *rangedb.Record)
	close(records)
	return records
}
