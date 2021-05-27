package remotestore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/rangedberror"
	"github.com/inklabs/rangedb/pkg/recordsubscriber"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

const (
	broadcastRecordBuffSize        = 100
	rpcErrContextCanceled          = "Canceled desc = context canceled"
	rpcErrUnexpectedSequenceNumber = "unexpected sequence number"
	rpcErrStreamNotFound           = "Unknown desc = stream not found"
)

// JsonSerializer defines the interface to bind events and identify event types.
type JsonSerializer interface {
	rangedb.EventBinder
	rangedb.EventTypeIdentifier
}

// PbRecordReceiver defines the interface to receive a protobuf record.
type PbRecordReceiver interface {
	Recv() (*rangedbpb.Record, error)
}

type remoteStore struct {
	serializer  JsonSerializer
	client      rangedbpb.RangeDBClient
	broadcaster broadcast.Broadcaster
}

// New constructs a new rangedb.Store client that communicates with a remote gRPC backend.
func New(conn *grpc.ClientConn) (*remoteStore, error) {
	client := rangedbpb.NewRangeDBClient(conn)
	s := &remoteStore{
		serializer:  jsonrecordserializer.New(),
		broadcaster: broadcast.New(broadcastRecordBuffSize, broadcast.DefaultTimeout),
		client:      client,
	}

	err := s.listenForEvents()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *remoteStore) Bind(events ...rangedb.Event) {
	s.serializer.Bind(events...)
}

func (s *remoteStore) Events(ctx context.Context, globalSequenceNumber uint64) rangedb.RecordIterator {
	request := &rangedbpb.EventsRequest{
		GlobalSequenceNumber: globalSequenceNumber,
	}

	events, err := s.client.Events(ctx, request)
	if err != nil {
		if strings.Contains(err.Error(), rpcErrContextCanceled) {
			return rangedb.NewRecordIteratorWithError(context.Canceled)
		}

		return rangedb.NewRecordIteratorWithError(err)
	}

	return s.readRecords(ctx, events)
}

func (s *remoteStore) EventsByAggregateTypes(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) rangedb.RecordIterator {
	request := &rangedbpb.EventsByAggregateTypeRequest{
		AggregateTypes:       aggregateTypes,
		GlobalSequenceNumber: globalSequenceNumber,
	}

	events, err := s.client.EventsByAggregateType(ctx, request)
	if err != nil {
		if strings.Contains(err.Error(), rpcErrContextCanceled) {
			return rangedb.NewRecordIteratorWithError(context.Canceled)
		}

		return rangedb.NewRecordIteratorWithError(err)
	}

	return s.readRecords(ctx, events)

}

func (s *remoteStore) EventsByStream(ctx context.Context, streamSequenceNumber uint64, streamName string) rangedb.RecordIterator {
	request := &rangedbpb.EventsByStreamRequest{
		StreamName:           streamName,
		StreamSequenceNumber: streamSequenceNumber,
	}

	events, err := s.client.EventsByStream(ctx, request)
	if err != nil {
		if strings.Contains(err.Error(), rpcErrContextCanceled) {
			return rangedb.NewRecordIteratorWithError(context.Canceled)
		}

		return rangedb.NewRecordIteratorWithError(err)
	}

	return s.readRecords(ctx, events)
}

func (s *remoteStore) OptimisticDeleteStream(ctx context.Context, expectedStreamSequenceNumber uint64, streamName string) error {
	request := &rangedbpb.OptimisticDeleteStreamRequest{
		ExpectedStreamSequenceNumber: expectedStreamSequenceNumber,
		StreamName:                   streamName,
	}
	_, err := s.client.OptimisticDeleteStream(ctx, request)
	if err != nil {
		if strings.Contains(err.Error(), rpcErrContextCanceled) {
			return context.Canceled
		}

		if strings.Contains(err.Error(), rpcErrStreamNotFound) {
			return rangedb.ErrStreamNotFound
		}

		if strings.Contains(err.Error(), rpcErrUnexpectedSequenceNumber) {
			return rangedberror.NewUnexpectedSequenceNumberFromString(err.Error())
		}

		return err
	}

	return nil
}

func (s *remoteStore) OptimisticSave(ctx context.Context, expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	if len(eventRecords) < 1 {
		return 0, fmt.Errorf("missing events")
	}

	var aggregateType, aggregateID string

	var events []*rangedbpb.Event
	for _, eventRecord := range eventRecords {
		if aggregateType != "" && aggregateType != eventRecord.Event.AggregateType() {
			return 0, fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != "" && aggregateID != eventRecord.Event.AggregateID() {
			return 0, fmt.Errorf("unmatched aggregate ID")
		}

		aggregateType = eventRecord.Event.AggregateType()
		aggregateID = eventRecord.Event.AggregateID()

		jsonData, err := json.Marshal(eventRecord.Event)
		if err != nil {
			return 0, err
		}

		jsonMetadata, err := json.Marshal(eventRecord.Metadata)
		if err != nil {
			return 0, err
		}

		events = append(events, &rangedbpb.Event{
			Type:     eventRecord.Event.EventType(),
			Data:     string(jsonData),
			Metadata: string(jsonMetadata),
		})
	}

	request := &rangedbpb.OptimisticSaveRequest{
		AggregateType:                aggregateType,
		AggregateID:                  aggregateID,
		Events:                       events,
		ExpectedStreamSequenceNumber: expectedStreamSequenceNumber,
	}

	response, err := s.client.OptimisticSave(ctx, request)
	if err != nil {
		if strings.Contains(err.Error(), rpcErrContextCanceled) {
			return 0, context.Canceled
		}

		if strings.Contains(err.Error(), rpcErrUnexpectedSequenceNumber) {
			return 0, rangedberror.NewUnexpectedSequenceNumberFromString(err.Error())
		}

		return 0, err
	}

	return response.LastStreamSequenceNumber, nil
}

func (s *remoteStore) Save(ctx context.Context, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	if len(eventRecords) < 1 {
		return 0, fmt.Errorf("missing events")
	}

	var aggregateType, aggregateID string

	var events []*rangedbpb.Event
	for _, eventRecord := range eventRecords {
		if aggregateType != "" && aggregateType != eventRecord.Event.AggregateType() {
			return 0, fmt.Errorf("unmatched aggregate type")
		}

		if aggregateID != "" && aggregateID != eventRecord.Event.AggregateID() {
			return 0, fmt.Errorf("unmatched aggregate ID")
		}

		aggregateType = eventRecord.Event.AggregateType()
		aggregateID = eventRecord.Event.AggregateID()

		jsonData, err := json.Marshal(eventRecord.Event)
		if err != nil {
			return 0, err
		}

		jsonMetadata, err := json.Marshal(eventRecord.Metadata)
		if err != nil {
			return 0, err
		}

		events = append(events, &rangedbpb.Event{
			Type:     eventRecord.Event.EventType(),
			Data:     string(jsonData),
			Metadata: string(jsonMetadata),
		})
	}

	request := &rangedbpb.SaveRequest{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Events:        events,
	}

	response, err := s.client.Save(ctx, request)
	if err != nil {
		if strings.Contains(err.Error(), rpcErrContextCanceled) {
			return 0, context.Canceled
		}

		return 0, err
	}

	return response.LastStreamSequenceNumber, nil
}

func (s *remoteStore) AllEventsSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AllEventsConfig(ctx, s, s.broadcaster, bufferSize,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *remoteStore) AggregateTypesSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber, aggregateTypes ...string) rangedb.RecordSubscription {
	return recordsubscriber.New(
		recordsubscriber.AggregateTypesConfig(ctx, s, s.broadcaster, bufferSize,
			aggregateTypes,
			func(record *rangedb.Record) error {
				subscriber.Accept(record)
				return nil
			},
		))
}

func (s *remoteStore) TotalEventsInStream(ctx context.Context, streamName string) (uint64, error) {
	request := &rangedbpb.TotalEventsInStreamRequest{
		StreamName: streamName,
	}

	response, err := s.client.TotalEventsInStream(ctx, request)
	if err != nil {
		if strings.Contains(err.Error(), rpcErrContextCanceled) {
			return 0, context.Canceled
		}

		return 0, err
	}

	return response.TotalEvents, nil
}

func (s *remoteStore) readRecords(ctx context.Context, events PbRecordReceiver) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		for {
			pbRecord, err := events.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}

				if strings.Contains(err.Error(), rpcErrContextCanceled) {
					resultRecords <- rangedb.ResultRecord{Err: context.Canceled}
					return
				}

				if strings.Contains(err.Error(), rpcErrStreamNotFound) {
					resultRecords <- rangedb.ResultRecord{Err: rangedb.ErrStreamNotFound}
					return
				}

				log.Printf("failed to get record: %v", err)
				resultRecords <- rangedb.ResultRecord{Err: err}
				return
			}

			record, err := rangedbpb.ToRecord(pbRecord, s.serializer)
			if err != nil {
				log.Printf("failed converting to record: %v", err)
				resultRecords <- rangedb.ResultRecord{Err: err}
				return
			}

			if !rangedb.PublishRecordOrCancel(ctx, resultRecords, record, time.Second) {
				return
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}

func (s *remoteStore) listenForEvents() error {
	request := &rangedbpb.SubscribeToLiveEventsRequest{}
	ctx := context.Background()
	events, err := s.client.SubscribeToLiveEvents(ctx, request)
	if err != nil {
		err = fmt.Errorf("failed to subscribe: %v", err)
		log.Print(err)
		return err
	}

	go func() {
		recordIterator := s.readRecords(ctx, events)
		for recordIterator.Next() {
			if recordIterator.Err() != nil {
				continue
			}

			s.broadcaster.Accept(recordIterator.Record())
		}
	}()

	return nil
}
