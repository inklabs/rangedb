package rangedbserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/recordsubscriber"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

const (
	broadcastRecordBuffSize  = 100
	subscriberRecordBuffSize = 20
)

type void struct{}

type streamSender interface {
	Send(*rangedbpb.Record) error
}

type rangeDBServer struct {
	rangedbpb.UnimplementedRangeDBServer
	store       rangedb.Store
	broadcaster broadcast.Broadcaster
	stopChan    chan void
}

// Option defines functional option parameters for rangeDBServer.
type Option func(*rangeDBServer)

// WithStore is a functional option to inject a Store.
func WithStore(store rangedb.Store) Option {
	return func(api *rangeDBServer) {
		api.store = store
	}
}

// New constructs a new rangeDBServer.
func New(options ...Option) (*rangeDBServer, error) {
	server := &rangeDBServer{
		store:       inmemorystore.New(),
		broadcaster: broadcast.New(broadcastRecordBuffSize, broadcast.DefaultTimeout),
		stopChan:    make(chan void),
	}

	for _, option := range options {
		option(server)
	}

	err := server.initBroadcaster()
	if err != nil {
		return nil, err
	}

	return server, nil
}

func (s *rangeDBServer) initBroadcaster() error {
	ctx := context.Background()
	subscription := s.store.AllEventsSubscription(ctx,
		subscriberRecordBuffSize,
		s.broadcaster,
	)
	return subscription.Start()
}

func (s *rangeDBServer) Stop() error {
	s.broadcaster.Close()
	close(s.stopChan)
	return nil
}

func (s *rangeDBServer) Events(req *rangedbpb.EventsRequest, stream rangedbpb.RangeDB_EventsServer) error {
	recordIterator := s.store.Events(stream.Context(), req.GlobalSequenceNumber)
	for recordIterator.Next() {
		if recordIterator.Err() != nil {
			return recordIterator.Err()
		}

		pbRecord, err := rangedbpb.ToPbRecord(recordIterator.Record())
		if err != nil {
			return err
		}

		err = stream.Send(pbRecord)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *rangeDBServer) EventsByStream(req *rangedbpb.EventsByStreamRequest, stream rangedbpb.RangeDB_EventsByStreamServer) error {
	recordIterator := s.store.EventsByStream(stream.Context(), req.StreamSequenceNumber, req.StreamName)

	for recordIterator.Next() {
		if recordIterator.Err() != nil {
			return recordIterator.Err()
		}

		pbRecord, err := rangedbpb.ToPbRecord(recordIterator.Record())
		if err != nil {
			return err
		}

		err = stream.Send(pbRecord)
		if err != nil {
			return err
		}
	}

	if recordIterator.Err() != nil {
		return recordIterator.Err()
	}

	return nil
}

func (s *rangeDBServer) EventsByAggregateType(req *rangedbpb.EventsByAggregateTypeRequest, stream rangedbpb.RangeDB_EventsByAggregateTypeServer) error {
	recordIterator := s.store.EventsByAggregateTypes(stream.Context(), req.GlobalSequenceNumber, req.AggregateTypes...)
	for recordIterator.Next() {
		if recordIterator.Err() != nil {
			return recordIterator.Err()
		}

		pbRecord, err := rangedbpb.ToPbRecord(recordIterator.Record())
		if err != nil {
			return err
		}

		err = stream.Send(pbRecord)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *rangeDBServer) OptimisticDeleteStream(ctx context.Context, req *rangedbpb.OptimisticDeleteStreamRequest) (*rangedbpb.OptimisticDeleteStreamResponse, error) {
	err := s.store.OptimisticDeleteStream(ctx, req.ExpectedStreamSequenceNumber, req.StreamName)
	if err != nil {
		return nil, err
	}

	return &rangedbpb.OptimisticDeleteStreamResponse{
		EventsDeleted: uint32(req.ExpectedStreamSequenceNumber),
	}, nil
}

func (s *rangeDBServer) OptimisticSave(ctx context.Context, req *rangedbpb.OptimisticSaveRequest) (*rangedbpb.SaveResponse, error) {
	var eventRecords []*rangedb.EventRecord

	for _, event := range req.Events {
		var data interface{}
		err := json.Unmarshal([]byte(event.Data), &data)
		if err != nil {
			message := fmt.Sprintf("unable to read event data: %v", err)
			st := status.New(codes.InvalidArgument, message)
			st, _ = st.WithDetails(&rangedbpb.SaveFailureResponse{
				Message: message,
			})
			return nil, st.Err()
		}

		var metadata interface{}
		if event.Metadata != "" {
			err = json.Unmarshal([]byte(event.Metadata), &metadata)
			if err != nil {
				message := fmt.Sprintf("unable to read event metadata: %v", err)
				st := status.New(codes.InvalidArgument, message)
				st, _ = st.WithDetails(&rangedbpb.SaveFailureResponse{
					Message: message,
				})
				return nil, st.Err()
			}
		}

		eventRecords = append(eventRecords, &rangedb.EventRecord{
			Event:    rangedb.NewRawEvent(req.AggregateType, req.AggregateID, event.Type, data),
			Metadata: metadata,
		})
	}

	lastStreamSequenceNumber, saveErr := s.store.OptimisticSave(ctx, req.ExpectedStreamSequenceNumber, eventRecords...)

	if saveErr != nil {
		message := fmt.Sprintf("unable to save to store: %v", saveErr)
		st := status.New(codes.Internal, message)
		st, _ = st.WithDetails(&rangedbpb.SaveFailureResponse{
			Message: message,
		})
		return nil, st.Err()
	}

	return &rangedbpb.SaveResponse{
		EventsSaved:              uint32(len(eventRecords)),
		LastStreamSequenceNumber: lastStreamSequenceNumber,
	}, nil
}

func (s *rangeDBServer) Save(ctx context.Context, req *rangedbpb.SaveRequest) (*rangedbpb.SaveResponse, error) {
	var eventRecords []*rangedb.EventRecord

	for _, event := range req.Events {
		var data interface{}
		err := json.Unmarshal([]byte(event.Data), &data)
		if err != nil {
			message := fmt.Sprintf("unable to read event data: %v", err)
			st := status.New(codes.InvalidArgument, message)
			st, _ = st.WithDetails(&rangedbpb.SaveFailureResponse{
				Message: message,
			})
			return nil, st.Err()
		}

		var metadata interface{}
		if event.Metadata != "" {
			err = json.Unmarshal([]byte(event.Metadata), &metadata)
			if err != nil {
				message := fmt.Sprintf("unable to read event metadata: %v", err)
				st := status.New(codes.InvalidArgument, message)
				st, _ = st.WithDetails(&rangedbpb.SaveFailureResponse{
					Message: message,
				})
				return nil, st.Err()
			}
		}

		eventRecords = append(eventRecords, &rangedb.EventRecord{
			Event:    rangedb.NewRawEvent(req.AggregateType, req.AggregateID, event.Type, data),
			Metadata: metadata,
		})
	}

	lastStreamSequenceNumber, saveErr := s.store.Save(ctx, eventRecords...)

	if saveErr != nil {
		message := fmt.Sprintf("unable to save to store: %v", saveErr)
		st := status.New(codes.Internal, message)
		st, _ = st.WithDetails(&rangedbpb.SaveFailureResponse{
			Message: message,
		})
		return nil, st.Err()
	}

	return &rangedbpb.SaveResponse{
		EventsSaved:              uint32(len(eventRecords)),
		LastStreamSequenceNumber: lastStreamSequenceNumber,
	}, nil
}

func (s *rangeDBServer) SubscribeToLiveEvents(_ *rangedbpb.SubscribeToLiveEventsRequest, stream rangedbpb.RangeDB_SubscribeToLiveEventsServer) error {
	config := recordsubscriber.AllEventsConfig(stream.Context(),
		s.store,
		s.broadcaster,
		subscriberRecordBuffSize,
		func(record *rangedb.Record) error {
			return s.broadcastRecord(stream, record)
		},
	)
	subscriber := recordsubscriber.New(config)
	err := subscriber.Start()
	if err != nil {
		return err
	}

	<-stream.Context().Done()

	return nil
}

func (s *rangeDBServer) SubscribeToEvents(req *rangedbpb.SubscribeToEventsRequest, stream rangedbpb.RangeDB_SubscribeToEventsServer) error {
	config := recordsubscriber.AllEventsConfig(
		stream.Context(),
		s.store,
		s.broadcaster,
		subscriberRecordBuffSize,
		func(record *rangedb.Record) error {
			return s.broadcastRecord(stream, record)
		},
	)
	subscriber := recordsubscriber.New(config)
	err := subscriber.StartFrom(req.GlobalSequenceNumber)
	if err != nil {
		return err
	}

	<-stream.Context().Done()

	return nil
}

func (s *rangeDBServer) SubscribeToEventsByAggregateType(req *rangedbpb.SubscribeToEventsByAggregateTypeRequest, stream rangedbpb.RangeDB_SubscribeToEventsByAggregateTypeServer) error {
	config := recordsubscriber.AggregateTypesConfig(
		stream.Context(),
		s.store,
		s.broadcaster,
		subscriberRecordBuffSize,
		req.AggregateTypes,
		func(record *rangedb.Record) error {
			return s.broadcastRecord(stream, record)
		},
	)
	subscriber := recordsubscriber.New(config)
	err := subscriber.StartFrom(req.GlobalSequenceNumber)
	if err != nil {
		return err
	}

	<-stream.Context().Done()

	return nil
}

func (s *rangeDBServer) TotalEventsInStream(ctx context.Context, request *rangedbpb.TotalEventsInStreamRequest) (*rangedbpb.TotalEventsInStreamResponse, error) {
	totalEvents, err := s.store.TotalEventsInStream(ctx, request.StreamName)
	if err != nil {
		return nil, err
	}
	return &rangedbpb.TotalEventsInStreamResponse{
		TotalEvents: totalEvents,
	}, nil
}

func (s *rangeDBServer) broadcastRecord(stream streamSender, record *rangedb.Record) error {
	pbRecord, err := rangedbpb.ToPbRecord(record)
	if err != nil {
		// s.logger.Printf("unable to marshal record: %v", err)
		log.Printf("unable to marshal record: %v", err)
		return err
	}

	err = stream.Send(pbRecord)
	if err != nil {
		log.Printf("unable to send record to gRPC client: %v", err)
		return err
	}

	return nil
}

// PbRecordSender defines the interface for sending a protobuf record.
type PbRecordSender interface {
	Send(*rangedbpb.Record) error
}
