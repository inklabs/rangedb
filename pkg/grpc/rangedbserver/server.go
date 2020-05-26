package rangedbserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

type rangeDBServer struct {
	store rangedb.Store

	sync                     sync.RWMutex
	allEventConnections      map[PbRecordSender]struct{}
	aggregateTypeConnections map[string]map[PbRecordSender]struct{}

	broadcastMutex sync.Mutex
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
func New(options ...Option) *rangeDBServer {
	server := &rangeDBServer{
		store:                    inmemorystore.New(),
		allEventConnections:      make(map[PbRecordSender]struct{}),
		aggregateTypeConnections: make(map[string]map[PbRecordSender]struct{}),
	}

	for _, option := range options {
		option(server)
	}

	server.initProjections()

	return server
}

func (s *rangeDBServer) initProjections() {
	s.store.SubscribeStartingWith(0, rangedb.RecordSubscriberFunc(s.broadcastRecord))
}

func (s *rangeDBServer) Events(req *rangedbpb.EventsRequest, stream rangedbpb.RangeDB_EventsServer) error {
	for record := range s.store.EventsStartingWith(stream.Context(), req.StartingWithEventNumber) {
		pbRecord, err := rangedbpb.ToPbRecord(record)
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
	for record := range s.store.EventsByStreamStartingWith(stream.Context(), req.StartingWithEventNumber, req.StreamName) {
		pbRecord, err := rangedbpb.ToPbRecord(record)
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

func (s *rangeDBServer) EventsByAggregateType(req *rangedbpb.EventsByAggregateTypeRequest, stream rangedbpb.RangeDB_EventsByAggregateTypeServer) error {
	for record := range s.store.EventsByAggregateTypesStartingWith(stream.Context(), req.StartingWithEventNumber, req.AggregateTypes...) {
		pbRecord, err := rangedbpb.ToPbRecord(record)
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

func (s *rangeDBServer) SaveEvents(_ context.Context, req *rangedbpb.SaveEventsRequest) (*rangedbpb.SaveEventResponse, error) {
	eventsSaved := uint32(0)

	for _, event := range req.Events {
		var data interface{}
		err := json.Unmarshal([]byte(event.Data), &data)
		if err != nil {
			st := status.New(codes.InvalidArgument, fmt.Sprintf("unable to read event data: %v", err))
			st, _ = st.WithDetails(&rangedbpb.SaveEventFailureResponse{
				EventsSaved: eventsSaved,
			})
			return nil, st.Err()
		}

		var metadata interface{}
		if event.Metadata != "" {
			err = json.Unmarshal([]byte(event.Metadata), &metadata)
			if err != nil {
				st := status.New(codes.InvalidArgument, fmt.Sprintf("unable to read event metadata: %v", err))
				st, _ = st.WithDetails(&rangedbpb.SaveEventFailureResponse{
					EventsSaved: eventsSaved,
				})
				return nil, st.Err()
			}
		}

		err = s.store.SaveEvent(
			req.AggregateType,
			req.AggregateID,
			event.Type,
			event.ID,
			data,
			metadata,
		)
		if err != nil {
			st := status.New(codes.Internal, fmt.Sprintf("unable to save to store: %v", err))
			st, _ = st.WithDetails(&rangedbpb.SaveEventFailureResponse{
				EventsSaved: eventsSaved,
			})
			return nil, st.Err()
		}

		eventsSaved++
	}

	return &rangedbpb.SaveEventResponse{
		EventsSaved: eventsSaved,
	}, nil
}

func (s *rangeDBServer) SubscribeToEvents(req *rangedbpb.SubscribeToEventsRequest, stream rangedbpb.RangeDB_SubscribeToEventsServer) error {
	s.broadcastMutex.Lock()
	for record := range s.store.EventsStartingWith(stream.Context(), req.StartingWithEventNumber) {
		pbRecord, err := rangedbpb.ToPbRecord(record)
		if err != nil {
			return err
		}

		err = stream.Send(pbRecord)
		if err != nil {
			return err
		}
	}

	s.subscribeToAllEvents(stream)
	s.broadcastMutex.Unlock()

	<-stream.Context().Done()
	s.unsubscribeFromAllEvents(stream)

	return nil
}

func (s *rangeDBServer) subscribeToAllEvents(stream rangedbpb.RangeDB_SubscribeToEventsServer) {
	s.sync.Lock()
	s.allEventConnections[stream] = struct{}{}
	s.sync.Unlock()
}

func (s *rangeDBServer) unsubscribeFromAllEvents(stream rangedbpb.RangeDB_SubscribeToEventsServer) {
	s.sync.Lock()
	delete(s.allEventConnections, stream)
	s.sync.Unlock()
}

func (s *rangeDBServer) SubscribeToEventsByAggregateType(req *rangedbpb.SubscribeToEventsByAggregateTypeRequest, stream rangedbpb.RangeDB_SubscribeToEventsByAggregateTypeServer) error {
	s.broadcastMutex.Lock()
	for record := range s.store.EventsByAggregateTypesStartingWith(stream.Context(), req.StartingWithEventNumber, req.AggregateTypes...) {
		pbRecord, err := rangedbpb.ToPbRecord(record)
		if err != nil {
			return err
		}

		err = stream.Send(pbRecord)
		if err != nil {
			return err
		}
	}

	s.subscribeToAggregateTypes(stream, req.AggregateTypes)
	s.broadcastMutex.Unlock()

	<-stream.Context().Done()
	s.unsubscribeFromAggregateTypes(stream, req.AggregateTypes)

	return nil
}

func (s *rangeDBServer) TotalEventsInStream(_ context.Context, request *rangedbpb.TotalEventsInStreamRequest) (*rangedbpb.TotalEventsInStreamResponse, error) {
	totalEvents := s.store.TotalEventsInStream(request.StreamName)
	return &rangedbpb.TotalEventsInStreamResponse{
		TotalEvents: totalEvents,
	}, nil
}

func (s *rangeDBServer) subscribeToAggregateTypes(stream rangedbpb.RangeDB_SubscribeToEventsByAggregateTypeServer, aggregateTypes []string) {
	s.sync.Lock()

	for _, aggregateType := range aggregateTypes {
		if _, ok := s.aggregateTypeConnections[aggregateType]; !ok {
			s.aggregateTypeConnections[aggregateType] = make(map[PbRecordSender]struct{})
		}

		s.aggregateTypeConnections[aggregateType][stream] = struct{}{}
	}

	s.sync.Unlock()
}

func (s *rangeDBServer) unsubscribeFromAggregateTypes(stream rangedbpb.RangeDB_SubscribeToEventsByAggregateTypeServer, aggregateTypes []string) {
	s.sync.Lock()

	for _, aggregateType := range aggregateTypes {
		delete(s.aggregateTypeConnections[aggregateType], stream)
	}

	s.sync.Unlock()
}

func (s *rangeDBServer) broadcastRecord(record *rangedb.Record) {
	s.broadcastMutex.Lock()
	go func() {
		defer s.broadcastMutex.Unlock()

		pbRecord, err := rangedbpb.ToPbRecord(record)
		if err != nil {
			//s.logger.Printf("unable to marshal record: %v", err)
			return
		}

		s.sync.RLock()
		defer s.sync.RUnlock()

		for connection := range s.allEventConnections {
			err := connection.Send(pbRecord)
			if err != nil {
				//s.logger.Printf("unable to send record to gRPC client: %v", err)
			}
		}

		for aggregateType, connections := range s.aggregateTypeConnections {
			if record.AggregateType != aggregateType {
				continue
			}

			for connection := range connections {
				err := connection.Send(pbRecord)
				if err != nil {
					//s.logger.Printf("unable to send record to gRPC client: %v", err)
				}
			}
		}
	}()
}

type PbRecordSender interface {
	Send(*rangedbpb.Record) error
}
