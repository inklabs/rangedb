package rangedbserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

const recordBuffSize = 100

type streamSender interface {
	Send(*rangedbpb.Record) error
}

type void struct{}

type rangeDBServer struct {
	rangedbpb.UnimplementedRangeDBServer
	store           rangedb.Store
	bufferedRecords chan *rangedb.Record
	stopChan        chan void

	sync                     sync.RWMutex
	allEventConnections      map[PbRecordSender]void
	aggregateTypeConnections map[string]map[PbRecordSender]void

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
		bufferedRecords:          make(chan *rangedb.Record, recordBuffSize),
		stopChan:                 make(chan void),
		allEventConnections:      make(map[PbRecordSender]void),
		aggregateTypeConnections: make(map[string]map[PbRecordSender]void),
	}

	for _, option := range options {
		option(server)
	}

	server.initProjections()
	go server.startBroadcaster()

	return server
}

func (s *rangeDBServer) initProjections() {
	s.store.Subscribe(
		rangedb.RecordSubscriberFunc(s.accept),
	)
}

func (s *rangeDBServer) startBroadcaster() {
	for {
		select {
		case <-s.stopChan:
			return

		default:
			s.broadcastRecord(<-s.bufferedRecords)
		}
	}
}

func (s *rangeDBServer) accept(record *rangedb.Record) {
	s.bufferedRecords <- record
}

func (s *rangeDBServer) Stop() {
	close(s.stopChan)
}

func (s *rangeDBServer) Events(req *rangedbpb.EventsRequest, stream rangedbpb.RangeDB_EventsServer) error {
	recordIterator := s.store.EventsStartingWith(stream.Context(), req.GlobalSequenceNumber)
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
	recordIterator := s.store.EventsByStreamStartingWith(stream.Context(), req.StreamSequenceNumber, req.StreamName)
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

func (s *rangeDBServer) EventsByAggregateType(req *rangedbpb.EventsByAggregateTypeRequest, stream rangedbpb.RangeDB_EventsByAggregateTypeServer) error {
	recordIterator := s.store.EventsByAggregateTypesStartingWith(stream.Context(), req.GlobalSequenceNumber, req.AggregateTypes...)
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

	saveErr := s.store.OptimisticSave(ctx, req.ExpectedStreamSequenceNumber, eventRecords...)

	if saveErr != nil {
		message := fmt.Sprintf("unable to save to store: %v", saveErr)
		st := status.New(codes.Internal, message)
		st, _ = st.WithDetails(&rangedbpb.SaveFailureResponse{
			Message: message,
		})
		return nil, st.Err()
	}

	return &rangedbpb.SaveResponse{
		EventsSaved: uint32(len(eventRecords)),
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

	saveErr := s.store.Save(ctx, eventRecords...)

	if saveErr != nil {
		message := fmt.Sprintf("unable to save to store: %v", saveErr)
		st := status.New(codes.Internal, message)
		st, _ = st.WithDetails(&rangedbpb.SaveFailureResponse{
			Message: message,
		})
		return nil, st.Err()
	}

	return &rangedbpb.SaveResponse{
		EventsSaved: uint32(len(eventRecords)),
	}, nil
}

func (s *rangeDBServer) SubscribeToLiveEvents(_ *rangedbpb.SubscribeToLiveEventsRequest, stream rangedbpb.RangeDB_SubscribeToLiveEventsServer) error {
	s.subscribeToAllEvents(stream)
	<-stream.Context().Done()
	s.unsubscribeFromAllEvents(stream)

	return nil
}

func (s *rangeDBServer) SubscribeToEvents(req *rangedbpb.SubscribeToEventsRequest, stream rangedbpb.RangeDB_SubscribeToEventsServer) error {
	lastGlobalSequenceNumber, err := s.writeEventsToStream(stream, s.store.EventsStartingWith(stream.Context(), req.GlobalSequenceNumber))
	if err != nil {
		return err
	}

	s.broadcastMutex.Lock()
	_, err = s.writeEventsToStream(stream, s.store.EventsStartingWith(stream.Context(), lastGlobalSequenceNumber+1))
	if err != nil {
		s.broadcastMutex.Unlock()
		return err
	}

	s.subscribeToAllEvents(stream)
	s.broadcastMutex.Unlock()

	<-stream.Context().Done()
	s.unsubscribeFromAllEvents(stream)

	return nil
}

func (s *rangeDBServer) subscribeToAllEvents(stream rangedbpb.RangeDB_SubscribeToEventsServer) {
	s.sync.Lock()
	s.allEventConnections[stream] = void{}
	s.sync.Unlock()
}

func (s *rangeDBServer) unsubscribeFromAllEvents(stream rangedbpb.RangeDB_SubscribeToEventsServer) {
	s.sync.Lock()
	delete(s.allEventConnections, stream)
	s.sync.Unlock()
}

func (s *rangeDBServer) SubscribeToEventsByAggregateType(req *rangedbpb.SubscribeToEventsByAggregateTypeRequest, stream rangedbpb.RangeDB_SubscribeToEventsByAggregateTypeServer) error {
	lastGlobalSequenceNumber, err := s.writeEventsToStream(stream, s.store.EventsByAggregateTypesStartingWith(stream.Context(), req.GlobalSequenceNumber, req.AggregateTypes...))
	if err != nil {
		return err
	}

	s.broadcastMutex.Lock()
	_, err = s.writeEventsToStream(stream, s.store.EventsByAggregateTypesStartingWith(stream.Context(), lastGlobalSequenceNumber+1, req.AggregateTypes...))
	if err != nil {
		s.broadcastMutex.Unlock()
		return err
	}

	s.subscribeToAggregateTypes(stream, req.AggregateTypes)
	s.broadcastMutex.Unlock()

	<-stream.Context().Done()
	s.unsubscribeFromAggregateTypes(stream, req.AggregateTypes)

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

func (s *rangeDBServer) subscribeToAggregateTypes(stream rangedbpb.RangeDB_SubscribeToEventsByAggregateTypeServer, aggregateTypes []string) {
	s.sync.Lock()

	for _, aggregateType := range aggregateTypes {
		if _, ok := s.aggregateTypeConnections[aggregateType]; !ok {
			s.aggregateTypeConnections[aggregateType] = make(map[PbRecordSender]void)
		}

		s.aggregateTypeConnections[aggregateType][stream] = void{}
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
	defer s.broadcastMutex.Unlock()

	pbRecord, err := rangedbpb.ToPbRecord(record)
	if err != nil {
		// s.logger.Printf("unable to marshal record: %v", err)
		return
	}

	s.sync.RLock()
	defer s.sync.RUnlock()

	for connection := range s.allEventConnections {
		err := connection.Send(pbRecord)
		if err != nil {
			log.Printf("unable to send record to gRPC client: %v", err)
		}
	}

	if connections, ok := s.aggregateTypeConnections[record.AggregateType]; ok {
		for connection := range connections {
			err := connection.Send(pbRecord)
			if err != nil {
				log.Printf("unable to send record to gRPC client: %v", err)
			}
		}
	}
}

func (s *rangeDBServer) writeEventsToStream(stream streamSender, recordIterator rangedb.RecordIterator) (uint64, error) {
	var lastGlobalSequenceNumber uint64
	for recordIterator.Next() {
		if recordIterator.Err() != nil {
			return lastGlobalSequenceNumber, recordIterator.Err()
		}

		pbRecord, err := rangedbpb.ToPbRecord(recordIterator.Record())
		if err != nil {
			return lastGlobalSequenceNumber, err
		}

		err = stream.Send(pbRecord)
		if err != nil {
			return lastGlobalSequenceNumber, err
		}
		lastGlobalSequenceNumber = recordIterator.Record().GlobalSequenceNumber
	}

	return lastGlobalSequenceNumber, nil
}

// PbRecordSender defines the interface for sending a protobuf record.
type PbRecordSender interface {
	Send(*rangedbpb.Record) error
}
