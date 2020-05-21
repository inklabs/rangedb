package rangedbserver

import (
	"encoding/json"
	"fmt"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

type rangeDBServer struct {
	store rangedb.Store
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
		store: inmemorystore.New(),
	}

	for _, option := range options {
		option(server)
	}

	return server
}

func (s *rangeDBServer) EventsStartingWith(req *rangedbpb.StartingWith, stream rangedbpb.RangeDB_EventsStartingWithServer) error {
	for record := range s.store.EventsStartingWith(req.EventNumber) {
		data, metadata, err := s.marshalDataAndMetadata(record)
		if err != nil {
			return err
		}

		err = stream.Send(&rangedbpb.Record{
			AggregateType:        record.AggregateType,
			AggregateID:          record.AggregateID,
			GlobalSequenceNumber: record.GlobalSequenceNumber,
			StreamSequenceNumber: record.StreamSequenceNumber,
			InsertTimestamp:      record.InsertTimestamp,
			EventID:              record.EventID,
			EventType:            record.EventType,
			Data:                 data,
			Metadata:             metadata,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *rangeDBServer) marshalDataAndMetadata(record *rangedb.Record) (string, string, error) {
	data, err := json.Marshal(record.Data)
	if err != nil {
		return "", "", fmt.Errorf("unable to marshal data: %v", err)
	}

	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return "", "", fmt.Errorf("unable to marshal metadata: %v", err)
	}

	return string(data), string(metadata), nil
}
