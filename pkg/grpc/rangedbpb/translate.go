package rangedbpb

import (
	"encoding/json"
	"fmt"

	"github.com/inklabs/rangedb"
)

func ToPbRecord(record *rangedb.Record) (*Record, error) {
	data, err := json.Marshal(record.Data)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal data: %v", err)
	}

	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal metadata: %v", err)
	}

	return &Record{
		AggregateType:        record.AggregateType,
		AggregateID:          record.AggregateID,
		GlobalSequenceNumber: record.GlobalSequenceNumber,
		StreamSequenceNumber: record.StreamSequenceNumber,
		InsertTimestamp:      record.InsertTimestamp,
		EventID:              record.EventID,
		EventType:            record.EventType,
		Data:                 string(data),
		Metadata:             string(metadata),
	}, nil
}
