package rangedbpb

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
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

func ToRecord(pbRecord *Record, eventTypeIdentifier rangedb.EventTypeIdentifier) (*rangedb.Record, error) {
	data, err := jsonrecordserializer.DecodeJsonData(
		pbRecord.EventType,
		strings.NewReader(pbRecord.Data),
		eventTypeIdentifier,
	)

	var metadata interface{}
	if pbRecord.Metadata != "null" {
		err = json.Unmarshal([]byte(pbRecord.Metadata), metadata)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal metadata: %v", err)
		}
	}

	return &rangedb.Record{
		AggregateType:        pbRecord.AggregateType,
		AggregateID:          pbRecord.AggregateID,
		GlobalSequenceNumber: pbRecord.GlobalSequenceNumber,
		StreamSequenceNumber: pbRecord.StreamSequenceNumber,
		InsertTimestamp:      pbRecord.InsertTimestamp,
		EventID:              pbRecord.EventID,
		EventType:            pbRecord.EventType,
		Data:                 data,
		Metadata:             metadata,
	}, nil
}
