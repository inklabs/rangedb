package rangedbws

import (
	"github.com/inklabs/rangedb"
)

// RecordBroadcaster is a projection that calls the supplied broadcast method.
type RecordBroadcaster struct {
	broadcast func(*rangedb.Record)
}

// NewRecordBroadcaster constructs the RecordBroadcaster projection.
func NewRecordBroadcaster(broadcast func(*rangedb.Record)) *RecordBroadcaster {
	return &RecordBroadcaster{broadcast: broadcast}
}

// Accept receives a record and calls the supplied broadcast method.
func (b *RecordBroadcaster) Accept(record *rangedb.Record) {
	b.broadcast(record)
}
