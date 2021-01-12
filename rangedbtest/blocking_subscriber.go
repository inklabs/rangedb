package rangedbtest

import (
	"github.com/inklabs/rangedb"
)

type blockingSubscriber struct {
	Records chan *rangedb.Record
	parent  rangedb.RecordSubscriber
}

// NewBlockingSubscriber constructs a RecordSubscriber that blocks on Accept into Records.
func NewBlockingSubscriber(parent rangedb.RecordSubscriber) *blockingSubscriber {
	return &blockingSubscriber{
		Records: make(chan *rangedb.Record, 10),
		parent:  parent,
	}
}

func (b blockingSubscriber) Accept(record *rangedb.Record) {
	if b.parent != nil {
		b.parent.Accept(record)
	}
	b.Records <- record
}
