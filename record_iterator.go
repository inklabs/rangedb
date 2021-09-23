package rangedb

import (
	"context"
)

type recordIterator struct {
	resultRecords <-chan ResultRecord
	current       ResultRecord
}

// NewRecordIterator constructs a new rangedb.Record iterator
func NewRecordIterator(recordResult <-chan ResultRecord) *recordIterator {
	return &recordIterator{resultRecords: recordResult}
}

func (i *recordIterator) Next() bool {
	if i.current.Err != nil {
		return false
	}

	i.current = <-i.resultRecords

	return i.current.Record != nil
}

func (i *recordIterator) NextContext(ctx context.Context) bool {
	if i.current.Err != nil {
		return false
	}

	select {
	case i.current = <-i.resultRecords:
	case <-ctx.Done():
		i.current = ResultRecord{
			Record: nil,
			Err:    ctx.Err(),
		}
	}

	return i.current.Record != nil
}

func (i *recordIterator) Record() *Record {
	return i.current.Record
}

func (i *recordIterator) Err() error {
	return i.current.Err
}

func NewRecordIteratorWithError(err error) *recordIterator {
	records := make(chan ResultRecord, 1)
	records <- ResultRecord{Err: err}
	close(records)
	return NewRecordIterator(records)
}
