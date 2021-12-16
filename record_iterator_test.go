package rangedb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestRecordIterator(t *testing.T) {
	event := &rangedbtest.ThingWasDone{ID: "A", Number: 1}
	record := &rangedb.Record{
		StreamName:           "thing-" + event.AggregateID(),
		AggregateType:        event.AggregateType(),
		AggregateID:          event.AggregateID(),
		GlobalSequenceNumber: 1,
		StreamSequenceNumber: 1,
		EventType:            event.EventType(),
		InsertTimestamp:      0,
		Data:                 event,
		Metadata:             nil,
	}

	t.Run("only 1 record in the stream", func(t *testing.T) {
		t.Run("first call to next", func(t *testing.T) {
			// Given
			iter := stubRecordIterator(rangedb.ResultRecord{Record: record, Err: nil})

			// When
			canContinue := iter.Next()

			// Then
			assert.True(t, canContinue)
			assert.Equal(t, record, iter.Record())
			assert.Nil(t, iter.Err())
		})

		t.Run("second call to Next", func(t *testing.T) {
			// Given
			iter := stubRecordIterator(rangedb.ResultRecord{Record: record, Err: nil})
			iter.Next()

			// When
			canContinue := iter.Next()

			// Then
			assert.False(t, canContinue)
			assert.Nil(t, iter.Record())
			assert.Nil(t, iter.Err())
		})

		t.Run("first call to next context", func(t *testing.T) {
			// Given
			iter := stubRecordIterator(rangedb.ResultRecord{Record: record, Err: nil})
			ctx := rangedbtest.TimeoutContext(t)

			// When
			canContinue := iter.NextContext(ctx)

			// Then
			assert.True(t, canContinue)
			assert.Equal(t, record, iter.Record())
			assert.Nil(t, iter.Err())
		})

		t.Run("second call to next context", func(t *testing.T) {
			// Given
			iter := stubRecordIterator(rangedb.ResultRecord{Record: record, Err: nil})
			ctx := rangedbtest.TimeoutContext(t)
			iter.NextContext(ctx)

			// When
			canContinue := iter.NextContext(ctx)

			// Then
			assert.False(t, canContinue)
			assert.Nil(t, iter.Record())
			assert.Nil(t, iter.Err())
		})

		t.Run("timeout from closed context", func(t *testing.T) {
			// Given
			iter := blockingRecordIterator(t)
			canceledCtx, done := context.WithCancel(context.Background())
			done()

			// When
			canContinue := iter.NextContext(canceledCtx)

			// Then
			assert.False(t, canContinue)
			assert.Nil(t, iter.Record())
			assert.Equal(t, context.Canceled, iter.Err())
		})
	})

	t.Run("only 1 error in stream", func(t *testing.T) {
		t.Run("first call to Next", func(t *testing.T) {
			// Given
			iter := stubRecordIterator(rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("first error"),
			})

			// When
			canContinue := iter.Next()

			// Then
			assert.False(t, canContinue)
			assert.EqualError(t, iter.Err(), "first error")
			assert.Nil(t, iter.Record())
		})

		t.Run("second call to Next retains error for use outside of a for loop", func(t *testing.T) {
			// Given
			iter := stubRecordIterator(rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("first error"),
			})
			iter.Next()

			// When
			canContinue := iter.Next()

			// Then
			assert.False(t, canContinue)
			assert.EqualError(t, iter.Err(), "first error")
			assert.Nil(t, iter.Record())
		})
	})
}

func stubRecordIterator(resultRecords ...rangedb.ResultRecord) rangedb.RecordIterator {
	resultRecordChan := make(chan rangedb.ResultRecord, len(resultRecords))
	for _, resultRecord := range resultRecords {
		resultRecordChan <- resultRecord
	}
	close(resultRecordChan)
	return rangedb.NewRecordIterator(resultRecordChan)
}

func blockingRecordIterator(t *testing.T) rangedb.RecordIterator {
	resultRecordChan := make(chan rangedb.ResultRecord, 1)
	t.Cleanup(func() {
		close(resultRecordChan)
	})
	return rangedb.NewRecordIterator(resultRecordChan)
}
