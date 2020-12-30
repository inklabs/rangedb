package rangedb_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestRecordIterator(t *testing.T) {
	event := &rangedbtest.ThingWasDone{ID: "A", Number: 1}
	record := &rangedb.Record{
		AggregateType:        event.AggregateType(),
		AggregateID:          event.AggregateID(),
		GlobalSequenceNumber: 0,
		StreamSequenceNumber: 0,
		EventType:            event.EventType(),
		InsertTimestamp:      0,
		Data:                 event,
		Metadata:             nil,
	}

	t.Run("only 1 record in the stream", func(t *testing.T) {
		t.Run("first call to next", func(t *testing.T) {
			// Given
			resultRecords := make(chan rangedb.ResultRecord, 1)
			resultRecords <- rangedb.ResultRecord{Record: record, Err: nil}
			close(resultRecords)
			iter := rangedb.NewRecordIterator(resultRecords)

			// When
			canContinue := iter.Next()

			// Then
			assert.True(t, canContinue)
			assert.Equal(t, record, iter.Record())
			assert.Nil(t, iter.Err())
		})

		t.Run("second call to Next", func(t *testing.T) {
			// Given
			resultRecords := make(chan rangedb.ResultRecord, 1)
			resultRecords <- rangedb.ResultRecord{Record: record, Err: nil}
			close(resultRecords)
			iter := rangedb.NewRecordIterator(resultRecords)
			iter.Next()

			// When
			canContinue := iter.Next()

			// Then
			assert.False(t, canContinue)
			assert.Nil(t, iter.Record())
			assert.Nil(t, iter.Err())
		})
	})

	t.Run("only 1 error in stream", func(t *testing.T) {
		t.Run("first call to Next", func(t *testing.T) {
			// Given
			resultRecords := make(chan rangedb.ResultRecord, 1)
			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("first error"),
			}
			close(resultRecords)
			iter := rangedb.NewRecordIterator(resultRecords)

			// When
			canContinue := iter.Next()

			// Then
			assert.False(t, canContinue)
			assert.EqualError(t, iter.Err(), "first error")
			assert.Nil(t, iter.Record())
		})

		t.Run("second call to Next retains error for use outside of a for loop", func(t *testing.T) {
			// Given
			resultRecords := make(chan rangedb.ResultRecord, 1)
			resultRecords <- rangedb.ResultRecord{
				Record: nil,
				Err:    fmt.Errorf("first error"),
			}
			close(resultRecords)
			iter := rangedb.NewRecordIterator(resultRecords)
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
