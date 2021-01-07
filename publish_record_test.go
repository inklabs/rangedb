package rangedb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestPublishRecordOrCancel(t *testing.T) {
	t.Run("returns record", func(t *testing.T) {
		// Given
		resultRecords := make(chan rangedb.ResultRecord, 1)
		defer close(resultRecords)
		record := rangedbtest.DummyRecord()
		ctx := rangedbtest.TimeoutContext(t)
		timeout := time.Millisecond

		// When
		isSuccess := rangedb.PublishRecordOrCancel(ctx, resultRecords, record, timeout)

		// Then
		actualResult := <-resultRecords
		assert.True(t, isSuccess)
		assert.Nil(t, actualResult.Err)
		assert.Equal(t, record, actualResult.Record)
	})

	t.Run("first canceled context and times out", func(t *testing.T) {
		// Given
		resultRecords := make(chan rangedb.ResultRecord)
		defer close(resultRecords)
		record := rangedbtest.DummyRecord()
		ctx, done := context.WithCancel(rangedbtest.TimeoutContext(t))
		done()
		timeout := time.Millisecond

		// When
		isSuccess := rangedb.PublishRecordOrCancel(ctx, resultRecords, record, timeout)

		// Then
		assert.False(t, isSuccess)
	})

	t.Run("first canceled context and returns error", func(t *testing.T) {
		// Given
		resultRecords := make(chan rangedb.ResultRecord, 1)
		defer close(resultRecords)
		record := rangedbtest.DummyRecord()
		ctx, done := context.WithCancel(rangedbtest.TimeoutContext(t))
		done()
		timeout := time.Millisecond

		// When
		isSuccess := rangedb.PublishRecordOrCancel(ctx, resultRecords, record, timeout)

		// Then
		actualResult := <-resultRecords
		assert.False(t, isSuccess)
		assert.Equal(t, context.Canceled, actualResult.Err)
		assert.Nil(t, actualResult.Record)
	})

	t.Run("second canceled context times out", func(t *testing.T) {
		// Given
		resultRecords := make(chan rangedb.ResultRecord)
		defer close(resultRecords)
		record := rangedbtest.DummyRecord()
		ctx, done := context.WithTimeout(rangedbtest.TimeoutContext(t), 10*time.Millisecond)
		defer done()
		timeout := time.Millisecond

		// When
		isSuccess := rangedb.PublishRecordOrCancel(ctx, resultRecords, record, timeout)

		// Then
		assert.False(t, isSuccess)
	})

	t.Run("second canceled context and returns error", func(t *testing.T) {
		// Given
		resultRecords := make(chan rangedb.ResultRecord)
		defer close(resultRecords)
		record := rangedbtest.DummyRecord()
		ctx, done := context.WithTimeout(rangedbtest.TimeoutContext(t), 10*time.Millisecond)
		defer done()
		actualResults := make(chan rangedb.ResultRecord)
		defer close(actualResults)
		time.AfterFunc(50*time.Millisecond, func() {
			actualResults <- <-resultRecords
		})
		timeout := 100 * time.Millisecond

		// When
		isSuccess := rangedb.PublishRecordOrCancel(ctx, resultRecords, record, timeout)

		// Then
		actualResult := <-actualResults
		assert.False(t, isSuccess)
		assert.Equal(t, context.DeadlineExceeded, actualResult.Err)
		assert.Nil(t, actualResult.Record)
	})
}
