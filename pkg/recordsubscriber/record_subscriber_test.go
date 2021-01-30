package recordsubscriber_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/pkg/recordsubscriber"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestRecordSubscriber(t *testing.T) {
	t.Run("consumes one existing record", func(t *testing.T) {
		// Given
		totalRecords := 0
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: returnNRecordsIterator(1),
			ConsumeRecord: func(record *rangedb.Record) error {
				totalRecords++
				return nil
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			Ctx:         rangedbtest.TimeoutContext(t),
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.StartFrom(0)
		defer subscriber.Stop()

		// Then
		assert.NoError(t, err)
		assert.Equal(t, 1, totalRecords)
	})

	t.Run("consumes no existing records, subscribes, then consumes a record", func(t *testing.T) {
		// Given
		totalRecords := 0
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: returnNRecordsIterator(0, 1),
			ConsumeRecord: func(record *rangedb.Record) error {
				totalRecords++
				if totalRecords == 1 {
					return nil
				}
				return fmt.Errorf("consume record error")
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			Ctx:         rangedbtest.TimeoutContext(t),
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.StartFrom(0)
		defer subscriber.Stop()

		// Then
		assert.NoError(t, err)
		assert.Equal(t, 1, totalRecords)
	})

	t.Run("consumes no existing records, subscribes and ignores one record, then consumes a record", func(t *testing.T) {
		// Given
		totalRecords := 0
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: returnNRecordsIterator(0, 1),
			ConsumeRecord: func(record *rangedb.Record) error {
				totalRecords++
				return nil
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			Ctx:         rangedbtest.TimeoutContext(t),
		}
		subscriber := recordsubscriber.New(config)
		record := rangedbtest.DummyRecord()
		record.GlobalSequenceNumber = 0
		subscriber.Receiver() <- record

		// When
		err := subscriber.StartFrom(0)
		defer subscriber.Stop()

		// Then
		assert.NoError(t, err)
		assert.Equal(t, 1, totalRecords)
	})

	t.Run("consumes no existing record, consumes record on subscription", func(t *testing.T) {
		// Given
		receivedRecords := make(chan *rangedb.Record, 1)
		defer close(receivedRecords)
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: returnNRecordsIterator(0),
			ConsumeRecord: func(record *rangedb.Record) error {
				receivedRecords <- record
				return nil
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			Ctx:         rangedbtest.TimeoutContext(t),
		}
		subscriber := recordsubscriber.New(config)
		err := subscriber.StartFrom(0)
		defer subscriber.Stop()
		require.NoError(t, err)

		// When
		record := rangedbtest.DummyRecord()
		subscriber.Receiver() <- record

		// Then
		assert.Equal(t, record, <-receivedRecords)
	})

	t.Run("consumes no existing record, errors when consuming record on subscription", func(t *testing.T) {
		// Given
		unsubscribed := make(chan bool)
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: returnNRecordsIterator(0),
			ConsumeRecord: func(record *rangedb.Record) error {
				return fmt.Errorf("consume record error")
			},
			Subscribe: stubSubscribeFunc,
			Unsubscribe: func(subscriber broadcast.RecordSubscriber) {
				unsubscribed <- true
			},
			Ctx: rangedbtest.TimeoutContext(t),
		}
		subscriber := recordsubscriber.New(config)
		err := subscriber.StartFrom(0)
		defer subscriber.Stop()
		require.NoError(t, err)

		// When
		record := rangedbtest.DummyRecord()
		subscriber.Receiver() <- record

		// Then
		assert.True(t, <-unsubscribed)
	})

	t.Run("consume record errors on first call", func(t *testing.T) {
		// Given
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: returnNRecordsIterator(1),
			ConsumeRecord: func(record *rangedb.Record) error {
				return fmt.Errorf("consume record error")
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			Ctx:         rangedbtest.TimeoutContext(t),
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.StartFrom(0)
		defer subscriber.Stop()

		// Then
		assert.EqualError(t, err, "consume record error")
	})

	t.Run("consume record errors on second call", func(t *testing.T) {
		// Given
		calls := 0
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: returnNRecordsIterator(1, 1),
			ConsumeRecord: func(record *rangedb.Record) error {
				if calls > 0 {
					return fmt.Errorf("consume record error")
				}
				calls++
				return nil
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			Ctx:         rangedbtest.TimeoutContext(t),
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.StartFrom(0)
		defer subscriber.Stop()

		// Then
		assert.EqualError(t, err, "consume record error")
	})

	t.Run("errors in get records", func(t *testing.T) {
		// Given
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: func(globalSequenceNumber uint64) rangedb.RecordIterator {
				recordResults := make(chan rangedb.ResultRecord, 1)
				recordResults <- rangedb.ResultRecord{
					Record: rangedbtest.DummyRecord(),
					Err:    fmt.Errorf("get record error"),
				}
				close(recordResults)
				return rangedb.NewRecordIterator(recordResults)
			},
			ConsumeRecord: func(record *rangedb.Record) error {
				return nil
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			Ctx:         rangedbtest.TimeoutContext(t),
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.StartFrom(0)
		defer subscriber.Stop()

		// Then
		assert.EqualError(t, err, "get record error")
	})

	t.Run("stops from closed context", func(t *testing.T) {
		// Given
		closedContext, done := context.WithCancel(rangedbtest.TimeoutContext(t))
		done()
		unsubscribed := make(chan bool)
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: func(globalSequenceNumber uint64) rangedb.RecordIterator {
				recordResults := make(chan rangedb.ResultRecord)
				close(recordResults)
				return rangedb.NewRecordIterator(recordResults)
			},
			ConsumeRecord: func(record *rangedb.Record) error {
				return nil
			},
			Subscribe: stubSubscribeFunc,
			Unsubscribe: func(subscriber broadcast.RecordSubscriber) {
				unsubscribed <- true
			},
			Ctx: closedContext,
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.StartFrom(0)
		defer subscriber.Stop()

		// Then
		require.NoError(t, err)
		assert.True(t, <-unsubscribed)
	})

	t.Run("calling stop is idempotent, then start errors", func(t *testing.T) {
		// Given
		totalRecords := 0
		config := recordsubscriber.Config{
			BufferSize: 10,
			GetRecords: returnNRecordsIterator(1),
			ConsumeRecord: func(record *rangedb.Record) error {
				totalRecords++
				return nil
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			Ctx:         rangedbtest.TimeoutContext(t),
		}
		subscriber := recordsubscriber.New(config)
		subscriber.Stop()
		subscriber.Stop()

		// When
		err := subscriber.Start()

		// Then
		assert.Equal(t, context.Canceled, err)
	})
}

func returnNRecordsIterator(totalPerCall ...uint64) func(globalSequenceNumber uint64) rangedb.RecordIterator {
	cnt := 0
	nextGlobalSequenceNumber := uint64(0)
	return func(globalSequenceNumber uint64) rangedb.RecordIterator {
		n := uint64(0)
		if cnt < len(totalPerCall) {
			n = totalPerCall[cnt]
		}
		cnt++
		recordResults := make(chan rangedb.ResultRecord, n)
		for i := uint64(0); i < n; i++ {
			record := rangedbtest.DummyRecord()
			record.GlobalSequenceNumber = nextGlobalSequenceNumber
			recordResults <- rangedb.ResultRecord{
				Record: record,
				Err:    nil,
			}
			nextGlobalSequenceNumber++
		}
		close(recordResults)
		return rangedb.NewRecordIterator(recordResults)
	}
}

func stubSubscribeFunc(_ broadcast.RecordSubscriber) {}
