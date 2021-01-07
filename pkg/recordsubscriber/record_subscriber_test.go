package recordsubscriber_test

import (
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
		doneChan := make(chan struct{})
		totalRecords := 0
		config := recordsubscriber.Config{
			BufLen:     10,
			GetRecords: returnNRecordsIterator(1),
			ConsumeRecord: func(record *rangedb.Record) error {
				totalRecords++
				return nil
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			DoneChan:    doneChan,
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.Start()
		defer subscriber.Stop()

		// Then
		assert.NoError(t, err)
		assert.Equal(t, 1, totalRecords)
	})

	t.Run("consumes no existing records, subscribes, then consumes a record", func(t *testing.T) {
		// Given
		doneChan := make(chan struct{})
		totalRecords := 0
		config := recordsubscriber.Config{
			BufLen:     10,
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
			DoneChan:    doneChan,
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.Start()
		defer subscriber.Stop()

		// Then
		assert.NoError(t, err)
		assert.Equal(t, 1, totalRecords)
	})

	t.Run("consumes no existing records, subscribes and ignores one record, then consumes a record", func(t *testing.T) {
		// Given
		doneChan := make(chan struct{})
		totalRecords := 0
		config := recordsubscriber.Config{
			BufLen:     10,
			GetRecords: returnNRecordsIterator(0, 1),
			ConsumeRecord: func(record *rangedb.Record) error {
				totalRecords++
				return nil
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			DoneChan:    doneChan,
		}
		subscriber := recordsubscriber.New(config)
		record := rangedbtest.DummyRecord()
		record.GlobalSequenceNumber = 0
		subscriber.Receiver() <- record

		// When
		err := subscriber.Start()
		defer subscriber.Stop()

		// Then
		assert.NoError(t, err)
		assert.Equal(t, 1, totalRecords)
	})

	t.Run("consumes no existing record, consumes record on subscription", func(t *testing.T) {
		// Given
		doneChan := make(chan struct{})
		receivedRecords := make(chan *rangedb.Record, 1)
		defer close(receivedRecords)
		config := recordsubscriber.Config{
			BufLen:     10,
			GetRecords: returnNRecordsIterator(0),
			ConsumeRecord: func(record *rangedb.Record) error {
				receivedRecords <- record
				return nil
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			DoneChan:    doneChan,
		}
		subscriber := recordsubscriber.New(config)
		err := subscriber.Start()
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
		doneChan := make(chan struct{})
		unsubscribed := false
		waitChan := make(chan struct{})
		config := recordsubscriber.Config{
			BufLen:     10,
			GetRecords: returnNRecordsIterator(0),
			ConsumeRecord: func(record *rangedb.Record) error {
				defer func() {
					waitChan <- struct{}{}
				}()

				return fmt.Errorf("consume record error")
			},
			Subscribe: stubSubscribeFunc,
			Unsubscribe: func(subscriber broadcast.RecordSubscriber) {
				unsubscribed = true
			},
			DoneChan: doneChan,
		}
		subscriber := recordsubscriber.New(config)
		err := subscriber.Start()
		defer subscriber.Stop()
		require.NoError(t, err)

		// When
		record := rangedbtest.DummyRecord()
		subscriber.Receiver() <- record

		// Then
		<-waitChan
		assert.True(t, unsubscribed)
	})

	t.Run("consume record errors on first call", func(t *testing.T) {
		// Given
		doneChan := make(chan struct{})
		config := recordsubscriber.Config{
			BufLen:     10,
			GetRecords: returnNRecordsIterator(1),
			ConsumeRecord: func(record *rangedb.Record) error {
				return fmt.Errorf("consume record error")
			},
			Subscribe:   stubSubscribeFunc,
			Unsubscribe: stubSubscribeFunc,
			DoneChan:    doneChan,
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.Start()
		defer subscriber.Stop()

		// Then
		assert.EqualError(t, err, "consume record error")
	})

	t.Run("consume record errors on second call", func(t *testing.T) {
		// Given
		doneChan := make(chan struct{})
		calls := 0
		config := recordsubscriber.Config{
			BufLen:     10,
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
			DoneChan:    doneChan,
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.Start()
		defer subscriber.Stop()

		// Then
		assert.EqualError(t, err, "consume record error")
	})

	t.Run("errors in get records", func(t *testing.T) {
		// Given
		doneChan := make(chan struct{})
		config := recordsubscriber.Config{
			BufLen: 10,
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
			DoneChan:    doneChan,
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.Start()
		defer subscriber.Stop()

		// Then
		assert.EqualError(t, err, "get record error")
	})

	t.Run("stops from done channel", func(t *testing.T) {
		// Given
		waitChan := make(chan struct{})
		doneChan := make(chan struct{})
		close(doneChan)
		unsubscribed := false
		config := recordsubscriber.Config{
			BufLen: 10,
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
				unsubscribed = true
				waitChan <- struct{}{}
			},
			DoneChan: doneChan,
		}
		subscriber := recordsubscriber.New(config)

		// When
		err := subscriber.Start()
		defer subscriber.Stop()

		// Then
		<-waitChan
		require.NoError(t, err)
		assert.True(t, unsubscribed)
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
