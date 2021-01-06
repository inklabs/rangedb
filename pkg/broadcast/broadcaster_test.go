package broadcast_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestBroadcast(t *testing.T) {
	t.Run("subscribe to all events", func(t *testing.T) {
		t.Run("broadcasts record to single subscriber", func(t *testing.T) {
			// Given
			broadcaster := broadcast.New(10)
			cleanup(t, broadcaster)
			spySubscriber := newSpySubscriber()
			record := dummyRecord()
			broadcaster.SubscribeAllEvents(spySubscriber)

			// When
			broadcaster.Accept(record)

			// Then
			assertReceivedRecord(t, spySubscriber, record)
		})

		t.Run("broadcasts record to two subscribers", func(t *testing.T) {
			// Given
			broadcaster := broadcast.New(10)
			cleanup(t, broadcaster)
			spySubscriber1 := newSpySubscriber()
			spySubscriber2 := newSpySubscriber()
			record := dummyRecord()
			broadcaster.SubscribeAllEvents(
				spySubscriber1,
				spySubscriber2,
			)

			// When
			broadcaster.Accept(record)

			// Then
			assertReceivedRecord(t, spySubscriber1, record)
			assertReceivedRecord(t, spySubscriber2, record)
		})

		t.Run("times out with first subscriber, and broadcasts record to second subscriber", func(t *testing.T) {
			// Given
			broadcaster := broadcast.New(10)
			cleanup(t, broadcaster)
			broadcaster.SetTimeout(time.Millisecond)
			blockingSubscriber := newBlockingSubscriber()
			spySubscriber := newSpySubscriber()
			record := dummyRecord()
			broadcaster.SubscribeAllEvents(
				blockingSubscriber,
				spySubscriber,
			)

			// When
			broadcaster.Accept(record)

			// Then
			assertReceivedRecord(t, spySubscriber, record)
		})

		t.Run("unsubscribes without sending on closed channel", func(t *testing.T) {
			// Given
			broadcaster := broadcast.New(10)
			cleanup(t, broadcaster)
			spySubscriber := newSpySubscriber()
			record := dummyRecord()
			broadcaster.SubscribeAllEvents(spySubscriber)
			broadcaster.UnsubscribeAllEvents(spySubscriber)

			// When
			broadcaster.Accept(record)

			// Then
			actualRecord, err := spySubscriber.Read()
			assert.Equal(t, context.DeadlineExceeded, err)
			assert.Nil(t, actualRecord)
		})
	})

	t.Run("subscribe to events by single aggregate type", func(t *testing.T) {
		t.Run("broadcasts record to single subscriber", func(t *testing.T) {
			// Given
			broadcaster := broadcast.New(10)
			cleanup(t, broadcaster)
			spySubscriber := newSpySubscriber()
			record := dummyRecord()
			broadcaster.SubscribeAggregateTypes(spySubscriber, record.AggregateType)

			// When
			broadcaster.Accept(record)

			// Then
			assertReceivedRecord(t, spySubscriber, record)
		})

		t.Run("unsubscribes without sending on closed channel", func(t *testing.T) {
			// Given
			broadcaster := broadcast.New(10)
			cleanup(t, broadcaster)
			spySubscriber := newSpySubscriber()
			record := dummyRecord()
			broadcaster.SubscribeAggregateTypes(spySubscriber, record.AggregateType)
			broadcaster.UnsubscribeAggregateTypes(spySubscriber, record.AggregateType)

			// When
			broadcaster.Accept(record)

			// Then
			actualRecord, err := spySubscriber.Read()
			assert.Equal(t, context.DeadlineExceeded, err)
			assert.Nil(t, actualRecord)
		})
	})
}

func assertReceivedRecord(t *testing.T, spySubscriber *spySubscriber, record *rangedb.Record) {
	actualRecord, err := spySubscriber.Read()
	require.NoError(t, err)
	assert.Equal(t, record, actualRecord)
}

func cleanup(t *testing.T, closer io.Closer) {
	t.Cleanup(func() {
		require.NoError(t, closer.Close())
	})
}

type spySubscriber struct {
	bufferedRecords chan *rangedb.Record
}

func newSpySubscriber() *spySubscriber {
	return &spySubscriber{
		bufferedRecords: make(chan *rangedb.Record, 10),
	}
}

func (s *spySubscriber) Receiver() broadcast.SendRecordChan {
	return s.bufferedRecords
}

func (s *spySubscriber) Read() (*rangedb.Record, error) {
	select {
	case <-time.After(100 * time.Millisecond):
		return nil, context.DeadlineExceeded
	case record := <-s.bufferedRecords:
		return record, nil
	}
}

type blockingSubscriber struct {
	UnbufferedRecords chan *rangedb.Record
}

func newBlockingSubscriber() *blockingSubscriber {
	return &blockingSubscriber{
		UnbufferedRecords: make(chan *rangedb.Record),
	}
}

func (s *blockingSubscriber) Receiver() broadcast.SendRecordChan {
	return s.UnbufferedRecords
}

func dummyRecord() *rangedb.Record {
	event := rangedbtest.ThingWasDone{
		ID:     "016b9872688041adb82e1536327bf153",
		Number: 100,
	}
	return &rangedb.Record{
		AggregateType:        event.AggregateType(),
		AggregateID:          event.AggregateID(),
		GlobalSequenceNumber: 0,
		StreamSequenceNumber: 0,
		InsertTimestamp:      0,
		EventID:              "231fdd0542bf48f1abc5d508c16ca66d",
		EventType:            event.EventType(),
		Data:                 event,
		Metadata:             nil,
	}
}
