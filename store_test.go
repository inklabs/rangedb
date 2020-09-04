package rangedb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_GetStream_CombinesAggregateTypeAndId(t *testing.T) {
	// Given
	aggregateType := "resource-owner"
	aggregateID := "8e91008eb3a84a3da6f53481ffa9ea88"

	// When
	stream := rangedb.GetStream(aggregateType, aggregateID)

	// Then
	assert.Equal(t, "resource-owner!8e91008eb3a84a3da6f53481ffa9ea88", stream)
}

func Test_ParseStream_ReturnsAggregateTypeAndId(t *testing.T) {
	// Given
	streamName := "resource-owner!8e91008eb3a84a3da6f53481ffa9ea88"

	// When
	aggregateType, aggregateID := rangedb.ParseStream(streamName)

	// Then
	assert.Equal(t, "resource-owner", aggregateType)
	assert.Equal(t, "8e91008eb3a84a3da6f53481ffa9ea88", aggregateID)
}

func Test_GetEventStream_ReturnsStreamFromMessage(t *testing.T) {
	// Given
	event := rangedbtest.ThingWasDone{
		ID: "e2c2b4fa64344d17984fc53631f3c462",
	}

	// When
	stream := rangedb.GetEventStream(event)

	// Then
	assert.Equal(t, "thing!e2c2b4fa64344d17984fc53631f3c462", stream)
}

func Test_ReplayEvents(t *testing.T) {
	t.Run("replays from the first event", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		event1 := rangedbtest.ThingWasDone{ID: "A", Number: 1}
		event2 := rangedbtest.ThingWasDone{ID: "A", Number: 2}
		require.NoError(t, inMemoryStore.Save(event1, nil))
		require.NoError(t, inMemoryStore.Save(event2, nil))
		subscriber := rangedbtest.NewTotalEventsSubscriber()

		// When
		rangedb.ReplayEvents(inMemoryStore, 0, subscriber)

		// Then
		assert.Equal(t, 2, subscriber.TotalEvents())
	})

	t.Run("replays from the second event", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		event1 := rangedbtest.ThingWasDone{ID: "A", Number: 1}
		event2 := rangedbtest.ThingWasDone{ID: "A", Number: 2}
		require.NoError(t, inMemoryStore.Save(event1, nil))
		require.NoError(t, inMemoryStore.Save(event2, nil))
		subscriber := rangedbtest.NewTotalEventsSubscriber()

		// When
		rangedb.ReplayEvents(inMemoryStore, 1, subscriber)

		// Then
		assert.Equal(t, 1, subscriber.TotalEvents())
	})
}

func TestReadNRecords(t *testing.T) {
	t.Run("reads 2 records from a channel with 3 records", func(t *testing.T) {
		// Given
		recordsChannel := make(chan *rangedb.Record, 10)
		recordsChannel <- getRecord(0)
		recordsChannel <- getRecord(1)
		recordsChannel <- getRecord(2)

		// When
		records := rangedb.ReadNRecords(
			2,
			func(ctx context.Context) <-chan *rangedb.Record {
				return recordsChannel
			},
		)

		// Then
		require.Len(t, records, 2)
		assert.Equal(t, uint64(0), records[0].GlobalSequenceNumber)
		assert.Equal(t, uint64(1), records[1].GlobalSequenceNumber)
	})
}

func TestRecordSubscriberFunc_Accept(t *testing.T) {
	// Given
	dummyRecordSubscriber := dummyRecordSubscriber{}
	f := rangedb.RecordSubscriberFunc(dummyRecordSubscriber.broadcast)
	record := getRecord(5)

	// When
	f.Accept(record)

	// Then
	assert.Equal(t, uint64(5), dummyRecordSubscriber.GlobalSequenceNumber)
}

type dummyRecordSubscriber struct {
	GlobalSequenceNumber uint64
}

func (f *dummyRecordSubscriber) broadcast(r *rangedb.Record) {
	f.GlobalSequenceNumber = r.GlobalSequenceNumber
}
