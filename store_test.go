package rangedb_test

import (
	"context"
	"encoding/json"
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
		const aggregateID = "4a4c36de56f94ae29f0b89497f2ba72f"
		event1 := rangedbtest.ThingWasDone{ID: aggregateID, Number: 1}
		event2 := rangedbtest.ThingWasDone{ID: aggregateID, Number: 2}
		ctx := rangedbtest.TimeoutContext(t)
		require.NoError(t, inMemoryStore.Save(ctx,
			&rangedb.EventRecord{Event: event1},
			&rangedb.EventRecord{Event: event2},
		))
		subscriber := rangedbtest.NewTotalEventsSubscriber()

		// When
		rangedb.ReplayEvents(ctx, inMemoryStore, 0, subscriber)

		// Then
		assert.Equal(t, 2, subscriber.TotalEvents())
	})

	t.Run("replays from the second event", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		const aggregateID = "67db313520884005b2657dfebfac74ae"
		event1 := rangedbtest.ThingWasDone{ID: aggregateID, Number: 1}
		event2 := rangedbtest.ThingWasDone{ID: aggregateID, Number: 2}
		ctx := rangedbtest.TimeoutContext(t)
		require.NoError(t, inMemoryStore.Save(ctx,
			&rangedb.EventRecord{Event: event1},
			&rangedb.EventRecord{Event: event2},
		))
		subscriber := rangedbtest.NewTotalEventsSubscriber()

		// When
		rangedb.ReplayEvents(ctx, inMemoryStore, 1, subscriber)

		// Then
		assert.Equal(t, 1, subscriber.TotalEvents())
	})

	t.Run("stops from context.Done", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		const aggregateID = "67db313520884005b2657dfebfac74ae"
		event1 := rangedbtest.ThingWasDone{ID: aggregateID, Number: 1}
		event2 := rangedbtest.ThingWasDone{ID: aggregateID, Number: 2}
		saveCtx := rangedbtest.TimeoutContext(t)
		require.NoError(t, inMemoryStore.Save(saveCtx,
			&rangedb.EventRecord{Event: event1},
			&rangedb.EventRecord{Event: event2},
		))
		subscriber := rangedbtest.NewTotalEventsSubscriber()
		ctx, done := context.WithCancel(rangedbtest.TimeoutContext(t))
		done()

		// When
		rangedb.ReplayEvents(ctx, inMemoryStore, 1, subscriber)

		// Then
		assert.Equal(t, 0, subscriber.TotalEvents())
	})

	t.Run("stops after first record from context.Done", func(t *testing.T) {
		// TODO
	})
}

func TestReadNRecords(t *testing.T) {
	t.Run("reads 2 records from a channel with 3 records", func(t *testing.T) {
		// Given
		resultRecords := make(chan rangedb.ResultRecord, 3)
		resultRecords <- rangedb.ResultRecord{Record: getRecord(0)}
		resultRecords <- rangedb.ResultRecord{Record: getRecord(1)}
		resultRecords <- rangedb.ResultRecord{Record: getRecord(2)}
		close(resultRecords)
		recordIterator := rangedb.NewRecordIterator(resultRecords)

		// When
		records := rangedb.ReadNRecords(
			2,
			func() (rangedb.RecordIterator, context.CancelFunc) {
				return recordIterator, func() {}
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

func TestRawEvent(t *testing.T) {
	// When
	const aggregateID = "e9bf256fce3449c19864b171a9593cb0"
	rawEvent := rangedb.NewRawEvent(
		"thing",
		aggregateID,
		"ThingWasDone",
		map[string]interface{}{
			"number": 100,
		},
	)

	// Then
	assert.Equal(t, aggregateID, rawEvent.AggregateID())
	assert.Equal(t, "thing", rawEvent.AggregateType())
	assert.Equal(t, "ThingWasDone", rawEvent.EventType())
	eventJSON, err := json.Marshal(rawEvent)
	require.NoError(t, err)
	assert.JSONEq(t, `{"number":100}`, string(eventJSON))
}

type dummyRecordSubscriber struct {
	GlobalSequenceNumber uint64
}

func (f *dummyRecordSubscriber) broadcast(r *rangedb.Record) {
	f.GlobalSequenceNumber = r.GlobalSequenceNumber
}
