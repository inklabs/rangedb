package rangedbserver_test

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbserver"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestRangeDBServer_WithFourEventsSaved(t *testing.T) {
	// Given
	uuid := rangedbtest.NewSeededUUIDGenerator()
	store := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
		inmemorystore.WithUUIDGenerator(uuid),
	)
	const (
		aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
		aggregateID2 = "5b36ae984b724685917b69ae47968be1"
		aggregateID3 = "9bc181144cef4fd19da1f32a17363997"
	)

	event1 := rangedbtest.ThingWasDone{ID: aggregateID1, Number: 100}
	event2 := rangedbtest.ThingWasDone{ID: aggregateID1, Number: 200}
	event3 := rangedbtest.ThingWasDone{ID: aggregateID2, Number: 300}
	event4 := rangedbtest.AnotherWasComplete{ID: aggregateID3}
	rangedbtest.BlockingSaveEvents(t, store,
		&rangedb.EventRecord{Event: event1},
		&rangedb.EventRecord{Event: event2},
	)
	rangedbtest.BlockingSaveEvents(t, store,
		&rangedb.EventRecord{Event: event3},
	)
	rangedbtest.BlockingSaveEvents(t, store,
		&rangedb.EventRecord{Event: event4},
	)

	t.Run("get all events", func(t *testing.T) {
		// Given
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		eventsRequest := &rangedbpb.EventsRequest{
			GlobalSequenceNumber: 1,
		}

		// When
		events, err := rangeDBClient.Events(ctx, eventsRequest)
		require.NoError(t, err)

		// Then
		actualRecords := make(chan *rangedbpb.Record, 10)
		for i := 0; i < 4; i++ {
			record, err := events.Recv()
			require.NoError(t, err)
			actualRecords <- record
		}
		close(actualRecords)

		expectedRecord1 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			InsertTimestamp:      0,
			EventID:              uuid.Get(1),
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":100}`,
			Metadata:             "null",
		}
		expectedRecord2 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 2,
			StreamSequenceNumber: 2,
			InsertTimestamp:      1,
			EventID:              uuid.Get(2),
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":200}`,
			Metadata:             "null",
		}
		expectedRecord3 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID2,
			GlobalSequenceNumber: 3,
			StreamSequenceNumber: 1,
			InsertTimestamp:      2,
			EventID:              uuid.Get(3),
			EventType:            "ThingWasDone",
			Data:                 `{"id":"5b36ae984b724685917b69ae47968be1","number":300}`,
			Metadata:             "null",
		}
		expectedRecord4 := &rangedbpb.Record{
			AggregateType:        "another",
			AggregateID:          aggregateID3,
			GlobalSequenceNumber: 4,
			StreamSequenceNumber: 1,
			InsertTimestamp:      3,
			EventID:              uuid.Get(4),
			EventType:            "AnotherWasComplete",
			Data:                 `{"id":"9bc181144cef4fd19da1f32a17363997"}`,
			Metadata:             "null",
		}
		assertPbRecordsEqual(t, expectedRecord1, <-actualRecords)
		assertPbRecordsEqual(t, expectedRecord2, <-actualRecords)
		assertPbRecordsEqual(t, expectedRecord3, <-actualRecords)
		assertPbRecordsEqual(t, expectedRecord4, <-actualRecords)
		assert.Equal(t, (*rangedbpb.Record)(nil), <-actualRecords)
	})

	t.Run("total events in stream", func(t *testing.T) {
		// Given
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		request := &rangedbpb.TotalEventsInStreamRequest{
			StreamName: rangedb.GetEventStream(event1),
		}

		// When
		response, err := rangeDBClient.TotalEventsInStream(ctx, request)
		require.NoError(t, err)

		// Then
		assert.Equal(t, uint64(2), response.TotalEvents)
	})
}

func TestRangeDBServer_SubscribeToLiveEvents(t *testing.T) {
	t.Run("subscribes to real time events, ignoring previous events", func(t *testing.T) {
		// Given
		uuid := rangedbtest.NewSeededUUIDGenerator()
		const (
			aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
			aggregateID2 = "5b36ae984b724685917b69ae47968be1"
		)
		store := inmemorystore.New(
			inmemorystore.WithClock(sequentialclock.New()),
			inmemorystore.WithUUIDGenerator(uuid),
		)
		ctx := rangedbtest.TimeoutContext(t)
		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: aggregateID1, Number: 100}},
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: aggregateID1, Number: 200}},
		)
		rangeDBClient := getClient(t, store)
		request := &rangedbpb.SubscribeToLiveEventsRequest{}

		// When
		events, err := rangeDBClient.SubscribeToLiveEvents(ctx, request)

		// Then
		require.NoError(t, err)
		time.Sleep(time.Millisecond * 5)
		actualRecords := make(chan *rangedbpb.Record, 10)

		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: aggregateID1, Number: 300}},
		)
		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: aggregateID2}},
		)

		for i := 0; i < 2; i++ {
			record, err := events.Recv()
			require.NoError(t, err)
			actualRecords <- record
		}
		close(actualRecords)

		expectedRecord1 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 3,
			StreamSequenceNumber: 3,
			InsertTimestamp:      2,
			EventID:              uuid.Get(3),
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":300}`,
			Metadata:             "null",
		}
		expectedRecord2 := &rangedbpb.Record{
			AggregateType:        "another",
			AggregateID:          aggregateID2,
			GlobalSequenceNumber: 4,
			StreamSequenceNumber: 1,
			InsertTimestamp:      3,
			EventID:              uuid.Get(4),
			EventType:            "AnotherWasComplete",
			Data:                 `{"id":"5b36ae984b724685917b69ae47968be1"}`,
			Metadata:             "null",
		}
		assertPbRecordsEqual(t, expectedRecord1, <-actualRecords)
		assertPbRecordsEqual(t, expectedRecord2, <-actualRecords)
		assert.Equal(t, (*rangedbpb.Record)(nil), <-actualRecords)
	})
}

func TestRangeDBServer_SubscribeToEvents(t *testing.T) {
	t.Run("subscribes to all events starting from the 2nd event", func(t *testing.T) {
		// Given
		uuid := rangedbtest.NewSeededUUIDGenerator()
		const (
			aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
			aggregateID2 = "5b36ae984b724685917b69ae47968be1"
		)
		store := inmemorystore.New(
			inmemorystore.WithClock(sequentialclock.New()),
			inmemorystore.WithUUIDGenerator(uuid),
		)
		ctx := rangedbtest.TimeoutContext(t)
		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: aggregateID1, Number: 100}},
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: aggregateID1, Number: 200}},
		)
		rangeDBClient := getClient(t, store)
		request := &rangedbpb.SubscribeToEventsRequest{
			GlobalSequenceNumber: 2,
		}

		// When
		events, err := rangeDBClient.SubscribeToEvents(ctx, request)
		require.NoError(t, err)

		// Then
		actualRecords := make(chan *rangedbpb.Record, 10)

		record, err := events.Recv()
		require.NoError(t, err)
		actualRecords <- record

		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: aggregateID1, Number: 300}},
		)
		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: aggregateID2}},
		)

		for i := 0; i < 2; i++ {
			record, err := events.Recv()
			require.NoError(t, err)
			actualRecords <- record
		}
		close(actualRecords)

		expectedRecord1 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 2,
			StreamSequenceNumber: 2,
			InsertTimestamp:      1,
			EventID:              uuid.Get(2),
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":200}`,
			Metadata:             "null",
		}
		expectedRecord2 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 3,
			StreamSequenceNumber: 3,
			InsertTimestamp:      2,
			EventID:              uuid.Get(3),
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":300}`,
			Metadata:             "null",
		}
		expectedRecord3 := &rangedbpb.Record{
			AggregateType:        "another",
			AggregateID:          aggregateID2,
			GlobalSequenceNumber: 4,
			StreamSequenceNumber: 1,
			InsertTimestamp:      3,
			EventID:              uuid.Get(4),
			EventType:            "AnotherWasComplete",
			Data:                 `{"id":"5b36ae984b724685917b69ae47968be1"}`,
			Metadata:             "null",
		}
		assertPbRecordsEqual(t, expectedRecord1, <-actualRecords)
		assertPbRecordsEqual(t, expectedRecord2, <-actualRecords)
		assertPbRecordsEqual(t, expectedRecord3, <-actualRecords)
		assert.Equal(t, (*rangedbpb.Record)(nil), <-actualRecords)
	})
}

func TestRangeDBServer_SubscribeToEventsByAggregateType(t *testing.T) {
	t.Run("subscribes to events by aggregate type starting from the 2nd event", func(t *testing.T) {
		// Given
		uuid := rangedbtest.NewSeededUUIDGenerator()
		const (
			aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
			aggregateID2 = "5b36ae984b724685917b69ae47968be1"
		)

		store := inmemorystore.New(
			inmemorystore.WithClock(sequentialclock.New()),
			inmemorystore.WithUUIDGenerator(uuid),
		)
		ctx := rangedbtest.TimeoutContext(t)
		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: aggregateID1, Number: 100}},
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: aggregateID1, Number: 200}},
		)
		rangeDBClient := getClient(t, store)
		request := &rangedbpb.SubscribeToEventsByAggregateTypeRequest{
			GlobalSequenceNumber: 2,
			AggregateTypes:       []string{"thing", "another"},
		}

		// When
		events, err := rangeDBClient.SubscribeToEventsByAggregateType(ctx, request)
		require.NoError(t, err)

		// Then
		actualRecords := make(chan *rangedbpb.Record, 10)

		record, err := events.Recv()
		require.NoError(t, err)
		actualRecords <- record

		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: aggregateID1, Number: 300}},
		)
		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.ThatWasDone{ID: aggregateID2}},
		)
		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: aggregateID2}},
		)

		for i := 0; i < 2; i++ {
			record, err := events.Recv()
			require.NoError(t, err)
			actualRecords <- record
		}
		close(actualRecords)

		expectedRecord1 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 2,
			StreamSequenceNumber: 2,
			InsertTimestamp:      1,
			EventID:              uuid.Get(2),
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":200}`,
			Metadata:             "null",
		}
		expectedRecord2 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 3,
			StreamSequenceNumber: 3,
			InsertTimestamp:      2,
			EventID:              uuid.Get(3),
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":300}`,
			Metadata:             "null",
		}
		expectedRecord3 := &rangedbpb.Record{
			AggregateType:        "another",
			AggregateID:          aggregateID2,
			GlobalSequenceNumber: 5,
			StreamSequenceNumber: 1,
			InsertTimestamp:      4,
			EventID:              uuid.Get(5),
			EventType:            "AnotherWasComplete",
			Data:                 `{"id":"5b36ae984b724685917b69ae47968be1"}`,
			Metadata:             "null",
		}
		assertPbRecordsEqual(t, expectedRecord1, <-actualRecords)
		assertPbRecordsEqual(t, expectedRecord2, <-actualRecords)
		assertPbRecordsEqual(t, expectedRecord3, <-actualRecords)
		assert.Equal(t, (*rangedbpb.Record)(nil), <-actualRecords)
	})
}

func TestRangeDBServer_Save(t *testing.T) {
	t.Run("saves 2 events", func(t *testing.T) {
		// Given
		uuid := rangedbtest.NewSeededUUIDGenerator()
		store := inmemorystore.New(
			inmemorystore.WithClock(sequentialclock.New()),
			inmemorystore.WithUUIDGenerator(uuid),
		)
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		const aggregateID = "69ac39e8df52419c98fe71e2f8692b72"
		request := &rangedbpb.SaveRequest{
			AggregateType: "thing",
			AggregateID:   aggregateID,
			Events: []*rangedbpb.Event{
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"69ac39e8df52419c98fe71e2f8692b72","number":100}`,
					Metadata: "",
				},
				{
					Type: "ThingWasDone",
					Data: `{"id":"69ac39e8df52419c98fe71e2f8692b72","number":200}`,
				},
			},
		}

		// When
		response, err := rangeDBClient.Save(ctx, request)
		require.NoError(t, err)

		// Then
		assert.Equal(t, uint32(2), response.EventsSaved)
		assert.Equal(t, uint64(2), response.LastStreamSequenceNumber)
		recordIterator := store.Events(ctx, 0)
		rangedbtest.AssertRecordsInIterator(t, recordIterator,
			&rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          aggregateID,
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				InsertTimestamp:      0,
				EventID:              uuid.Get(1),
				EventType:            "ThingWasDone",
				Data: map[string]interface{}{
					"id":     aggregateID,
					"number": json.Number("100"),
				},
				Metadata: nil,
			},
			&rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          aggregateID,
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 2,
				InsertTimestamp:      1,
				EventID:              uuid.Get(2),
				EventType:            "ThingWasDone",
				Data: map[string]interface{}{
					"id":     aggregateID,
					"number": json.Number("200"),
				},
				Metadata: nil,
			},
		)
	})

	t.Run("fails on 2nd from invalid event data", func(t *testing.T) {
		// Given
		uuid := rangedbtest.NewSeededUUIDGenerator()
		store := inmemorystore.New(
			inmemorystore.WithClock(sequentialclock.New()),
			inmemorystore.WithUUIDGenerator(uuid),
		)
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		const aggregateID = "7501649949fc46b389772a3e7df6c563"
		request := &rangedbpb.SaveRequest{
			AggregateType: "thing",
			AggregateID:   aggregateID,
			Events: []*rangedbpb.Event{
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"7501649949fc46b389772a3e7df6c563","number":100}`,
					Metadata: "",
				},
				{
					Type: "ThingWasDone",
					Data: `{invalid-json`,
				},
			},
		}

		// When
		_, err := rangeDBClient.Save(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = InvalidArgument desc = unable to read event data: invalid character 'i' looking for beginning of object key string")
		errorResponse, ok := status.Convert(err).Details()[0].(*rangedbpb.SaveFailureResponse)
		require.True(t, ok)
		assert.Equal(t, "unable to read event data: invalid character 'i' looking for beginning of object key string", errorResponse.Message)
	})

	t.Run("fails on 2nd from invalid event metadata", func(t *testing.T) {
		// Given
		uuid := rangedbtest.NewSeededUUIDGenerator()
		store := inmemorystore.New(
			inmemorystore.WithClock(sequentialclock.New()),
			inmemorystore.WithUUIDGenerator(uuid),
		)
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		const aggregateID = "340e6f63af8b4fcc8d66ba02adb7d92d"
		request := &rangedbpb.SaveRequest{
			AggregateType: "thing",
			AggregateID:   aggregateID,
			Events: []*rangedbpb.Event{
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"340e6f63af8b4fcc8d66ba02adb7d92d","number":100}`,
					Metadata: "",
				},
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"340e6f63af8b4fcc8d66ba02adb7d92d","number":100}`,
					Metadata: `{invalid-json`,
				},
			},
		}

		// When
		_, err := rangeDBClient.Save(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = InvalidArgument desc = unable to read event metadata: invalid character 'i' looking for beginning of object key string")
		errorResponse, ok := status.Convert(err).Details()[0].(*rangedbpb.SaveFailureResponse)
		require.True(t, ok)
		assert.Equal(t, "unable to read event metadata: invalid character 'i' looking for beginning of object key string", errorResponse.Message)
	})

	t.Run("fails from failing store", func(t *testing.T) {
		// Given
		store := rangedbtest.NewFailingEventStore()
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		const aggregateID = "1193540fc0f3485b8dee57ae57fc745b"
		request := &rangedbpb.SaveRequest{
			AggregateType: "thing",
			AggregateID:   aggregateID,
			Events: []*rangedbpb.Event{
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"1193540fc0f3485b8dee57ae57fc745b","number":100}`,
					Metadata: "",
				},
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"1193540fc0f3485b8dee57ae57fc745b","number":100}`,
					Metadata: "",
				},
			},
		}

		// When
		_, err := rangeDBClient.Save(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = Internal desc = unable to save to store: failingEventStore.Save")
		errorResponse, ok := status.Convert(err).Details()[0].(*rangedbpb.SaveFailureResponse)
		require.True(t, ok)
		assert.Equal(t, "unable to save to store: failingEventStore.Save", errorResponse.Message)
	})
}

func TestRangeDBServer_OptimisticSave(t *testing.T) {
	t.Run("saves 2 events", func(t *testing.T) {
		// Given
		uuid := rangedbtest.NewSeededUUIDGenerator()
		store := inmemorystore.New(
			inmemorystore.WithClock(sequentialclock.New()),
			inmemorystore.WithUUIDGenerator(uuid),
		)
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		const aggregateID = "c46671e5c22b478e9f5ccb5185910e5d"
		request := &rangedbpb.OptimisticSaveRequest{
			ExpectedStreamSequenceNumber: 0,
			AggregateType:                "thing",
			AggregateID:                  aggregateID,
			Events: []*rangedbpb.Event{
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"c46671e5c22b478e9f5ccb5185910e5d","number":100}`,
					Metadata: "",
				},
				{
					Type: "ThingWasDone",
					Data: `{"id":"c46671e5c22b478e9f5ccb5185910e5d","number":200}`,
				},
			},
		}

		// When
		response, err := rangeDBClient.OptimisticSave(ctx, request)
		require.NoError(t, err)

		// Then
		assert.Equal(t, uint32(2), response.EventsSaved)
		recordIterator := store.Events(ctx, 0)
		rangedbtest.AssertRecordsInIterator(t, recordIterator,
			&rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          aggregateID,
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				InsertTimestamp:      0,
				EventID:              uuid.Get(1),
				EventType:            "ThingWasDone",
				Data: map[string]interface{}{
					"id":     aggregateID,
					"number": json.Number("100"),
				},
				Metadata: nil,
			},
			&rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          aggregateID,
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 2,
				InsertTimestamp:      1,
				EventID:              uuid.Get(2),
				EventType:            "ThingWasDone",
				Data: map[string]interface{}{
					"id":     aggregateID,
					"number": json.Number("200"),
				},
				Metadata: nil,
			},
		)
	})

	t.Run("fails on 2nd from invalid event data", func(t *testing.T) {
		// Given
		uuid := rangedbtest.NewSeededUUIDGenerator()
		store := inmemorystore.New(
			inmemorystore.WithClock(sequentialclock.New()),
			inmemorystore.WithUUIDGenerator(uuid),
		)
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		const aggregateID = "618d9342a96c4a7f811ff5264e2ea813"
		request := &rangedbpb.OptimisticSaveRequest{
			ExpectedStreamSequenceNumber: 0,
			AggregateType:                "thing",
			AggregateID:                  aggregateID,
			Events: []*rangedbpb.Event{
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"618d9342a96c4a7f811ff5264e2ea813","number":100}`,
					Metadata: "",
				},
				{
					Type: "ThingWasDone",
					Data: `{invalid-json`,
				},
			},
		}

		// When
		_, err := rangeDBClient.OptimisticSave(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = InvalidArgument desc = unable to read event data: invalid character 'i' looking for beginning of object key string")
		errorResponse, ok := status.Convert(err).Details()[0].(*rangedbpb.SaveFailureResponse)
		require.True(t, ok)
		assert.Equal(t, "unable to read event data: invalid character 'i' looking for beginning of object key string", errorResponse.Message)
	})

	t.Run("fails on 2nd from invalid event metadata", func(t *testing.T) {
		// Given
		uuid := rangedbtest.NewSeededUUIDGenerator()
		store := inmemorystore.New(
			inmemorystore.WithClock(sequentialclock.New()),
			inmemorystore.WithUUIDGenerator(uuid),
		)
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		const aggregateID = "01745588c1854a2c84ed5a13c6fd133c"
		request := &rangedbpb.OptimisticSaveRequest{
			AggregateType: "thing",
			AggregateID:   aggregateID,
			Events: []*rangedbpb.Event{
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"01745588c1854a2c84ed5a13c6fd133c","number":100}`,
					Metadata: "",
				},
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"01745588c1854a2c84ed5a13c6fd133c","number":100}`,
					Metadata: `{invalid-json`,
				},
			},
		}

		// When
		_, err := rangeDBClient.OptimisticSave(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = InvalidArgument desc = unable to read event metadata: invalid character 'i' looking for beginning of object key string")
		errorResponse, ok := status.Convert(err).Details()[0].(*rangedbpb.SaveFailureResponse)
		require.True(t, ok)
		assert.Equal(t, "unable to read event metadata: invalid character 'i' looking for beginning of object key string", errorResponse.Message)
	})

	t.Run("fails from failing store", func(t *testing.T) {
		// Given
		store := rangedbtest.NewFailingEventStore()
		rangeDBClient := getClient(t, store)
		ctx := rangedbtest.TimeoutContext(t)
		const aggregateID = "92256cc1d6af4d5f96f31eff23de61cd"
		request := &rangedbpb.OptimisticSaveRequest{
			AggregateType: "thing",
			AggregateID:   aggregateID,
			Events: []*rangedbpb.Event{
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"92256cc1d6af4d5f96f31eff23de61cd","number":100}`,
					Metadata: "",
				},
				{
					Type:     "ThingWasDone",
					Data:     `{"id":"92256cc1d6af4d5f96f31eff23de61cd","number":100}`,
					Metadata: "",
				},
			},
		}

		// When
		_, err := rangeDBClient.OptimisticSave(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = Internal desc = unable to save to store: failingEventStore.OptimisticSave")
		errorResponse, ok := status.Convert(err).Details()[0].(*rangedbpb.SaveFailureResponse)
		require.True(t, ok)
		assert.Equal(t, "unable to save to store: failingEventStore.OptimisticSave", errorResponse.Message)
	})
}

func TestRangeDBServer_TotalEventsInStream(t *testing.T) {
	t.Run("errors from failing store", func(t *testing.T) {
		// Given
		store := rangedbtest.NewFailingEventStore()
		rangeDBClient := getClient(t, store)
		const aggregateID1 = "68588e1435af44beb75ad97c66b2a63f"
		event1 := rangedbtest.ThingWasDone{ID: aggregateID1, Number: 100}
		request := &rangedbpb.TotalEventsInStreamRequest{
			StreamName: rangedb.GetEventStream(event1),
		}
		ctx := rangedbtest.TimeoutContext(t)

		// When
		response, err := rangeDBClient.TotalEventsInStream(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = Unknown desc = failingEventStore.TotalEventsInStream")
		assert.Nil(t, response)
	})
}

func getClient(t *testing.T, store rangedb.Store) rangedbpb.RangeDBClient {
	bufListener := bufconn.Listen(7)
	server := grpc.NewServer()
	t.Cleanup(server.Stop)
	rangeDBServer, err := rangedbserver.New(rangedbserver.WithStore(store))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, rangeDBServer.Stop())
	})
	rangedbpb.RegisterRangeDBServer(server, rangeDBServer)

	go func() {
		if err := server.Serve(bufListener); err != nil {
			log.Printf("panic [%s] %v", t.Name(), err)
			t.Fail()
		}
	}()

	dialer := grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return bufListener.Dial()
	})
	ctx := rangedbtest.TimeoutContext(t)
	conn, err := grpc.DialContext(ctx, "bufnet", dialer, grpc.WithInsecure(), grpc.WithBlock())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	return rangedbpb.NewRangeDBClient(conn)
}

func assertPbRecordsEqual(t *testing.T, expected, actual *rangedbpb.Record) {
	expectedJson, err := json.Marshal(expected)
	require.NoError(t, err)

	actualJson, err := json.Marshal(actual)
	require.NoError(t, err)

	assert.JSONEq(t, string(expectedJson), string(actualJson))
}
