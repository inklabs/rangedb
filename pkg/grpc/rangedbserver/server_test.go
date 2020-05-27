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
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestRangeDBServer_WithFourEventsSaved(t *testing.T) {
	// Given
	shortuuid.SetRand(100)
	store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
	const aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
	const aggregateID2 = "5b36ae984b724685917b69ae47968be1"
	const aggregateID3 = "9bc181144cef4fd19da1f32a17363997"

	saveEvents(t, store,
		rangedbtest.ThingWasDone{
			ID:     aggregateID1,
			Number: 100,
		},
		rangedbtest.ThingWasDone{
			ID:     aggregateID1,
			Number: 200,
		},
		rangedbtest.ThingWasDone{
			ID:     aggregateID2,
			Number: 300,
		},
		rangedbtest.AnotherWasComplete{
			ID: aggregateID3,
		},
	)

	t.Run("get all events", func(t *testing.T) {
		// Given
		rangeDBClient := getClient(t, store)
		ctx := context.Background()
		eventsRequest := &rangedbpb.EventsRequest{
			StartingWithEventNumber: 0,
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
			AggregateID:          "f187760f4d8c4d1c9d9cf17b66766abd",
			GlobalSequenceNumber: 0,
			StreamSequenceNumber: 0,
			InsertTimestamp:      0,
			EventID:              "d2ba8e70072943388203c438d4e94bf3",
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":100}`,
			Metadata:             "null",
		}
		expectedRecord2 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          "f187760f4d8c4d1c9d9cf17b66766abd",
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			InsertTimestamp:      1,
			EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":200}`,
			Metadata:             "null",
		}
		expectedRecord3 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          "5b36ae984b724685917b69ae47968be1",
			GlobalSequenceNumber: 2,
			StreamSequenceNumber: 0,
			InsertTimestamp:      2,
			EventID:              "2e9e6918af10498cb7349c89a351fdb7",
			EventType:            "ThingWasDone",
			Data:                 `{"id":"5b36ae984b724685917b69ae47968be1","number":300}`,
			Metadata:             "null",
		}
		expectedRecord4 := &rangedbpb.Record{
			AggregateType:        "another",
			AggregateID:          "9bc181144cef4fd19da1f32a17363997",
			GlobalSequenceNumber: 3,
			StreamSequenceNumber: 0,
			InsertTimestamp:      3,
			EventID:              "5042958739514c948f776fc9f820bca0",
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
}

func TestRangeDBServer_SubscribeToLiveEvents(t *testing.T) {
	t.Run("subscribes to real time events, ignoring previous events", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
		const aggregateID2 = "5b36ae984b724685917b69ae47968be1"
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		saveEvents(t, store,
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 100,
			},
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 200,
			},
		)
		rangeDBClient := getClient(t, store)
		ctx, done := context.WithCancel(context.Background())
		request := &rangedbpb.SubscribeToLiveEventsRequest{}

		// When
		events, err := rangeDBClient.SubscribeToLiveEvents(ctx, request)
		require.NoError(t, err)

		// Then
		time.Sleep(time.Millisecond * 5)
		actualRecords := make(chan *rangedbpb.Record, 10)

		saveEvents(t, store,
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 300,
			},
			rangedbtest.AnotherWasComplete{
				ID: aggregateID2,
			},
		)

		for i := 0; i < 2; i++ {
			record, err := events.Recv()
			require.NoError(t, err)
			actualRecords <- record
		}
		done()
		close(actualRecords)

		expectedRecord1 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          "f187760f4d8c4d1c9d9cf17b66766abd",
			GlobalSequenceNumber: 2,
			StreamSequenceNumber: 2,
			InsertTimestamp:      2,
			EventID:              "2e9e6918af10498cb7349c89a351fdb7",
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":300}`,
			Metadata:             "null",
		}
		expectedRecord2 := &rangedbpb.Record{
			AggregateType:        "another",
			AggregateID:          "5b36ae984b724685917b69ae47968be1",
			GlobalSequenceNumber: 3,
			StreamSequenceNumber: 0,
			InsertTimestamp:      3,
			EventID:              "5042958739514c948f776fc9f820bca0",
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
		shortuuid.SetRand(100)
		const aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
		const aggregateID2 = "5b36ae984b724685917b69ae47968be1"
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		saveEvents(t, store,
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 100,
			},
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 200,
			},
		)
		rangeDBClient := getClient(t, store)
		ctx, done := context.WithCancel(context.Background())
		request := &rangedbpb.SubscribeToEventsRequest{
			StartingWithEventNumber: 1,
		}

		// When
		events, err := rangeDBClient.SubscribeToEvents(ctx, request)
		require.NoError(t, err)

		// Then
		actualRecords := make(chan *rangedbpb.Record, 10)

		record, err := events.Recv()
		require.NoError(t, err)
		actualRecords <- record

		saveEvents(t, store,
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 300,
			},
			rangedbtest.AnotherWasComplete{
				ID: aggregateID2,
			},
		)

		for i := 0; i < 2; i++ {
			record, err := events.Recv()
			require.NoError(t, err)
			actualRecords <- record
		}
		done()
		close(actualRecords)

		expectedRecord1 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          "f187760f4d8c4d1c9d9cf17b66766abd",
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			InsertTimestamp:      1,
			EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":200}`,
			Metadata:             "null",
		}
		expectedRecord2 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          "f187760f4d8c4d1c9d9cf17b66766abd",
			GlobalSequenceNumber: 2,
			StreamSequenceNumber: 2,
			InsertTimestamp:      2,
			EventID:              "2e9e6918af10498cb7349c89a351fdb7",
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":300}`,
			Metadata:             "null",
		}
		expectedRecord3 := &rangedbpb.Record{
			AggregateType:        "another",
			AggregateID:          "5b36ae984b724685917b69ae47968be1",
			GlobalSequenceNumber: 3,
			StreamSequenceNumber: 0,
			InsertTimestamp:      3,
			EventID:              "5042958739514c948f776fc9f820bca0",
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
		shortuuid.SetRand(100)
		const aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
		const aggregateID2 = "5b36ae984b724685917b69ae47968be1"
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		saveEvents(t, store,
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 100,
			},
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 200,
			},
		)
		rangeDBClient := getClient(t, store)
		ctx, done := context.WithCancel(context.Background())
		request := &rangedbpb.SubscribeToEventsByAggregateTypeRequest{
			StartingWithEventNumber: 1,
			AggregateTypes:          []string{"thing", "another"},
		}

		// When
		events, err := rangeDBClient.SubscribeToEventsByAggregateType(ctx, request)
		require.NoError(t, err)

		// Then
		actualRecords := make(chan *rangedbpb.Record, 10)

		record, err := events.Recv()
		require.NoError(t, err)
		actualRecords <- record

		saveEvents(t, store,
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 300,
			},
			rangedbtest.ThatWasDone{
				ID: "54d8ee5ba84d45a09b3186d9617c4f86",
			},
			rangedbtest.AnotherWasComplete{
				ID: aggregateID2,
			},
		)

		for i := 0; i < 2; i++ {
			record, err := events.Recv()
			require.NoError(t, err)
			actualRecords <- record
		}
		done()
		close(actualRecords)

		expectedRecord1 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          "f187760f4d8c4d1c9d9cf17b66766abd",
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			InsertTimestamp:      1,
			EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":200}`,
			Metadata:             "null",
		}
		expectedRecord2 := &rangedbpb.Record{
			AggregateType:        "thing",
			AggregateID:          "f187760f4d8c4d1c9d9cf17b66766abd",
			GlobalSequenceNumber: 2,
			StreamSequenceNumber: 2,
			InsertTimestamp:      2,
			EventID:              "2e9e6918af10498cb7349c89a351fdb7",
			EventType:            "ThingWasDone",
			Data:                 `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":300}`,
			Metadata:             "null",
		}
		expectedRecord3 := &rangedbpb.Record{
			AggregateType:        "another",
			AggregateID:          "5b36ae984b724685917b69ae47968be1",
			GlobalSequenceNumber: 4,
			StreamSequenceNumber: 0,
			InsertTimestamp:      4,
			EventID:              "4059365d39ce4f0082f419ba1350d9c0",
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

func TestRangeDBServer_SaveEvents(t *testing.T) {
	t.Run("saves 2 events", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		rangeDBClient := getClient(t, store)
		ctx := context.Background()
		request := &rangedbpb.SaveEventsRequest{
			AggregateType: "thing",
			AggregateID:   "b5ef2296339d4ad1887f1deb486f7821",
			Events: []*rangedbpb.Event{
				{
					ID:       "dea776d1a9104e389d0153e391717f26",
					Type:     "ThingWasDone",
					Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
					Metadata: "",
				},
				{
					ID:   "96489f712adf44f3bd6ee4509d19d46c",
					Type: "ThingWasDone",
					Data: `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":200}`,
				},
			},
		}

		// When
		response, err := rangeDBClient.SaveEvents(ctx, request)
		require.NoError(t, err)

		// Then
		assert.Equal(t, uint32(2), response.EventsSaved)
		actualRecords := store.EventsStartingWith(ctx, 0)
		expectedRecord1 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateID:          "b5ef2296339d4ad1887f1deb486f7821",
			GlobalSequenceNumber: 0,
			StreamSequenceNumber: 0,
			InsertTimestamp:      0,
			EventID:              "dea776d1a9104e389d0153e391717f26",
			EventType:            "ThingWasDone",
			Data: map[string]interface{}{
				"id":     "141b39d2b9854f8093ef79dc47dae6af",
				"number": json.Number("100"),
			},
			Metadata: nil,
		}
		expectedRecord2 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateID:          "b5ef2296339d4ad1887f1deb486f7821",
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			InsertTimestamp:      1,
			EventID:              "96489f712adf44f3bd6ee4509d19d46c",
			EventType:            "ThingWasDone",
			Data: map[string]interface{}{
				"id":     "141b39d2b9854f8093ef79dc47dae6af",
				"number": json.Number("200"),
			},
			Metadata: nil,
		}
		assert.Equal(t, expectedRecord1, <-actualRecords)
		assert.Equal(t, expectedRecord2, <-actualRecords)
		assert.Equal(t, (*rangedb.Record)(nil), <-actualRecords)
	})

	t.Run("saves 1 event and fails on 2nd from invalid event data", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		rangeDBClient := getClient(t, store)
		ctx := context.Background()
		request := &rangedbpb.SaveEventsRequest{
			AggregateType: "thing",
			AggregateID:   "b5ef2296339d4ad1887f1deb486f7821",
			Events: []*rangedbpb.Event{
				{
					ID:       "dea776d1a9104e389d0153e391717f26",
					Type:     "ThingWasDone",
					Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
					Metadata: "",
				},
				{
					ID:   "96489f712adf44f3bd6ee4509d19d46c",
					Type: "ThingWasDone",
					Data: `{invalid-json`,
				},
			},
		}

		// When
		response, err := rangeDBClient.SaveEvents(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = InvalidArgument desc = unable to read event data: invalid character 'i' looking for beginning of object key string")
		log.Println(response)
		errorResponse, ok := status.Convert(err).Details()[0].(*rangedbpb.SaveEventFailureResponse)
		require.True(t, ok)
		assert.Equal(t, uint32(1), errorResponse.EventsSaved)
	})

	t.Run("saves 1 event and fails on 2nd from invalid event metadata", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		rangeDBClient := getClient(t, store)
		ctx := context.Background()
		request := &rangedbpb.SaveEventsRequest{
			AggregateType: "thing",
			AggregateID:   "b5ef2296339d4ad1887f1deb486f7821",
			Events: []*rangedbpb.Event{
				{
					ID:       "dea776d1a9104e389d0153e391717f26",
					Type:     "ThingWasDone",
					Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
					Metadata: "",
				},
				{
					ID:       "96489f712adf44f3bd6ee4509d19d46c",
					Type:     "ThingWasDone",
					Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
					Metadata: `{invalid-json`,
				},
			},
		}

		// When
		response, err := rangeDBClient.SaveEvents(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = InvalidArgument desc = unable to read event metadata: invalid character 'i' looking for beginning of object key string")
		log.Println(response)
		errorResponse, ok := status.Convert(err).Details()[0].(*rangedbpb.SaveEventFailureResponse)
		require.True(t, ok)
		assert.Equal(t, uint32(1), errorResponse.EventsSaved)
	})

	t.Run("fails from failing store", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := rangedbtest.NewFailingEventStore()
		rangeDBClient := getClient(t, store)
		ctx := context.Background()
		request := &rangedbpb.SaveEventsRequest{
			AggregateType: "thing",
			AggregateID:   "b5ef2296339d4ad1887f1deb486f7821",
			Events: []*rangedbpb.Event{
				{
					ID:       "dea776d1a9104e389d0153e391717f26",
					Type:     "ThingWasDone",
					Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
					Metadata: "",
				},
				{
					ID:       "96489f712adf44f3bd6ee4509d19d46c",
					Type:     "ThingWasDone",
					Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
					Metadata: `{invalid-json`,
				},
			},
		}

		// When
		response, err := rangeDBClient.SaveEvents(ctx, request)

		// Then
		require.EqualError(t, err, "rpc error: code = Internal desc = unable to save to store: failingEventStore.SaveEvent")
		log.Println(response)
		errorResponse, ok := status.Convert(err).Details()[0].(*rangedbpb.SaveEventFailureResponse)
		require.True(t, ok)
		assert.Equal(t, uint32(0), errorResponse.EventsSaved)
	})
}

func saveEvents(t *testing.T, store rangedb.Store, events ...rangedb.Event) {
	for _, event := range events {
		require.NoError(t, store.Save(event, nil))
	}
}

func getClient(t *testing.T, store rangedb.Store) rangedbpb.RangeDBClient {
	bufListener := bufconn.Listen(7)
	server := grpc.NewServer()
	rangeDBServer := rangedbserver.New(rangedbserver.WithStore(store))
	rangedbpb.RegisterRangeDBServer(server, rangeDBServer)
	dialer := grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return bufListener.Dial()
	})
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "", dialer, grpc.WithInsecure())
	require.NoError(t, err)

	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, conn.Close())
	})

	go func() {
		require.NoError(t, server.Serve(bufListener))
	}()

	return rangedbpb.NewRangeDBClient(conn)
}

func assertPbRecordsEqual(t *testing.T, expected, actual *rangedbpb.Record) {
	expectedJson, err := json.Marshal(expected)
	require.NoError(t, err)

	actualJson, err := json.Marshal(actual)
	require.NoError(t, err)

	assert.JSONEq(t, string(expectedJson), string(actualJson))
}
