package rangedbserver_test

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"testing"

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

		// Then
		require.NoError(t, err)
		record1, err := events.Recv()
		require.NoError(t, err)
		assert.Equal(t, "thing", record1.AggregateType)
		assert.Equal(t, "f187760f4d8c4d1c9d9cf17b66766abd", record1.AggregateID)
		assert.Equal(t, 0, int(record1.GlobalSequenceNumber))
		assert.Equal(t, 0, int(record1.StreamSequenceNumber))
		assert.Equal(t, 0, int(record1.InsertTimestamp))
		assert.Equal(t, "d2ba8e70072943388203c438d4e94bf3", record1.EventID)
		assert.Equal(t, "ThingWasDone", record1.EventType)
		assert.Equal(t, `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":100}`, record1.Data)
		assert.Equal(t, "null", record1.Metadata)
		record2, err := events.Recv()
		require.NoError(t, err)
		assert.Equal(t, "thing", record2.AggregateType)
		assert.Equal(t, "f187760f4d8c4d1c9d9cf17b66766abd", record2.AggregateID)
		assert.Equal(t, 1, int(record2.GlobalSequenceNumber))
		assert.Equal(t, 1, int(record2.StreamSequenceNumber))
		assert.Equal(t, 1, int(record2.InsertTimestamp))
		assert.Equal(t, "99cbd88bbcaf482ba1cc96ed12541707", record2.EventID)
		assert.Equal(t, "ThingWasDone", record2.EventType)
		assert.Equal(t, `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":200}`, record2.Data)
		assert.Equal(t, "null", record2.Metadata)
		record3, err := events.Recv()
		require.NoError(t, err)
		assert.Equal(t, "thing", record3.AggregateType)
		assert.Equal(t, "5b36ae984b724685917b69ae47968be1", record3.AggregateID)
		assert.Equal(t, 2, int(record3.GlobalSequenceNumber))
		assert.Equal(t, 0, int(record3.StreamSequenceNumber))
		assert.Equal(t, 2, int(record3.InsertTimestamp))
		assert.Equal(t, "2e9e6918af10498cb7349c89a351fdb7", record3.EventID)
		assert.Equal(t, "ThingWasDone", record3.EventType)
		assert.Equal(t, `{"id":"5b36ae984b724685917b69ae47968be1","number":300}`, record3.Data)
		assert.Equal(t, "null", record3.Metadata)
		record4, err := events.Recv()
		require.NoError(t, err)
		assert.Equal(t, "another", record4.AggregateType)
		assert.Equal(t, "9bc181144cef4fd19da1f32a17363997", record4.AggregateID)
		assert.Equal(t, 3, int(record4.GlobalSequenceNumber))
		assert.Equal(t, 0, int(record4.StreamSequenceNumber))
		assert.Equal(t, 3, int(record4.InsertTimestamp))
		assert.Equal(t, "5042958739514c948f776fc9f820bca0", record4.EventID)
		assert.Equal(t, "AnotherWasComplete", record4.EventType)
		assert.Equal(t, `{"id":"9bc181144cef4fd19da1f32a17363997"}`, record4.Data)
		assert.Equal(t, "null", record4.Metadata)
		endRecord, err := events.Recv()
		assert.Equal(t, io.EOF, err)
		assert.Equal(t, (*rangedbpb.Record)(nil), endRecord)
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
		eventsRequest := &rangedbpb.EventsRequest{
			StartingWithEventNumber: 1,
		}
		events, err := rangeDBClient.SubscribeToEvents(ctx, eventsRequest)
		require.NoError(t, err)
		var actualRecords []*rangedbpb.Record

		// When
		go func() {
			for i := 0; i < 3; i++ {
				record, err := events.Recv()
				require.NoError(t, err)
				actualRecords = append(actualRecords, record)
			}
			done()
		}()

		saveEvents(t, store,
			rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 300,
			},
			rangedbtest.AnotherWasComplete{
				ID: aggregateID2,
			},
		)

		<-ctx.Done()

		// Then
		require.Equal(t, 3, len(actualRecords))
		record1 := actualRecords[0]
		assert.Equal(t, "thing", record1.AggregateType)
		assert.Equal(t, "f187760f4d8c4d1c9d9cf17b66766abd", record1.AggregateID)
		assert.Equal(t, 1, int(record1.GlobalSequenceNumber))
		assert.Equal(t, 1, int(record1.StreamSequenceNumber))
		assert.Equal(t, 1, int(record1.InsertTimestamp))
		assert.Equal(t, "99cbd88bbcaf482ba1cc96ed12541707", record1.EventID)
		assert.Equal(t, "ThingWasDone", record1.EventType)
		assert.Equal(t, `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":200}`, record1.Data)
		assert.Equal(t, "null", record1.Metadata)
		record2 := actualRecords[1]
		assert.Equal(t, "thing", record2.AggregateType)
		assert.Equal(t, "f187760f4d8c4d1c9d9cf17b66766abd", record2.AggregateID)
		assert.Equal(t, 2, int(record2.GlobalSequenceNumber))
		assert.Equal(t, 2, int(record2.StreamSequenceNumber))
		assert.Equal(t, 2, int(record2.InsertTimestamp))
		assert.Equal(t, "2e9e6918af10498cb7349c89a351fdb7", record2.EventID)
		assert.Equal(t, "ThingWasDone", record2.EventType)
		assert.Equal(t, `{"id":"f187760f4d8c4d1c9d9cf17b66766abd","number":300}`, record2.Data)
		assert.Equal(t, "null", record2.Metadata)
		record3 := actualRecords[2]
		assert.Equal(t, "another", record3.AggregateType)
		assert.Equal(t, "5b36ae984b724685917b69ae47968be1", record3.AggregateID)
		assert.Equal(t, 3, int(record3.GlobalSequenceNumber))
		assert.Equal(t, 0, int(record3.StreamSequenceNumber))
		assert.Equal(t, 3, int(record3.InsertTimestamp))
		assert.Equal(t, "5042958739514c948f776fc9f820bca0", record3.EventID)
		assert.Equal(t, "AnotherWasComplete", record3.EventType)
		assert.Equal(t, `{"id":"5b36ae984b724685917b69ae47968be1"}`, record3.Data)
		assert.Equal(t, "null", record3.Metadata)
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

		// Then
		require.NoError(t, err)
		assert.Equal(t, uint32(2), response.EventsSaved)

		records := rangedb.RecordChannelToSlice(store.EventsStartingWith(0))
		require.Equal(t, 2, len(records))
		assert.Equal(t, "thing", records[0].AggregateType)
		assert.Equal(t, "b5ef2296339d4ad1887f1deb486f7821", records[0].AggregateID)
		assert.Equal(t, uint64(0), records[0].GlobalSequenceNumber)
		assert.Equal(t, uint64(0), records[0].StreamSequenceNumber)
		assert.Equal(t, uint64(0), records[0].InsertTimestamp)
		assert.Equal(t, "dea776d1a9104e389d0153e391717f26", records[0].EventID)
		assert.Equal(t, "ThingWasDone", records[0].EventType)
		assert.Equal(t,
			map[string]interface{}{
				"id":     "141b39d2b9854f8093ef79dc47dae6af",
				"number": json.Number("100"),
			},
			records[0].Data,
		)
		assert.Equal(t, nil, records[0].Metadata)
		assert.Equal(t, "thing", records[1].AggregateType)
		assert.Equal(t, "b5ef2296339d4ad1887f1deb486f7821", records[1].AggregateID)
		assert.Equal(t, uint64(1), records[1].GlobalSequenceNumber)
		assert.Equal(t, uint64(1), records[1].StreamSequenceNumber)
		assert.Equal(t, uint64(1), records[1].InsertTimestamp)
		assert.Equal(t, "96489f712adf44f3bd6ee4509d19d46c", records[1].EventID)
		assert.Equal(t, "ThingWasDone", records[1].EventType)
		assert.Equal(t,
			map[string]interface{}{
				"id":     "141b39d2b9854f8093ef79dc47dae6af",
				"number": json.Number("200"),
			},
			records[1].Data,
		)
		assert.Equal(t, nil, records[1].Metadata)
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

	go func() {
		require.NoError(t, server.Serve(bufListener))
	}()

	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, conn.Close())
	})

	return rangedbpb.NewRangeDBClient(conn)
}
