package rangedbserver_test

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
		startingWith := &rangedbpb.StartingWith{
			EventNumber: 0,
		}

		// When
		events, err := rangeDBClient.EventsStartingWith(ctx, startingWith)

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
