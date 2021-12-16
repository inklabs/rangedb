package rangedbserver_test

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/inklabs/rangedb/pkg/grpc/rangedbserver"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func ExampleRangeDBServer_OptimisticDeleteStream_failure() {
	// Given
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)
	rangedbtest.BindEvents(inMemoryStore)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const aggregateID = "c129a8fa3d8945928c3e300beb0b6d58"
	event1 := rangedbtest.ThingWasDone{ID: aggregateID, Number: 100}
	event2 := rangedbtest.ThingWasDone{ID: aggregateID, Number: 200}
	streamName := "thing-c129a8fa3d8945928c3e300beb0b6d58"
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamName,
		&rangedb.EventRecord{Event: event1},
		&rangedb.EventRecord{Event: event2},
	)))

	// Setup gRPC server
	bufListener := bufconn.Listen(7)
	server := grpc.NewServer()
	defer server.Stop()
	rangeDBServer, err := rangedbserver.New(rangedbserver.WithStore(inMemoryStore))
	PrintError(err)
	defer rangeDBServer.Stop()
	rangedbpb.RegisterRangeDBServer(server, rangeDBServer)
	go func() {
		PrintError(server.Serve(bufListener))
	}()

	// Setup gRPC connection
	dialer := grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return bufListener.Dial()
	})
	conn, err := grpc.DialContext(ctx, "bufnet", dialer, grpc.WithInsecure(), grpc.WithBlock())
	defer Close(conn)
	PrintError(err)

	// Setup gRPC client
	rangeDBClient := rangedbpb.NewRangeDBClient(conn)
	optimisticDeleteStream := &rangedbpb.OptimisticDeleteStreamRequest{
		ExpectedStreamSequenceNumber: 5,
		StreamName:                   streamName,
	}

	// When
	_, err = rangeDBClient.OptimisticDeleteStream(ctx, optimisticDeleteStream)
	PrintError(err)

	// Output:
	// rpc error: code = Unknown desc = unexpected sequence number: 5, actual: 2
}
