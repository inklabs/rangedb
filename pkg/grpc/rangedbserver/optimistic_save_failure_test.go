package rangedbserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbserver"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func ExampleRangeDBServer_OptimisticSave_withOptimisticConcurrencyFailure() {
	// Given
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)
	rangedbtest.BindEvents(inMemoryStore)

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
	connCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(connCtx, "bufnet", dialer, grpc.WithInsecure(), grpc.WithBlock())
	PrintError(err)
	defer Close(conn)

	// Setup gRPC client
	rangeDBClient := rangedbpb.NewRangeDBClient(conn)
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	request := &rangedbpb.OptimisticSaveRequest{
		ExpectedStreamSequenceNumber: 2,
		StreamName:                   "thing-141b39d2b9854f8093ef79dc47dae6af",
		Events: []*rangedbpb.Event{
			{
				AggregateType: "thing",
				AggregateID:   "141b39d2b9854f8093ef79dc47dae6af",
				EventType:     "ThingWasDone",
				Data:          `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
				Metadata:      "",
			},
			{
				AggregateType: "thing",
				AggregateID:   "141b39d2b9854f8093ef79dc47dae6af",
				EventType:     "ThingWasDone",
				Data:          `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":200}`,
				Metadata:      "",
			},
		},
	}

	// When
	_, err = rangeDBClient.OptimisticSave(ctx, request)
	PrintError(err)

	for _, detail := range status.Convert(err).Details() {
		failureResponse := detail.(*rangedbpb.SaveFailureResponse)

		body, err := json.Marshal(failureResponse)
		PrintError(err)

		fmt.Println(jsontools.PrettyJSON(body))
	}

	// Output:
	// rpc error: code = Internal desc = unable to save to store: unexpected sequence number: 2, actual: 0
	// {
	//   "Message": "unable to save to store: unexpected sequence number: 2, actual: 0"
	// }
}
