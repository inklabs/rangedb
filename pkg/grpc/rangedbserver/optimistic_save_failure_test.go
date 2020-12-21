package rangedbserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbserver"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

func ExampleRangeDBServer_OptimisticSave_withOptimisticConcurrencyFailure() {
	// Given
	shortuuid.SetRand(100)
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)

	// Setup gRPC server
	bufListener := bufconn.Listen(7)
	server := grpc.NewServer()
	rangeDBServer := rangedbserver.New(rangedbserver.WithStore(inMemoryStore))
	rangedbpb.RegisterRangeDBServer(server, rangeDBServer)
	go func() {
		PrintError(server.Serve(bufListener))
	}()

	// Setup gRPC connection
	conn, err := grpc.Dial(
		"",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return bufListener.Dial()
		}),
		grpc.WithInsecure(),
	)
	PrintError(err)

	defer server.Stop()
	defer Close(conn)

	// Setup gRPC client
	rangeDBClient := rangedbpb.NewRangeDBClient(conn)
	ctx := context.Background()
	request := &rangedbpb.OptimisticSaveRequest{
		AggregateType:                "thing",
		AggregateID:                  "141b39d2b9854f8093ef79dc47dae6af",
		ExpectedStreamSequenceNumber: 2,
		Events: []*rangedbpb.Event{
			{
				Type:     "ThingWasDone",
				Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
				Metadata: "",
			},
			{
				Type:     "ThingWasDone",
				Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":200}`,
				Metadata: "",
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
	// rpc error: code = Internal desc = unable to save to store: unexpected sequence number: 2, next: 0
	// {
	//   "Message": "unable to save to store: unexpected sequence number: 2, next: 0"
	// }
}
