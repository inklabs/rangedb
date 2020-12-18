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

func ExampleRangeDBServer_SaveEvents_failureResponse() {
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
	request := &rangedbpb.SaveEventsRequest{
		AggregateType: "thing",
		AggregateID:   "141b39d2b9854f8093ef79dc47dae6af",
		Events: []*rangedbpb.SaveEventRequest{
			{
				Type:     "ThingWasDone",
				Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
				Metadata: "",
			},
			{
				Type:     "ThingWasDone",
				Data:     `{invalid-json`,
				Metadata: "",
			},
		},
	}

	// When
	_, err = rangeDBClient.SaveEvents(ctx, request)
	fmt.Println(err)

	for _, detail := range status.Convert(err).Details() {
		failureResponse := detail.(*rangedbpb.SaveEventFailureResponse)

		body, err := json.Marshal(failureResponse)
		PrintError(err)

		fmt.Println(jsontools.PrettyJSON(body))
	}

	// Output:
	// rpc error: code = InvalidArgument desc = unable to read event data: invalid character 'i' looking for beginning of object key string
	// {
	//   "Message": "unable to read event data: invalid character 'i' looking for beginning of object key string"
	// }
}
