package rangedbserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbserver"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

func ExampleRangeDBServer_SaveEvents() {
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
		Events: []*rangedbpb.Event{
			{
				ID:       "2b1bb91150db464a8723cae30def7996",
				Type:     "ThingWasDone",
				Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":100}`,
				Metadata: "",
			},
			{
				ID:       "c8df652d85f2419e83ad6ef3afa49b08",
				Type:     "ThingWasDone",
				Data:     `{"id":"141b39d2b9854f8093ef79dc47dae6af","number":200}`,
				Metadata: "",
			},
		},
	}

	// When
	response, err := rangeDBClient.SaveEvents(ctx, request)
	PrintError(err)

	body, err := json.Marshal(response)
	PrintError(err)

	fmt.Println(jsontools.PrettyJSON(body))

	// Output:
	// {
	//   "EventsSaved": 2
	// }
}
