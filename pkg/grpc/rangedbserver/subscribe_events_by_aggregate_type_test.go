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
	"github.com/inklabs/rangedb/rangedbtest"
)

func ExampleRangeDBServer_SubscribeToEventsByAggregateType() {
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
	ctx, done := context.WithCancel(context.Background())
	request := &rangedbpb.SubscribeToEventsByAggregateTypeRequest{
		StartingWithEventNumber: 0,
		AggregateTypes:          []string{"thing", "another"},
	}

	// When
	events, err := rangeDBClient.SubscribeToEventsByAggregateType(ctx, request)
	PrintError(err)

	PrintError(
		inMemoryStore.Save(rangedbtest.ThingWasDone{ID: "52e247a7c0a54a65906e006dac9be108", Number: 100}, nil),
		inMemoryStore.Save(rangedbtest.ThatWasDone{ID: "de33dd02222f443b86861a9fb4574ce9"}, nil),
		inMemoryStore.Save(rangedbtest.AnotherWasComplete{ID: "a3d9faa7614a46b388c6dce9984b6620"}, nil),
	)

	for i := 0; i < 2; i++ {
		record, err := events.Recv()
		PrintError(err)

		body, err := json.Marshal(record)
		PrintError(err)

		fmt.Println(jsontools.PrettyJSON(body))
	}
	done()

	// Output:
	// {
	//   "AggregateType": "thing",
	//   "AggregateID": "52e247a7c0a54a65906e006dac9be108",
	//   "EventID": "d2ba8e70072943388203c438d4e94bf3",
	//   "EventType": "ThingWasDone",
	//   "Data": "{\"id\":\"52e247a7c0a54a65906e006dac9be108\",\"number\":100}",
	//   "Metadata": "null"
	// }
	// {
	//   "AggregateType": "another",
	//   "AggregateID": "a3d9faa7614a46b388c6dce9984b6620",
	//   "GlobalSequenceNumber": 2,
	//   "InsertTimestamp": 2,
	//   "EventID": "2e9e6918af10498cb7349c89a351fdb7",
	//   "EventType": "AnotherWasComplete",
	//   "Data": "{\"id\":\"a3d9faa7614a46b388c6dce9984b6620\"}",
	//   "Metadata": "null"
	// }
}
