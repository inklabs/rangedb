package rangedbserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbserver"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func ExampleRangeDBServer_Events() {
	// Given
	shortuuid.SetRand(100)
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)
	PrintError(inMemoryStore.Save(
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "605f20348fb940e386c171d51c877bf1", Number: 100}},
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "605f20348fb940e386c171d51c877bf1", Number: 200}},
	))
	PrintError(inMemoryStore.Save(
		&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: "a095086e52bc4617a1763a62398cd645"}},
	))

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
	eventsRequest := &rangedbpb.EventsRequest{
		StartingWithEventNumber: 0,
	}

	// When
	events, err := rangeDBClient.Events(ctx, eventsRequest)
	PrintError(err)

	for {
		record, err := events.Recv()
		if err == io.EOF {
			break
		}
		PrintError(err)

		body, err := json.Marshal(record)
		PrintError(err)

		fmt.Println(jsontools.PrettyJSON(body))
	}

	// Output:
	// {
	//   "AggregateType": "thing",
	//   "AggregateID": "605f20348fb940e386c171d51c877bf1",
	//   "EventID": "d2ba8e70072943388203c438d4e94bf3",
	//   "EventType": "ThingWasDone",
	//   "Data": "{\"id\":\"605f20348fb940e386c171d51c877bf1\",\"number\":100}",
	//   "Metadata": "null"
	// }
	// {
	//   "AggregateType": "thing",
	//   "AggregateID": "605f20348fb940e386c171d51c877bf1",
	//   "GlobalSequenceNumber": 1,
	//   "StreamSequenceNumber": 1,
	//   "InsertTimestamp": 1,
	//   "EventID": "99cbd88bbcaf482ba1cc96ed12541707",
	//   "EventType": "ThingWasDone",
	//   "Data": "{\"id\":\"605f20348fb940e386c171d51c877bf1\",\"number\":200}",
	//   "Metadata": "null"
	// }
	// {
	//   "AggregateType": "another",
	//   "AggregateID": "a095086e52bc4617a1763a62398cd645",
	//   "GlobalSequenceNumber": 2,
	//   "InsertTimestamp": 2,
	//   "EventID": "2e9e6918af10498cb7349c89a351fdb7",
	//   "EventType": "AnotherWasComplete",
	//   "Data": "{\"id\":\"a095086e52bc4617a1763a62398cd645\"}",
	//   "Metadata": "null"
	// }
}
