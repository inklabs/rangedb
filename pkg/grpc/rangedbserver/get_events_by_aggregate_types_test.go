package rangedbserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

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

func ExampleRangeDBServer_EventsByAggregateType() {
	// Given
	shortuuid.SetRand(100)
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)
	rangedbtest.BindEvents(inMemoryStore)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "605f20348fb940e386c171d51c877bf1", Number: 100}},
	)))
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx,
		&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: "a095086e52bc4617a1763a62398cd645"}},
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
	PrintError(err)
	defer Close(conn)

	// Setup gRPC client
	rangeDBClient := rangedbpb.NewRangeDBClient(conn)
	request := &rangedbpb.EventsByAggregateTypeRequest{
		GlobalSequenceNumber: 0,
		AggregateTypes:       []string{"thing", "another"},
	}

	// When
	events, err := rangeDBClient.EventsByAggregateType(ctx, request)
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
	//   "AggregateType": "another",
	//   "AggregateID": "a095086e52bc4617a1763a62398cd645",
	//   "GlobalSequenceNumber": 1,
	//   "InsertTimestamp": 1,
	//   "EventID": "99cbd88bbcaf482ba1cc96ed12541707",
	//   "EventType": "AnotherWasComplete",
	//   "Data": "{\"id\":\"a095086e52bc4617a1763a62398cd645\"}",
	//   "Metadata": "null"
	// }
}
