package rangedbserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
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

func ExampleRangeDBServer_SubscribeToEvents() {
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
	dialer := grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return bufListener.Dial()
	})
	connCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(connCtx, "bufnet", dialer, grpc.WithInsecure(), grpc.WithBlock())
	PrintError(err)

	defer func() {
		Close(conn)
		cancel()
		server.Stop()
	}()

	// Setup gRPC client
	rangeDBClient := rangedbpb.NewRangeDBClient(conn)
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	request := &rangedbpb.SubscribeToEventsRequest{
		StartingWithEventNumber: 0,
	}

	// When
	events, err := rangeDBClient.SubscribeToEvents(ctx, request)
	PrintError(err)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		PrintError(inMemoryStore.Save(
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "52e247a7c0a54a65906e006dac9be108", Number: 100}},
		))
		PrintError(inMemoryStore.Save(
			&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: "a3d9faa7614a46b388c6dce9984b6620"}},
		))
		wg.Done()
	}()

	go func() {
		for i := 0; i < 2; i++ {
			record, err := events.Recv()
			PrintError(err)

			body, err := json.Marshal(record)
			PrintError(err)

			fmt.Println(jsontools.PrettyJSON(body))
		}
		wg.Done()
	}()
	wg.Wait()

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
	//   "GlobalSequenceNumber": 1,
	//   "InsertTimestamp": 1,
	//   "EventID": "99cbd88bbcaf482ba1cc96ed12541707",
	//   "EventType": "AnotherWasComplete",
	//   "Data": "{\"id\":\"a3d9faa7614a46b388c6dce9984b6620\"}",
	//   "Metadata": "null"
	// }
}
