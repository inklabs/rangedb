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
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func ExampleRangeDBServer_SubscribeToEventsByAggregateType() {
	// Given
	rangedbtest.SetRand(100)
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
		inmemorystore.WithUUIDGenerator(rangedbtest.NewSeededUUIDGenerator()),
	)
	rangedbtest.BindEvents(inMemoryStore)
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	streamName := "thing-9f5b723b51fe4703883bde0d6d6f3fa9"
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamName,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "9f5b723b51fe4703883bde0d6d6f3fa9", Number: 1}},
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
	request := &rangedbpb.SubscribeToEventsByAggregateTypeRequest{
		GlobalSequenceNumber: 1,
		AggregateTypes:       []string{"thing", "another"},
	}

	// When
	events, err := rangeDBClient.SubscribeToEventsByAggregateType(ctx, request)
	PrintError(err)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for i := 0; i < 3; i++ {
			record, err := events.Recv()
			PrintError(err)

			body, err := json.Marshal(record)
			PrintError(err)

			fmt.Println(jsontools.PrettyJSON(body))
		}
		wg.Done()
	}()

	streamNameA := "thing-52e247a7c0a54a65906e006dac9be108"
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameA,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "52e247a7c0a54a65906e006dac9be108", Number: 2}},
	)))
	streamNameB := "that-de33dd02222f443b86861a9fb4574ce9"
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameB,
		&rangedb.EventRecord{Event: rangedbtest.ThatWasDone{ID: "de33dd02222f443b86861a9fb4574ce9"}},
	)))
	streamNameC := "another-a3d9faa7614a46b388c6dce9984b6620"
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameC,
		&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: "a3d9faa7614a46b388c6dce9984b6620"}},
	)))

	wg.Wait()

	// Output:
	// {
	//   "StreamName": "thing-9f5b723b51fe4703883bde0d6d6f3fa9",
	//   "AggregateType": "thing",
	//   "AggregateID": "9f5b723b51fe4703883bde0d6d6f3fa9",
	//   "GlobalSequenceNumber": 1,
	//   "StreamSequenceNumber": 1,
	//   "EventID": "d2ba8e70072943388203c438d4e94bf3",
	//   "EventType": "ThingWasDone",
	//   "Data": "{\"id\":\"9f5b723b51fe4703883bde0d6d6f3fa9\",\"number\":1}",
	//   "Metadata": "null"
	// }
	// {
	//   "StreamName": "thing-52e247a7c0a54a65906e006dac9be108",
	//   "AggregateType": "thing",
	//   "AggregateID": "52e247a7c0a54a65906e006dac9be108",
	//   "GlobalSequenceNumber": 2,
	//   "StreamSequenceNumber": 1,
	//   "InsertTimestamp": 1,
	//   "EventID": "99cbd88bbcaf482ba1cc96ed12541707",
	//   "EventType": "ThingWasDone",
	//   "Data": "{\"id\":\"52e247a7c0a54a65906e006dac9be108\",\"number\":2}",
	//   "Metadata": "null"
	// }
	// {
	//   "StreamName": "another-a3d9faa7614a46b388c6dce9984b6620",
	//   "AggregateType": "another",
	//   "AggregateID": "a3d9faa7614a46b388c6dce9984b6620",
	//   "GlobalSequenceNumber": 4,
	//   "StreamSequenceNumber": 1,
	//   "InsertTimestamp": 3,
	//   "EventID": "5042958739514c948f776fc9f820bca0",
	//   "EventType": "AnotherWasComplete",
	//   "Data": "{\"id\":\"a3d9faa7614a46b388c6dce9984b6620\"}",
	//   "Metadata": "null"
	// }
}
