package rangedbws_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/rangedbws"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Example_streamAllEvents() {
	// Given
	rangedbtest.SetRand(100)
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
		inmemorystore.WithUUIDGenerator(rangedbtest.NewSeededUUIDGenerator()),
	)
	websocketApi, err := rangedbws.New(rangedbws.WithStore(inMemoryStore))
	PrintError(err)
	defer websocketApi.Stop()

	server := httptest.NewServer(websocketApi)
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	PrintError(err)
	serverURL.Scheme = "ws"
	serverURL.Path = "/events"

	socket, _, err := websocket.DefaultDialer.Dial(serverURL.String(), nil)
	PrintError(err)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer Close(socket)

		for i := 0; i < 2; i++ {
			_, message, err := socket.ReadMessage()
			PrintError(err)
			fmt.Println(jsontools.PrettyJSON(message))
		}

		wg.Done()
	}()

	// When
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()

	streamNameA := "thing-52e247a7c0a54a65906e006dac9be108"
	streamNameB := "another-a3d9faa7614a46b388c6dce9984b6620"
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameA,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "52e247a7c0a54a65906e006dac9be108", Number: 100}},
	)))
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameB,
		&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: "a3d9faa7614a46b388c6dce9984b6620"}},
	)))

	wg.Wait()

	// Output:
	// {
	//   "streamName": "thing-52e247a7c0a54a65906e006dac9be108",
	//   "aggregateType": "thing",
	//   "aggregateID": "52e247a7c0a54a65906e006dac9be108",
	//   "globalSequenceNumber": 1,
	//   "streamSequenceNumber": 1,
	//   "insertTimestamp": 0,
	//   "eventID": "d2ba8e70072943388203c438d4e94bf3",
	//   "eventType": "ThingWasDone",
	//   "data": {
	//     "id": "52e247a7c0a54a65906e006dac9be108",
	//     "number": 100
	//   },
	//   "metadata": null
	// }
	// {
	//   "streamName": "another-a3d9faa7614a46b388c6dce9984b6620",
	//   "aggregateType": "another",
	//   "aggregateID": "a3d9faa7614a46b388c6dce9984b6620",
	//   "globalSequenceNumber": 2,
	//   "streamSequenceNumber": 1,
	//   "insertTimestamp": 1,
	//   "eventID": "99cbd88bbcaf482ba1cc96ed12541707",
	//   "eventType": "AnotherWasComplete",
	//   "data": {
	//     "id": "a3d9faa7614a46b388c6dce9984b6620"
	//   },
	//   "metadata": null
	// }
}
