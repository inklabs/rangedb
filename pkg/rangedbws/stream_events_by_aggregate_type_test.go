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

func Example_streamAggregateTypeEvents() {
	// Given
	rangedbtest.SetRand(100)
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
		inmemorystore.WithUUIDGenerator(rangedbtest.NewSeededUUIDGenerator()),
	)
	api, err := rangedbws.New(rangedbws.WithStore(inMemoryStore))
	PrintError(err)
	defer api.Stop()

	server := httptest.NewServer(api)
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	PrintError(err)
	serverURL.Scheme = "ws"
	serverURL.Path = "/events/thing,that"

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

	streamNameA := "thing-dce275e43137467b92c9f4eb6c9c77a3"
	streamNameB := "another-594c68cfa7944f9b94afc83505ff99e9"
	streamNameC := "that-075d37ae85894093aa818b391442df9b"
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameA,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "dce275e43137467b92c9f4eb6c9c77a3", Number: 100}},
	)))
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameB,
		&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: "594c68cfa7944f9b94afc83505ff99e9"}},
	)))
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameC,
		&rangedb.EventRecord{Event: rangedbtest.ThatWasDone{ID: "075d37ae85894093aa818b391442df9b"}},
	)))

	wg.Wait()

	// Output:
	// {
	//   "streamName": "thing-dce275e43137467b92c9f4eb6c9c77a3",
	//   "aggregateType": "thing",
	//   "aggregateID": "dce275e43137467b92c9f4eb6c9c77a3",
	//   "globalSequenceNumber": 1,
	//   "streamSequenceNumber": 1,
	//   "insertTimestamp": 0,
	//   "eventID": "d2ba8e70072943388203c438d4e94bf3",
	//   "eventType": "ThingWasDone",
	//   "data": {
	//     "id": "dce275e43137467b92c9f4eb6c9c77a3",
	//     "number": 100
	//   },
	//   "metadata": null
	// }
	// {
	//   "streamName": "that-075d37ae85894093aa818b391442df9b",
	//   "aggregateType": "that",
	//   "aggregateID": "075d37ae85894093aa818b391442df9b",
	//   "globalSequenceNumber": 3,
	//   "streamSequenceNumber": 1,
	//   "insertTimestamp": 2,
	//   "eventID": "2e9e6918af10498cb7349c89a351fdb7",
	//   "eventType": "ThatWasDone",
	//   "data": {
	//     "ID": "075d37ae85894093aa818b391442df9b"
	//   },
	//   "metadata": null
	// }
}
