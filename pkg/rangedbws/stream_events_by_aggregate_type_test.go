package rangedbws_test

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/rangedbws"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Example_streamAggregateTypeEvents() {
	// Given
	shortuuid.SetRand(100)
	inMemoryStore := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
	api := rangedbws.New(rangedbws.WithStore(inMemoryStore))

	server := httptest.NewServer(api)
	defer server.Close()

	serverAddress := strings.TrimPrefix(server.URL, "http://")
	websocketUrl := fmt.Sprintf("ws://%s/events/thing,that", serverAddress)
	socket, _, err := websocket.DefaultDialer.Dial(websocketUrl, nil)
	PrintError(err)

	wg := &sync.WaitGroup{}
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
	PrintError(inMemoryStore.Save(
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "dce275e43137467b92c9f4eb6c9c77a3", Number: 100}},
	))
	PrintError(inMemoryStore.Save(
		&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: "594c68cfa7944f9b94afc83505ff99e9"}},
	))
	PrintError(inMemoryStore.Save(
		&rangedb.EventRecord{Event: rangedbtest.ThatWasDone{ID: "075d37ae85894093aa818b391442df9b"}},
	))

	wg.Wait()

	// Output:
	// {
	//   "aggregateType": "thing",
	//   "aggregateID": "dce275e43137467b92c9f4eb6c9c77a3",
	//   "globalSequenceNumber": 0,
	//   "sequenceNumber": 0,
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
	//   "aggregateType": "that",
	//   "aggregateID": "075d37ae85894093aa818b391442df9b",
	//   "globalSequenceNumber": 2,
	//   "sequenceNumber": 0,
	//   "insertTimestamp": 2,
	//   "eventID": "2e9e6918af10498cb7349c89a351fdb7",
	//   "eventType": "ThatWasDone",
	//   "data": {
	//     "ID": "075d37ae85894093aa818b391442df9b"
	//   },
	//   "metadata": null
	// }
}
