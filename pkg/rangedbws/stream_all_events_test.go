package rangedbws_test

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/rangedbws"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Example_streamAllEvents() {
	// Given
	shortuuid.SetRand(100)
	inMemoryStore := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
	websocketApi := rangedbws.New(rangedbws.WithStore(inMemoryStore))

	server := httptest.NewServer(websocketApi)
	defer server.Close()

	serverAddress := strings.TrimPrefix(server.URL, "http://")
	websocketUrl := fmt.Sprintf("ws://%s/events", serverAddress)
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
	PrintError(
		inMemoryStore.Save(rangedbtest.ThingWasDone{ID: "52e247a7c0a54a65906e006dac9be108", Number: 100}, nil),
		inMemoryStore.Save(rangedbtest.AnotherWasComplete{ID: "a3d9faa7614a46b388c6dce9984b6620"}, nil),
	)

	wg.Wait()

	// Output:
	// {
	//   "aggregateType": "thing",
	//   "aggregateID": "52e247a7c0a54a65906e006dac9be108",
	//   "globalSequenceNumber": 0,
	//   "sequenceNumber": 0,
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
	//   "aggregateType": "another",
	//   "aggregateID": "a3d9faa7614a46b388c6dce9984b6620",
	//   "globalSequenceNumber": 1,
	//   "sequenceNumber": 0,
	//   "insertTimestamp": 1,
	//   "eventID": "99cbd88bbcaf482ba1cc96ed12541707",
	//   "eventType": "AnotherWasComplete",
	//   "data": {
	//     "id": "a3d9faa7614a46b388c6dce9984b6620"
	//   },
	//   "metadata": null
	// }
}
