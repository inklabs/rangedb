package rangedbapi_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Example_getEventsByStreamNdJson() {
	// Given
	rangedbtest.SetRand(100)
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
		inmemorystore.WithUUIDGenerator(rangedbtest.NewSeededUUIDGenerator()),
	)
	api, err := rangedbapi.New(rangedbapi.WithStore(inMemoryStore))
	PrintError(err)

	server := httptest.NewServer(api)
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	PrintError(err)
	serverURL.Path = "/events-by-stream/thing-605f20348fb940e386c171d51c877bf1.ndjson"

	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()

	streamNameA := "thing-605f20348fb940e386c171d51c877bf1"
	streamNameB := "another-a095086e52bc4617a1763a62398cd645"
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameA,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "605f20348fb940e386c171d51c877bf1", Number: 100}},
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "605f20348fb940e386c171d51c877bf1", Number: 200}},
	)))
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx, streamNameB,
		&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: "a095086e52bc4617a1763a62398cd645"}},
	)))

	// When
	response, err := http.Get(serverURL.String())
	PrintError(err)
	defer Close(response.Body)

	body, err := ioutil.ReadAll(response.Body)
	PrintError(err)

	fmt.Println(string(body))

	// Output:
	// {"streamName":"thing-605f20348fb940e386c171d51c877bf1","aggregateType":"thing","aggregateID":"605f20348fb940e386c171d51c877bf1","globalSequenceNumber":1,"streamSequenceNumber":1,"insertTimestamp":0,"eventID":"d2ba8e70072943388203c438d4e94bf3","eventType":"ThingWasDone","data":{"id":"605f20348fb940e386c171d51c877bf1","number":100},"metadata":null}
	// {"streamName":"thing-605f20348fb940e386c171d51c877bf1","aggregateType":"thing","aggregateID":"605f20348fb940e386c171d51c877bf1","globalSequenceNumber":2,"streamSequenceNumber":2,"insertTimestamp":1,"eventID":"99cbd88bbcaf482ba1cc96ed12541707","eventType":"ThingWasDone","data":{"id":"605f20348fb940e386c171d51c877bf1","number":200},"metadata":null}
}
