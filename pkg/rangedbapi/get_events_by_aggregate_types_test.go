package rangedbapi_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Example_getEventsByAggregateTypes() {
	// Given
	shortuuid.SetRand(100)
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)
	api := rangedbapi.New(rangedbapi.WithStore(inMemoryStore))

	server := httptest.NewServer(api)
	defer server.Close()

	PrintError(
		inMemoryStore.Save(
			&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "605f20348fb940e386c171d51c877bf1", Number: 100}},
			&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{ID: "a095086e52bc4617a1763a62398cd645"}},
		),
	)
	url := fmt.Sprintf("%s/events/thing,another.json", server.URL)

	// When
	response, err := http.Get(url)
	PrintError(err)
	defer Close(response.Body)

	body, err := ioutil.ReadAll(response.Body)
	PrintError(err)

	fmt.Println(jsontools.PrettyJSON(body))

	// Output:
	// [
	//   {
	//     "aggregateType": "thing",
	//     "aggregateID": "605f20348fb940e386c171d51c877bf1",
	//     "globalSequenceNumber": 0,
	//     "sequenceNumber": 0,
	//     "insertTimestamp": 0,
	//     "eventID": "d2ba8e70072943388203c438d4e94bf3",
	//     "eventType": "ThingWasDone",
	//     "data": {
	//       "id": "605f20348fb940e386c171d51c877bf1",
	//       "number": 100
	//     },
	//     "metadata": null
	//   },
	//   {
	//     "aggregateType": "another",
	//     "aggregateID": "a095086e52bc4617a1763a62398cd645",
	//     "globalSequenceNumber": 1,
	//     "sequenceNumber": 0,
	//     "insertTimestamp": 1,
	//     "eventID": "99cbd88bbcaf482ba1cc96ed12541707",
	//     "eventType": "AnotherWasComplete",
	//     "data": {
	//       "id": "a095086e52bc4617a1763a62398cd645"
	//     },
	//     "metadata": null
	//   }
	// ]
}
