package rangedbapi_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

func Example_saveEvent() {
	// Given
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)
	api := rangedbapi.New(rangedbapi.WithStore(inMemoryStore))

	server := httptest.NewServer(api)
	defer server.Close()

	const requestBody = `[
		{
			"eventID": "2b1bb91150db464a8723cae30def7996",
			"eventType": "ThingWasDone",
			"data":{
				"id": "141b39d2b9854f8093ef79dc47dae6af",
				"number": 100
			},
			"metadata":null
		},
		{
			"eventID": "c8df652d85f2419e83ad6ef3afa49b08",
			"eventType": "ThingWasDone",
			"data":{
				"id": "141b39d2b9854f8093ef79dc47dae6af",
				"number": 200
			},
			"metadata":null
		}
	]`
	url := fmt.Sprintf("%s/save-events/thing/141b39d2b9854f8093ef79dc47dae6af", server.URL)

	// When
	response, err := http.Post(url, "application/json", strings.NewReader(requestBody))
	PrintError(err)
	defer Close(response.Body)

	body, err := ioutil.ReadAll(response.Body)
	PrintError(err)
	fmt.Println(string(body))

	// Output:
	// {"status":"OK"}
}
