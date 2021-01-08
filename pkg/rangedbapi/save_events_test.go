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
	api, err := rangedbapi.New(rangedbapi.WithStore(inMemoryStore))
	PrintError(err)

	server := httptest.NewServer(api)
	defer server.Close()

	const requestBody = `[
		{
			"eventType": "ThingWasDone",
			"data":{
				"id": "141b39d2b9854f8093ef79dc47dae6af",
				"number": 100
			},
			"metadata":null
		},
		{
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
