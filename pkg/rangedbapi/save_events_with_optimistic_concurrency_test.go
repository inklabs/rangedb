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

func Example_optimisticSaveEvents() {
	// Given
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)
	api := rangedbapi.New(rangedbapi.WithStore(inMemoryStore))

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
	request, err := http.NewRequest(http.MethodPost, url, strings.NewReader(requestBody))
	PrintError(err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("ExpectedStreamSequenceNumber", "0")
	client := http.DefaultClient

	// When
	response, err := client.Do(request)
	PrintError(err)
	defer Close(response.Body)

	body, err := ioutil.ReadAll(response.Body)
	PrintError(err)
	fmt.Println(string(body))

	// Output:
	// {"status":"OK"}
}
