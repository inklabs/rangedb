package rangedbapi_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

func Example_optimisticSaveEvents_failure() {
	// Given
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)
	api, err := rangedbapi.New(rangedbapi.WithStore(inMemoryStore))
	PrintError(err)

	server := httptest.NewServer(api)
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	PrintError(err)
	serverURL.Path = "/save-events/thing-141b39d2b9854f8093ef79dc47dae6af"

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

	request, err := http.NewRequest(http.MethodPost, serverURL.String(), strings.NewReader(requestBody))
	PrintError(err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("ExpectedStreamSequenceNumber", "2")
	client := http.DefaultClient

	// When
	response, err := client.Do(request)
	PrintError(err)
	defer Close(response.Body)

	body, err := ioutil.ReadAll(response.Body)
	PrintError(err)
	fmt.Println(response.Status)
	fmt.Println(jsontools.PrettyJSON(body))

	// Output:
	// 409 Conflict
	// {
	//   "status": "Failed",
	//   "message": "unexpected sequence number: 2, actual: 0"
	// }
}
