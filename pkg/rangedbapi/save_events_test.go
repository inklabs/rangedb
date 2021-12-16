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

func Example_saveEvent() {
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

	// When
	response, err := http.Post(serverURL.String(), "application/json", strings.NewReader(requestBody))
	PrintError(err)
	defer Close(response.Body)

	body, err := ioutil.ReadAll(response.Body)
	PrintError(err)
	fmt.Println(jsontools.PrettyJSON(body))

	// Output:
	// {
	//   "status": "OK",
	//   "streamSequenceNumber": 2
	// }
}
