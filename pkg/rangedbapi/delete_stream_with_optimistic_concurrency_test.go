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
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Example_optimisticDeleteStream() {
	// Given
	inMemoryStore := inmemorystore.New(
		inmemorystore.WithClock(sequentialclock.New()),
	)
	api, err := rangedbapi.New(rangedbapi.WithStore(inMemoryStore))
	PrintError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	PrintError(IgnoreFirstNumber(inMemoryStore.Save(ctx,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{ID: "4b9a415c53734b69ac459a7e53eb4c1b", Number: 100}},
	)))

	server := httptest.NewServer(api)
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	PrintError(err)
	serverURL.Path = "/delete-stream/thing/4b9a415c53734b69ac459a7e53eb4c1b"

	request, err := http.NewRequest(http.MethodPost, serverURL.String(), nil)
	PrintError(err)
	request.Header.Set("ExpectedStreamSequenceNumber", "1")
	client := http.DefaultClient

	// When
	response, err := client.Do(request)
	PrintError(err)
	defer Close(response.Body)

	body, err := ioutil.ReadAll(response.Body)
	PrintError(err)
	fmt.Println(jsontools.PrettyJSON(body))

	// Output:
	// {
	//   "status": "OK",
	//   "eventsDeleted": 1
	// }
}
