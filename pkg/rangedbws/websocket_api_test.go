package rangedbws_test

import (
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/rangedbws"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_WebsocketApi_SubscribeToAllEvents_ReadsEventsOverWebsocket(t *testing.T) {
	// Given
	uuid.SetRand(rand.New(rand.NewSource(100)))
	store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
	event1 := &rangedbtest.ThingWasDone{ID: "6595a5c206c746c3a9d9006c7df5784e", Number: 1}
	event2 := &rangedbtest.ThingWasDone{ID: "8cc839e1fd3545b7a0fe67808d84cbd4", Number: 2}
	require.NoError(t, store.Save(
		&rangedb.EventRecord{Event: event1},
		&rangedb.EventRecord{Event: event2},
	))
	api := rangedbws.New(rangedbws.WithStore(store))
	server := httptest.NewServer(api)
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/events"
	socket, response, err := websocket.DefaultDialer.Dial(url, nil)
	require.NoError(t, err)
	defer closeOrFail(t, socket)
	defer closeOrFail(t, response.Body)

	// When
	_, actualBytes1, err := socket.ReadMessage()
	require.NoError(t, err)
	_, actualBytes2, err := socket.ReadMessage()
	require.NoError(t, err)

	// Then
	expectedEvent1 := `{
		"aggregateType": "thing",
		"aggregateID": "6595a5c206c746c3a9d9006c7df5784e",
		"globalSequenceNumber":0,
		"sequenceNumber":0,
		"insertTimestamp":0,
		"eventID": "d2ba8e70072943388203c438d4e94bf3",
		"eventType": "ThingWasDone",
		"data":{
			"id": "6595a5c206c746c3a9d9006c7df5784e",
			"number": 1
		},
		"metadata":null
	}`
	expectedEvent2 := `{
		"aggregateType": "thing",
		"aggregateID": "8cc839e1fd3545b7a0fe67808d84cbd4",
		"globalSequenceNumber":1,
		"sequenceNumber":0,
		"insertTimestamp":1,
		"eventID": "99cbd88bbcaf482ba1cc96ed12541707",
		"eventType": "ThingWasDone",
		"data":{
			"id": "8cc839e1fd3545b7a0fe67808d84cbd4",
			"number": 2
		},
		"metadata":null
	}`
	assert.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)
	assertJsonEqual(t, expectedEvent1, string(actualBytes1))
	assertJsonEqual(t, expectedEvent2, string(actualBytes2))
}

func Test_WebsocketApi_Failures(t *testing.T) {
	t.Run("all events fails when upgrading connection", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		api := rangedbws.New(rangedbws.WithStore(store))
		request := httptest.NewRequest("GET", "/events", nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "Bad Request\nunable to upgrade websocket connection\n", response.Body.String())
	})

	t.Run("events by aggregate type fails when upgrading connection", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		api := rangedbws.New(rangedbws.WithStore(store))
		request := httptest.NewRequest("GET", "/events/thing", nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "Bad Request\nunable to upgrade websocket connection\n", response.Body.String())
	})

	t.Run("fails when writing existing events to connection", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		event := &rangedbtest.ThingWasDone{ID: "372b47686e1b43d29d2fd48f2a0e83f0", Number: 1}
		require.NoError(t, store.Save(
			&rangedb.EventRecord{Event: event},
		))
		api := rangedbws.New(rangedbws.WithStore(store))
		server := httptest.NewServer(api)
		defer server.Close()

		url := "ws" + strings.TrimPrefix(server.URL, "http") + "/events"
		socket, response, err := websocket.DefaultDialer.Dial(url, nil)
		require.NoError(t, err)
		defer closeOrFail(t, response.Body)

		// When
		require.NoError(t, socket.Close())

		// Then

	})
}
func closeOrFail(t *testing.T, c io.Closer) {
	err := c.Close()
	if err != nil {
		t.Error(err)
	}
}

func assertJsonEqual(t *testing.T, expectedJson, actualJson string) {
	t.Helper()
	assert.Equal(t, jsontools.PrettyJSONString(expectedJson), jsontools.PrettyJSONString(actualJson))
}
