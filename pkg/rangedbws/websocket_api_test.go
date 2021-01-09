package rangedbws_test

import (
	"context"
	"fmt"
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

func Test_WebsocketApi(t *testing.T) {
	t.Run("all events", func(t *testing.T) {
		t.Run("reads two events", func(t *testing.T) {
			// Given
			uuid.SetRand(rand.New(rand.NewSource(100)))
			store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
			event1 := &rangedbtest.ThingWasDone{ID: "6595a5c206c746c3a9d9006c7df5784e", Number: 1}
			event2 := &rangedbtest.ThingWasDone{ID: "8cc839e1fd3545b7a0fe67808d84cbd4", Number: 2}
			rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event1})
			rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event2})
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)
			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			url := fmt.Sprintf("ws://%s/events", strings.TrimPrefix(server.URL, "http://"))

			// When
			socket, response, err := websocket.DefaultDialer.Dial(url, nil)

			// Then
			require.NoError(t, err)
			defer closeOrFail(t, socket)
			defer closeOrFail(t, response.Body)
			_, actualBytes1, err := socket.ReadMessage()
			require.NoError(t, err)
			_, actualBytes2, err := socket.ReadMessage()
			require.NoError(t, err)
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
		})

		t.Run("reads two events, starting from second event", func(t *testing.T) {
			// Given
			uuid.SetRand(rand.New(rand.NewSource(100)))
			store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
			event1 := &rangedbtest.ThingWasDone{ID: "24f4ffa9bd2c43468241d648f16b6786", Number: 1}
			event2 := &rangedbtest.ThingWasDone{ID: "b75064a917054396a9bb3a6b46d7bd4c", Number: 2}
			event3 := &rangedbtest.ThingWasDone{ID: "d8e3d651d1e9477d9af18893a2e337b9", Number: 3}
			rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event1})
			rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event2})
			rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event3})
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)
			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			const globalSequenceNumber = 1
			url := fmt.Sprintf("ws://%s/events?global-sequence-number=%d", strings.TrimPrefix(server.URL, "http://"), globalSequenceNumber)

			// When
			socket, response, err := websocket.DefaultDialer.Dial(url, nil)

			// Then
			require.NoError(t, err)
			defer closeOrFail(t, socket)
			defer closeOrFail(t, response.Body)
			_, actualBytes1, err := socket.ReadMessage()
			require.NoError(t, err)
			_, actualBytes2, err := socket.ReadMessage()
			require.NoError(t, err)
			expectedEvent1 := `{
				"aggregateType": "thing",
				"aggregateID": "b75064a917054396a9bb3a6b46d7bd4c",
				"globalSequenceNumber":1,
				"sequenceNumber":0,
				"insertTimestamp":1,
				"eventID": "99cbd88bbcaf482ba1cc96ed12541707",
				"eventType": "ThingWasDone",
				"data":{
					"id": "b75064a917054396a9bb3a6b46d7bd4c",
					"number": 2
				},
				"metadata":null
			}`
			expectedEvent2 := `{
				"aggregateType": "thing",
				"aggregateID": "d8e3d651d1e9477d9af18893a2e337b9",
				"globalSequenceNumber":2,
				"sequenceNumber":0,
				"insertTimestamp":2,
				"eventID": "2e9e6918af10498cb7349c89a351fdb7",
				"eventType": "ThingWasDone",
				"data":{
					"id": "d8e3d651d1e9477d9af18893a2e337b9",
					"number": 3
				},
				"metadata":null
			}`
			assert.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)
			assertJsonEqual(t, expectedEvent1, string(actualBytes1))
			assertJsonEqual(t, expectedEvent2, string(actualBytes2))
		})

		t.Run("errors from invalid global sequence number", func(t *testing.T) {
			// Given
			uuid.SetRand(rand.New(rand.NewSource(100)))
			store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)
			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			url := fmt.Sprintf("ws://%s/events?global-sequence-number=invalid", strings.TrimPrefix(server.URL, "http://"))

			// When
			socket, response, err := websocket.DefaultDialer.Dial(url, nil)

			// Then
			require.EqualError(t, err, "websocket: bad handshake")
			assert.Equal(t, http.StatusBadRequest, response.StatusCode)
			assert.Nil(t, socket)
			closeOrFail(t, response.Body)
		})

		t.Run("unable to upgrade HTTP connection to the WebSocket protocol", func(t *testing.T) {
			// Given
			uuid.SetRand(rand.New(rand.NewSource(100)))
			store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)
			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			request := httptest.NewRequest(http.MethodGet, "/events", nil)
			response := httptest.NewRecorder()

			// When
			api.ServeHTTP(response, request)

			// Then
			require.Equal(t, http.StatusBadRequest, response.Code)
			assert.Equal(t, "Bad Request\nunable to upgrade websocket connection\n", response.Body.String())
		})

		t.Run("unable to send first event from failing store", func(t *testing.T) {
			// Given
			uuid.SetRand(rand.New(rand.NewSource(100)))
			failingStore := rangedbtest.NewFailingEventStore()
			ctx := rangedbtest.TimeoutContext(t)
			api, err := rangedbws.New(rangedbws.WithStore(failingStore))
			require.NoError(t, err)
			t.Cleanup(api.Stop)
			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			url := fmt.Sprintf("ws://%s/events", strings.TrimPrefix(server.URL, "http://"))
			ctx, done := context.WithCancel(rangedbtest.TimeoutContext(t))

			// When
			socket, response, err := websocket.DefaultDialer.DialContext(ctx, url, nil)

			// Then
			require.NoError(t, err)
			done()
			require.NoError(t, socket.Close())
			defer closeOrFail(t, response.Body)
		})
	})

	t.Run("events by aggregate types", func(t *testing.T) {
		t.Run("reads two events", func(t *testing.T) {
			// Given
			uuid.SetRand(rand.New(rand.NewSource(100)))
			store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
			event1 := &rangedbtest.ThingWasDone{ID: "c0f61ffbe61a418383244277e9ee6084", Number: 1}
			event2 := &rangedbtest.AnotherWasComplete{ID: "0ebd3f223484462185cf0e78da083696"}
			event3 := &rangedbtest.ThatWasDone{ID: "14d50d2cbf8d4b2cbdac21308a4f8155"}
			rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event1})
			rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event2})
			rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event3})
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)
			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			url := fmt.Sprintf("ws://%s/events/thing,that", strings.TrimPrefix(server.URL, "http://"))

			// When
			socket, response, err := websocket.DefaultDialer.Dial(url, nil)

			// Then
			require.NoError(t, err)
			defer closeOrFail(t, socket)
			defer closeOrFail(t, response.Body)
			_, actualBytes1, err := socket.ReadMessage()
			require.NoError(t, err)
			_, actualBytes2, err := socket.ReadMessage()
			require.NoError(t, err)
			expectedEvent1 := `{
				"aggregateType": "thing",
				"aggregateID": "c0f61ffbe61a418383244277e9ee6084",
				"globalSequenceNumber":0,
				"sequenceNumber":0,
				"insertTimestamp":0,
				"eventID": "d2ba8e70072943388203c438d4e94bf3",
				"eventType": "ThingWasDone",
				"data":{
					"id": "c0f61ffbe61a418383244277e9ee6084",
					"number": 1
				},
				"metadata":null
			}`
			expectedEvent2 := `{
				"aggregateType": "that",
				"aggregateID": "14d50d2cbf8d4b2cbdac21308a4f8155",
				"globalSequenceNumber":2,
				"sequenceNumber":0,
				"insertTimestamp":2,
				"eventID": "2e9e6918af10498cb7349c89a351fdb7",
				"eventType": "ThatWasDone",
				"data":{
					"ID": "14d50d2cbf8d4b2cbdac21308a4f8155"
				},
				"metadata":null
			}`
			assert.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)
			assertJsonEqual(t, expectedEvent1, string(actualBytes1))
			assertJsonEqual(t, expectedEvent2, string(actualBytes2))
		})

		t.Run("errors from invalid global sequence number", func(t *testing.T) {
			// Given
			uuid.SetRand(rand.New(rand.NewSource(100)))
			store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)
			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			url := fmt.Sprintf("ws://%s/events/thing,that?global-sequence-number=invalid", strings.TrimPrefix(server.URL, "http://"))

			// When
			socket, response, err := websocket.DefaultDialer.Dial(url, nil)

			// Then
			require.EqualError(t, err, "websocket: bad handshake")
			assert.Equal(t, http.StatusBadRequest, response.StatusCode)
			assert.Nil(t, socket)
			closeOrFail(t, response.Body)
		})
	})
}

func Test_WebsocketApi_Failures(t *testing.T) {
	t.Run("all events fails when upgrading connection", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		api, err := rangedbws.New(rangedbws.WithStore(store))
		require.NoError(t, err)
		t.Cleanup(api.Stop)
		request := httptest.NewRequest(http.MethodGet, "/events", nil)
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
		api, err := rangedbws.New(rangedbws.WithStore(store))
		require.NoError(t, err)
		t.Cleanup(api.Stop)
		request := httptest.NewRequest(http.MethodGet, "/events/thing", nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "Bad Request\nunable to upgrade websocket connection\n", response.Body.String())
	})

	t.Run("errors when writing existing events to connection", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		event := &rangedbtest.ThingWasDone{ID: "372b47686e1b43d29d2fd48f2a0e83f0", Number: 1}
		rangedbtest.SaveEvents(t, store,
			&rangedb.EventRecord{Event: event},
		)
		api, err := rangedbws.New(rangedbws.WithStore(store))
		require.NoError(t, err)
		t.Cleanup(api.Stop)
		server := httptest.NewServer(api)
		t.Cleanup(server.Close)

		url := "ws" + strings.TrimPrefix(server.URL, "http") + "/events"
		socket, response, err := websocket.DefaultDialer.Dial(url, nil)
		require.NoError(t, err)
		defer closeOrFail(t, response.Body)

		// When
		require.NoError(t, socket.Close())

		// Then

	})

	t.Run("errors from failing store", func(t *testing.T) {
		// Given
		failingStore := rangedbtest.NewFailingSubscribeEventStore()

		// When
		api, err := rangedbws.New(rangedbws.WithStore(failingStore))

		// Then
		assert.EqualError(t, err, "failingSubscribeEventStore.Subscribe")
		assert.Nil(t, api)
	})

}

func closeOrFail(t *testing.T, c io.Closer) {
	require.NoError(t, c.Close())
}

func assertJsonEqual(t *testing.T, expectedJson, actualJson string) {
	t.Helper()
	assert.Equal(t, jsontools.PrettyJSONString(expectedJson), jsontools.PrettyJSONString(actualJson))
}
