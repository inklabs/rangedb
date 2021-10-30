package rangedbws_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

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
			uuid := rangedbtest.NewSeededUUIDGenerator()
			store := inmemorystore.New(
				inmemorystore.WithClock(sequentialclock.New()),
				inmemorystore.WithUUIDGenerator(uuid),
			)
			const (
				aggregateID1 = "0315b9a9c2cf460483551b6bb8b59671"
				aggregateID2 = "477c1dac64c745dbb3968a75af5d23f5"
			)

			event1 := &rangedbtest.ThingWasDone{ID: aggregateID1, Number: 1}
			event2 := &rangedbtest.ThingWasDone{ID: aggregateID2, Number: 2}
			rangedbtest.BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event1})
			rangedbtest.BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event2})
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)

			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)
			serverURL.Scheme = "ws"
			serverURL.Path = "/events"

			// When
			socket, response, err := websocket.DefaultDialer.Dial(serverURL.String(), nil)

			// Then
			require.NoError(t, err)
			defer closeOrFail(t, socket)
			defer closeOrFail(t, response.Body)
			_, actualBytes1, err := socket.ReadMessage()
			require.NoError(t, err)
			_, actualBytes2, err := socket.ReadMessage()
			require.NoError(t, err)
			expectedEvent1 := fmt.Sprintf(`{
				"aggregateType": "thing",
				"aggregateID": "%s",
				"globalSequenceNumber":1,
				"streamSequenceNumber":1,
				"insertTimestamp":0,
				"eventID": "%s",
				"eventType": "ThingWasDone",
				"data":{
					"id": "%s",
					"number": 1
				},
				"metadata":null
			}`,
				aggregateID1,
				uuid.Get(1),
				aggregateID1,
			)
			expectedEvent2 := fmt.Sprintf(`{
				"aggregateType": "thing",
				"aggregateID": "%s",
				"globalSequenceNumber":2,
				"streamSequenceNumber":1,
				"insertTimestamp":1,
				"eventID": "%s",
				"eventType": "ThingWasDone",
				"data":{
					"id": "%s",
					"number": 2
				},
				"metadata":null
			}`,
				aggregateID2,
				uuid.Get(2),
				aggregateID2,
			)
			assert.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)
			assertJsonEqual(t, expectedEvent1, string(actualBytes1))
			assertJsonEqual(t, expectedEvent2, string(actualBytes2))
		})

		t.Run("reads two events, starting from second event", func(t *testing.T) {
			// Given
			uuid := rangedbtest.NewSeededUUIDGenerator()
			store := inmemorystore.New(
				inmemorystore.WithClock(sequentialclock.New()),
				inmemorystore.WithUUIDGenerator(uuid),
			)
			const (
				aggregateID1 = "0d18c8603167498ca4004519a24268a7"
				aggregateID2 = "153d2820edee4fa5ac040640bd1900ed"
				aggregateID3 = "4a32af1f745c4975a7f5784695e1ba49"
			)
			event1 := &rangedbtest.ThingWasDone{ID: aggregateID1, Number: 1}
			event2 := &rangedbtest.ThingWasDone{ID: aggregateID2, Number: 2}
			event3 := &rangedbtest.ThingWasDone{ID: aggregateID3, Number: 3}
			rangedbtest.BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event1})
			rangedbtest.BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event2})
			rangedbtest.BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event3})
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)

			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			const globalSequenceNumber = 2

			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)
			serverURL.Scheme = "ws"
			serverURL.Path = "/events"
			query := url.Values{}
			query.Add("global-sequence-number", strconv.Itoa(globalSequenceNumber))
			serverURL.RawQuery = query.Encode()

			// When
			socket, response, err := websocket.DefaultDialer.Dial(serverURL.String(), nil)

			// Then
			require.NoError(t, err, serverURL.String())
			defer closeOrFail(t, socket)
			defer closeOrFail(t, response.Body)
			_, actualBytes1, err := socket.ReadMessage()
			require.NoError(t, err)
			_, actualBytes2, err := socket.ReadMessage()
			require.NoError(t, err)
			expectedEvent1 := fmt.Sprintf(`{
				"aggregateType": "thing",
				"aggregateID": "%s",
				"globalSequenceNumber":2,
				"streamSequenceNumber":1,
				"insertTimestamp":1,
				"eventID": "%s",
				"eventType": "ThingWasDone",
				"data":{
					"id": "%s",
					"number": 2
				},
				"metadata":null
			}`,
				aggregateID2,
				uuid.Get(2),
				aggregateID2,
			)
			expectedEvent2 := fmt.Sprintf(`{
				"aggregateType": "thing",
				"aggregateID": "%s",
				"globalSequenceNumber":3,
				"streamSequenceNumber":1,
				"insertTimestamp":2,
				"eventID": "%s",
				"eventType": "ThingWasDone",
				"data":{
					"id": "%s",
					"number": 3
				},
				"metadata":null
			}`,
				aggregateID3,
				uuid.Get(3),
				aggregateID3,
			)
			assert.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)
			assertJsonEqual(t, expectedEvent1, string(actualBytes1))
			assertJsonEqual(t, expectedEvent2, string(actualBytes2))
		})

		t.Run("errors from invalid global sequence number", func(t *testing.T) {
			// Given
			store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)

			server := httptest.NewServer(api)
			t.Cleanup(server.Close)

			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)
			serverURL.Scheme = "ws"
			serverURL.Path = "/events"
			query := url.Values{}
			query.Add("global-sequence-number", "invalid")
			serverURL.RawQuery = query.Encode()

			// When
			socket, response, err := websocket.DefaultDialer.Dial(serverURL.String(), nil)

			// Then
			require.EqualError(t, err, "websocket: bad handshake")
			assert.Equal(t, http.StatusBadRequest, response.StatusCode)
			assert.Nil(t, socket)
			closeOrFail(t, response.Body)
		})

		t.Run("unable to upgrade HTTP connection to the WebSocket protocol", func(t *testing.T) {
			// Given
			store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)

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
			failingStore := rangedbtest.NewFailingEventStore()
			api, err := rangedbws.New(rangedbws.WithStore(failingStore))
			require.NoError(t, err)
			t.Cleanup(api.Stop)

			server := httptest.NewServer(api)
			t.Cleanup(server.Close)

			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)
			serverURL.Scheme = "ws"
			serverURL.Path = "/events"

			ctx, done := context.WithCancel(rangedbtest.TimeoutContext(t))

			// When
			socket, response, err := websocket.DefaultDialer.DialContext(ctx, serverURL.String(), nil)

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
			uuid := rangedbtest.NewSeededUUIDGenerator()
			store := inmemorystore.New(
				inmemorystore.WithClock(sequentialclock.New()),
				inmemorystore.WithUUIDGenerator(uuid),
			)
			const (
				aggregateID1 = "66b7516e209a4ec39ca798a35467021b"
				aggregateID2 = "8e09afd055214ca0bae59b38ddb7d8b8"
				aggregateID3 = "e1eb6179b4034183a55d8315111db3fa"
			)
			event1 := &rangedbtest.ThingWasDone{ID: aggregateID1, Number: 1}
			event2 := &rangedbtest.AnotherWasComplete{ID: aggregateID2}
			event3 := &rangedbtest.ThatWasDone{ID: aggregateID3}
			rangedbtest.BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event1})
			rangedbtest.BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event2})
			rangedbtest.BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event3})
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)

			server := httptest.NewServer(api)
			t.Cleanup(server.Close)

			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)
			serverURL.Scheme = "ws"
			serverURL.Path = "/events/thing,that"

			// When
			socket, response, err := websocket.DefaultDialer.Dial(serverURL.String(), nil)

			// Then
			require.NoError(t, err)
			defer closeOrFail(t, socket)
			defer closeOrFail(t, response.Body)
			_, actualBytes1, err := socket.ReadMessage()
			require.NoError(t, err)
			_, actualBytes2, err := socket.ReadMessage()
			require.NoError(t, err)
			expectedEvent1 := fmt.Sprintf(`{
				"aggregateType": "thing",
				"aggregateID": "%s",
				"globalSequenceNumber":1,
				"streamSequenceNumber":1,
				"insertTimestamp":0,
				"eventID": "%s",
				"eventType": "ThingWasDone",
				"data":{
					"id": "%s",
					"number": 1
				},
				"metadata":null
			}`,
				aggregateID1,
				uuid.Get(1),
				aggregateID1,
			)
			expectedEvent2 := fmt.Sprintf(`{
				"aggregateType": "that",
				"aggregateID": "%s",
				"globalSequenceNumber":3,
				"streamSequenceNumber":1,
				"insertTimestamp":2,
				"eventID": "%s",
				"eventType": "ThatWasDone",
				"data":{
					"ID": "%s"
				},
				"metadata":null
			}`,
				aggregateID3,
				uuid.Get(3),
				aggregateID3,
			)
			assert.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)
			assertJsonEqual(t, expectedEvent1, string(actualBytes1))
			assertJsonEqual(t, expectedEvent2, string(actualBytes2))
		})

		t.Run("errors from invalid global sequence number", func(t *testing.T) {
			// Given
			store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
			api, err := rangedbws.New(rangedbws.WithStore(store))
			require.NoError(t, err)
			t.Cleanup(api.Stop)

			server := httptest.NewServer(api)
			t.Cleanup(server.Close)

			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)
			serverURL.Scheme = "ws"
			serverURL.Path = "/events/thing,that"
			query := url.Values{}
			query.Add("global-sequence-number", "invalid")
			serverURL.RawQuery = query.Encode()

			// When
			socket, response, err := websocket.DefaultDialer.Dial(serverURL.String(), nil)

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
		rangedbtest.BlockingSaveEvents(t, store,
			&rangedb.EventRecord{Event: event},
		)
		api, err := rangedbws.New(rangedbws.WithStore(store))
		require.NoError(t, err)
		t.Cleanup(api.Stop)

		server := httptest.NewServer(api)
		t.Cleanup(server.Close)

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)
		serverURL.Scheme = "ws"
		serverURL.Path = "/events"

		socket, response, err := websocket.DefaultDialer.Dial(serverURL.String(), nil)
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
		assert.EqualError(t, err, "failingRecordSubscription.Start")
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
