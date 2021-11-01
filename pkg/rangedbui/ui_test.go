package rangedbui_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/pkg/rangedbui"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_Index(t *testing.T) {
	// Given
	ui := rangedbui.New(nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()

	// When
	ui.ServeHTTP(response, request)

	// Then
	assert.Equal(t, http.StatusFound, response.Code)
	assert.Equal(t, "/aggregate-types", response.Header().Get("Location"))
}

func Test_ListAggregateTypes(t *testing.T) {
	t.Run("renders list of aggregate types", func(t *testing.T) {
		// Given
		store, aggregateTypeStats := storeWithTwoEvents(t)
		ui := rangedbui.New(aggregateTypeStats, store)
		request := httptest.NewRequest(http.MethodGet, "/aggregate-types", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing")
		assert.Contains(t, response.Body.String(), "another")
	})
}

func Test_AggregateType(t *testing.T) {
	// Given
	store, aggregateTypeStats := storeWithTwoEvents(t)
	rangedbtest.BlockingSaveEvents(t, store,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{
			ID:     "1ce1d596e54744b3b878d579ccc31d81",
			Number: 0,
		}},
	)

	ui := rangedbui.New(aggregateTypeStats, store)

	t.Run("renders events by aggregate type", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/e/thing", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing")
		assert.Contains(t, response.Body.String(), "thing (2)")
		assert.Contains(t, response.Body.String(), "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac")
		assert.Contains(t, response.Body.String(), "/e/thing/1ce1d596e54744b3b878d579ccc31d81")
	})

	t.Run("renders events by aggregate type, one record per page, 1st page", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/e/thing?itemsPerPage=1&page=1", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing (2)")
		assert.Contains(t, response.Body.String(), "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac")
		assert.NotContains(t, response.Body.String(), "/e/thing/1ce1d596e54744b3b878d579ccc31d81")
		assert.NotContains(t, response.Body.String(), "/e/thing?itemsPerPage=1&amp;page=1")
		assert.Contains(t, response.Body.String(), "/e/thing?itemsPerPage=1&amp;page=2")
	})

	t.Run("renders events by aggregate type, one record per page, 2nd page", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/e/thing?itemsPerPage=1&page=2", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing (2)")
		assert.NotContains(t, response.Body.String(), "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac")
		assert.Contains(t, response.Body.String(), "/e/thing/1ce1d596e54744b3b878d579ccc31d81")
		assert.Contains(t, response.Body.String(), "/e/thing?itemsPerPage=1&amp;page=1")
		assert.NotContains(t, response.Body.String(), "/e/thing?itemsPerPage=1&amp;page=2")
		assert.NotContains(t, response.Body.String(), "/e/thing?itemsPerPage=1&amp;page=3")
	})
}

func Test_Stream(t *testing.T) {
	// Given
	store, aggregateTypeStats := storeWithTwoEvents(t)
	rangedbtest.BlockingSaveEvents(t, store,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{
			ID:     "f6b6f8ed682c4b5180f625e53b3c4bac",
			Number: 0,
		}},
	)
	ui := rangedbui.New(aggregateTypeStats, store)

	t.Run("renders events by stream", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing!f6b6f8ed682c4b5180f625e53b3c4bac (2)")
		assert.Contains(t, response.Body.String(), "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac")
	})

	t.Run("renders events by stream, one record per page, 1st page", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac?itemsPerPage=1&page=1", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing!f6b6f8ed682c4b5180f625e53b3c4bac (2)")
		assert.Contains(t, response.Body.String(), "f6b6f8ed682c4b5180f625e53b3c4bac")
		assert.NotContains(t, response.Body.String(), "01f96eb13c204a7699d2138e7d64639b")
		assert.NotContains(t, response.Body.String(), "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac?itemsPerPage=1&amp;page=1")
		assert.Contains(t, response.Body.String(), "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac?itemsPerPage=1&amp;page=2")
	})
}

func Test_Stream_UsingCustomTemplates(t *testing.T) {
	// Given
	store, aggregateTypeStats := storeWithTwoEvents(t)
	rangedbtest.BlockingSaveEvents(t, store,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{
			ID:     "f6b6f8ed682c4b5180f625e53b3c4bac",
			Number: 0,
		}},
	)

	ui := rangedbui.New(
		aggregateTypeStats,
		store,
		rangedbui.WithTemplateFS(os.DirFS(".")),
	)

	t.Run("renders events by stream", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac", nil)
		response := httptest.NewRecorder()

		// When
		ui.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "text/html; charset=utf-8", response.Header().Get("Content-Type"))
		assert.Contains(t, response.Body.String(), "thing!f6b6f8ed682c4b5180f625e53b3c4bac (2)")
		assert.Contains(t, response.Body.String(), "/e/thing/f6b6f8ed682c4b5180f625e53b3c4bac")
	})
}

func Test_ServesStaticAssets(t *testing.T) {
	// Given
	ui := rangedbui.New(nil, nil)
	request := httptest.NewRequest(http.MethodGet, "/static/css/foundation-6.5.3.min.css", nil)
	response := httptest.NewRecorder()

	// When
	ui.ServeHTTP(response, request)

	// Then
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "text/css; charset=utf-8", response.Header().Get("Content-Type"))
}

func Test_RealtimeEventsByAggregateType(t *testing.T) {
	uuid := rangedbtest.NewSeededUUIDGenerator()
	store, aggregateTypeStats := storeWithTwoEvents(t,
		inmemorystore.WithUUIDGenerator(uuid),
		inmemorystore.WithClock(sequentialclock.New()),
	)
	event := rangedbtest.ThingWasDone{
		ID:     "f6b6f8ed682c4b5180f625e53b3c4bac",
		Number: 1,
	}
	rangedbtest.BlockingSaveEvents(t, store,
		&rangedb.EventRecord{Event: event},
	)
	ui := rangedbui.New(aggregateTypeStats, store)

	t.Run("reads two events", func(t *testing.T) {
		// Given
		server := httptest.NewServer(ui)
		t.Cleanup(server.Close)

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)
		serverURL.Scheme = "ws"
		serverURL.Path = "/live/e/" + event.AggregateType()

		// When
		socket, response, err := websocket.DefaultDialer.Dial(serverURL.String(), nil)

		// Then
		require.NoError(t, err)
		errCleanup(t, socket)
		errCleanup(t, response.Body)
		_, actualBytes1, err := socket.ReadMessage()
		require.NoError(t, err)
		_, actualBytes2, err := socket.ReadMessage()
		require.NoError(t, err)
		expectedEvent1 := fmt.Sprintf(`{
				"Record": {
					"aggregateType": "thing",
					"aggregateID": "%s",
					"globalSequenceNumber":1,
					"streamSequenceNumber":1,
					"insertTimestamp":0,
					"eventID": "%s",
					"eventType": "ThingWasDone",
					"data":{
						"id": "%s",
						"number": 0
					},
					"metadata":null
				},
				"TotalEvents": 1
			}`,
			event.AggregateID(),
			uuid.Get(1),
			event.AggregateID(),
		)
		expectedEvent2 := fmt.Sprintf(`{
				"Record": {
					"aggregateType": "thing",
					"aggregateID": "%s",
					"globalSequenceNumber":3,
					"streamSequenceNumber":2,
					"insertTimestamp":2,
					"eventID": "%s",
					"eventType": "ThingWasDone",
					"data":{
						"id": "%s",
						"number": 1
					},
					"metadata":null
				},
				"TotalEvents": 2
			}`,
			event.AggregateID(),
			uuid.Get(3),
			event.AggregateID(),
		)
		assert.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)
		assert.JSONEq(t, expectedEvent1, string(actualBytes1))
		assert.JSONEq(t, expectedEvent2, string(actualBytes2))
	})
}

func storeWithTwoEvents(t *testing.T, options ...inmemorystore.Option) (rangedb.Store, *projection.AggregateTypeStats) {
	store := inmemorystore.New(options...)
	aggregateTypeStats := projection.NewAggregateTypeStats()
	blockingSubscriber := rangedbtest.NewBlockingSubscriber(aggregateTypeStats)

	ctx := rangedbtest.TimeoutContext(t)
	subscription := store.AllEventsSubscription(ctx, 10, blockingSubscriber)
	require.NoError(t, subscription.Start())

	rangedbtest.SaveEvents(t, store,
		&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{
			ID:     "f6b6f8ed682c4b5180f625e53b3c4bac",
			Number: 0,
		}},
	)
	rangedbtest.SaveEvents(t, store,
		&rangedb.EventRecord{Event: rangedbtest.AnotherWasComplete{
			ID: "5e4a649230924041a7ccf18887ccc153",
		}},
	)
	rangedbtest.ReadRecord(t, blockingSubscriber.Records)
	rangedbtest.ReadRecord(t, blockingSubscriber.Records)

	return store, aggregateTypeStats
}

func errCleanup(t *testing.T, c io.Closer) {
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})
}
