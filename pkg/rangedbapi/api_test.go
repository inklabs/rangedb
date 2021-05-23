package rangedbapi_test

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/jsontools"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/provider/msgpackrecordiostream"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestApi_HealthCheck(t *testing.T) {
	// Given
	api, err := rangedbapi.New()
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodGet, "/health-check", nil)

	t.Run("regular response", func(t *testing.T) {
		// Given
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"OK"}`, response.Body.String())
	})

	t.Run("gzip response", func(t *testing.T) {
		// Given
		request.Header.Add("Accept-Encoding", "gzip")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		require.Equal(t, "gzip", response.Header().Get("Content-Encoding"))
		assert.Equal(t, `{"status":"OK"}`, readGzippedBody(t, response.Body))
	})
}

func TestApi_SaveEvents(t *testing.T) {
	// Given
	singleJsonEvent := `[
		{
			"eventType": "ThingWasDone",
			"data":{
				"id": "0a403cfe0e8c4284b2107e12bbe19881",
				"number": 100
			},
			"metadata":null
		}
	]`

	t.Run("saves from json", func(t *testing.T) {
		// Given
		const aggregateID = "2c12be033de7402d9fb28d9b635b3330"
		const aggregateType = "thing"
		api, err := rangedbapi.New()
		require.NoError(t, err)
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, saveUri, strings.NewReader(singleJsonEvent))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusCreated, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"OK","streamSequenceNumber":1}`, response.Body.String())
	})

	t.Run("saves from json with expected stream sequence number", func(t *testing.T) {
		// Given
		jsonEvent := `[
			{
				"eventType": "ThingWasDone",
				"data":{
					"id": "0a403cfe0e8c4284b2107e12bbe19881",
					"number": 100
				},
				"metadata":null
			}
		]`

		const aggregateID = "2c12be033de7402d9fb28d9b635b3330"
		const aggregateType = "thing"
		api, err := rangedbapi.New()
		require.NoError(t, err)
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, saveUri, strings.NewReader(jsonEvent))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("ExpectedStreamSequenceNumber", "0")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusCreated, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"OK","streamSequenceNumber":1}`, response.Body.String())
	})

	t.Run("saves 2 events from json with expected stream sequence number", func(t *testing.T) {
		// Given
		jsonEvent := `[
			{
				"eventType": "ThingWasDone",
				"data":{
					"id": "0a403cfe0e8c4284b2107e12bbe19881",
					"number": 100
				},
				"metadata":null
			},
			{
				"eventType": "ThingWasDone",
				"data":{
					"id": "0a403cfe0e8c4284b2107e12bbe19881",
					"number": 100
				},
				"metadata":null
			}
		]`

		const aggregateID = "2c12be033de7402d9fb28d9b635b3330"
		const aggregateType = "thing"
		api, err := rangedbapi.New()
		require.NoError(t, err)
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, saveUri, strings.NewReader(jsonEvent))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("ExpectedStreamSequenceNumber", "0")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusCreated, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"OK","streamSequenceNumber":2}`, response.Body.String())
	})

	t.Run("errors to save from json with wrong expected stream sequence number", func(t *testing.T) {
		// Given
		jsonEvent := `[
			{
				"eventType": "ThingWasDone",
				"data":{
					"id": "0a403cfe0e8c4284b2107e12bbe19881",
					"number": 100
				},
				"metadata":null
			}
		]`

		const aggregateID = "2c12be033de7402d9fb28d9b635b3330"
		const aggregateType = "thing"
		api, err := rangedbapi.New()
		require.NoError(t, err)
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, saveUri, strings.NewReader(jsonEvent))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("ExpectedStreamSequenceNumber", "1")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed", "message": "unexpected sequence number: 1, actual: 0"}`, response.Body.String())
	})

	t.Run("errors when content type not set", func(t *testing.T) {
		// Given
		const aggregateID = "2c12be033de7402d9fb28d9b635b3330"
		const aggregateType = "thing"
		api, err := rangedbapi.New()
		require.NoError(t, err)
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, saveUri, strings.NewReader(singleJsonEvent))
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "invalid content type\n", response.Body.String())
	})

	t.Run("errors when store save errors", func(t *testing.T) {
		// Given
		const aggregateID = "cbba5f386b2d4924ac34d1b9e9217d67"
		const aggregateType = "thing"
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		api, err := rangedbapi.New(
			rangedbapi.WithStore(rangedbtest.NewFailingEventStore()),
			rangedbapi.WithLogger(logger),
		)
		require.NoError(t, err)
		jsonBody := `[
		{
			"eventType": "ThingWasDone",
			"data":{
				"Name": "Thing Test",
				"Timestamp": 1546302589
			},
			"metadata":null
		}
	]`
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, saveUri, strings.NewReader(jsonBody))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusInternalServerError, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed"}`, response.Body.String())
		assert.Equal(t, "unable to save: failingEventStore.Save\n", logBuffer.String())
	})

	t.Run("errors when input json is invalid", func(t *testing.T) {
		// Given
		const aggregateID = "cbba5f386b2d4924ac34d1b9e9217d67"
		const aggregateType = "thing"
		api, err := rangedbapi.New(rangedbapi.WithStore(rangedbtest.NewFailingEventStore()))
		require.NoError(t, err)
		invalidJson := `x`
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, saveUri, strings.NewReader(invalidJson))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed", "message": "invalid json request body"}`, response.Body.String())
	})

	t.Run("errors from invalid expected stream sequence number", func(t *testing.T) {
		// Given
		const aggregateID = "2c12be033de7402d9fb28d9b635b3330"
		const aggregateType = "thing"
		api, err := rangedbapi.New()
		require.NoError(t, err)
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, saveUri, strings.NewReader(singleJsonEvent))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("ExpectedStreamSequenceNumber", "xyz")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed", "message": "invalid ExpectedStreamSequenceNumber"}`, response.Body.String())
	})
}

func TestApi_DeleteStream(t *testing.T) {
	t.Run("deletes stream with 2 events", func(t *testing.T) {
		// Given
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		api, err := rangedbapi.New(rangedbapi.WithStore(store))
		require.NoError(t, err)
		const aggregateID = "439b8969c82b43d6bf0ee219b42d93ac"
		const aggregateType = "thing"
		shortuuid.SetRand(100)
		saveEvents(t, api, aggregateType, aggregateID,
			SaveEventRequest{
				EventType: "ThingWasDone",
				Data: rangedbtest.ThingWasDone{
					ID:     aggregateID,
					Number: 100,
				},
				Metadata: nil,
			},
			SaveEventRequest{
				EventType: "ThingWasDone",
				Data: rangedbtest.ThingWasDone{
					ID:     aggregateID,
					Number: 200,
				},
				Metadata: nil,
			},
		)

		deleteStreamUri := fmt.Sprintf("/delete-stream/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, deleteStreamUri, nil)
		request.Header.Set("ExpectedStreamSequenceNumber", "2")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"OK","eventsDeleted":2}`, response.Body.String())
	})

	t.Run("errors from wrong expected stream sequence number", func(t *testing.T) {
		// Given
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		api, err := rangedbapi.New(rangedbapi.WithStore(store))
		require.NoError(t, err)
		const aggregateID = "439b8969c82b43d6bf0ee219b42d93ac"
		const aggregateType = "thing"
		shortuuid.SetRand(100)
		saveEvents(t, api, aggregateType, aggregateID,
			SaveEventRequest{
				EventType: "ThingWasDone",
				Data: rangedbtest.ThingWasDone{
					ID:     aggregateID,
					Number: 100,
				},
				Metadata: nil,
			},
			SaveEventRequest{
				EventType: "ThingWasDone",
				Data: rangedbtest.ThingWasDone{
					ID:     aggregateID,
					Number: 200,
				},
				Metadata: nil,
			},
		)

		deleteStreamUri := fmt.Sprintf("/delete-stream/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, deleteStreamUri, nil)
		request.Header.Set("ExpectedStreamSequenceNumber", "3")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed", "message": "unexpected sequence number: 3, actual: 2"}`, response.Body.String())
	})

	t.Run("errors from invalid expected stream sequence number", func(t *testing.T) {
		// Given
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		api, err := rangedbapi.New(rangedbapi.WithStore(store))
		require.NoError(t, err)
		const aggregateID = "439b8969c82b43d6bf0ee219b42d93ac"
		const aggregateType = "thing"
		shortuuid.SetRand(100)
		saveEvents(t, api, aggregateType, aggregateID,
			SaveEventRequest{
				EventType: "ThingWasDone",
				Data: rangedbtest.ThingWasDone{
					ID:     aggregateID,
					Number: 100,
				},
				Metadata: nil,
			},
			SaveEventRequest{
				EventType: "ThingWasDone",
				Data: rangedbtest.ThingWasDone{
					ID:     aggregateID,
					Number: 200,
				},
				Metadata: nil,
			},
		)

		deleteStreamUri := fmt.Sprintf("/delete-stream/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, deleteStreamUri, nil)
		request.Header.Set("ExpectedStreamSequenceNumber", "-1")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed", "message": "invalid ExpectedStreamSequenceNumber"}`, response.Body.String())
	})

	t.Run("errors when store DeleteStream errors", func(t *testing.T) {
		// Given
		store := rangedbtest.NewFailingEventStore()
		api, err := rangedbapi.New(rangedbapi.WithStore(store))
		require.NoError(t, err)
		const aggregateID = "439b8969c82b43d6bf0ee219b42d93ac"
		const aggregateType = "thing"
		shortuuid.SetRand(100)

		deleteStreamUri := fmt.Sprintf("/delete-stream/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, deleteStreamUri, nil)
		request.Header.Set("ExpectedStreamSequenceNumber", "0")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusInternalServerError, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed"}`, response.Body.String())
	})

	t.Run("errors from missing stream", func(t *testing.T) {
		// Given
		store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
		api, err := rangedbapi.New(rangedbapi.WithStore(store))
		require.NoError(t, err)
		const aggregateID = "4fb43e659e2743049ae036e5c699c5e5"
		const aggregateType = "thing"
		shortuuid.SetRand(100)

		deleteStreamUri := fmt.Sprintf("/delete-stream/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest(http.MethodPost, deleteStreamUri, nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusNotFound, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed", "message": "stream not found"}`, response.Body.String())
	})
}

func TestApi_WithFourEventsSaved(t *testing.T) {
	// Given
	store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
	api, err := rangedbapi.New(rangedbapi.WithStore(store))
	require.NoError(t, err)
	const aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
	const aggregateID2 = "5b36ae984b724685917b69ae47968be1"
	const aggregateID3 = "9bc181144cef4fd19da1f32a17363997"

	shortuuid.SetRand(100)
	saveEvents(t, api, "thing", aggregateID1,
		SaveEventRequest{
			EventType: "ThingWasDone",
			Data: rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 100,
			},
			Metadata: nil,
		},
		SaveEventRequest{
			EventType: "ThingWasDone",
			Data: rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 200,
			},
			Metadata: nil,
		},
	)
	saveEvents(t, api, "thing", aggregateID2,
		SaveEventRequest{
			EventType: "ThingWasDone",
			Data: rangedbtest.ThingWasDone{
				ID:     aggregateID2,
				Number: 300,
			},
			Metadata: nil,
		},
	)
	saveEvents(t, api, "another", aggregateID3,
		SaveEventRequest{
			EventType: "AnotherWasComplete",
			Data: rangedbtest.AnotherWasComplete{
				ID: aggregateID3,
			},
			Metadata: nil,
		},
	)

	t.Run("get all events as json", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/events.json", nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		expectedJson := `[
			{
				"aggregateType": "thing",
				"aggregateID": "f187760f4d8c4d1c9d9cf17b66766abd",
				"globalSequenceNumber": 1,
				"streamSequenceNumber": 1,
				"insertTimestamp": 0,
				"eventID": "d2ba8e70072943388203c438d4e94bf3",
				"eventType": "ThingWasDone",
				"data":{
					"id": "f187760f4d8c4d1c9d9cf17b66766abd",
					"number": 100
				},
				"metadata":null
			},
			{
				"aggregateType": "thing",
				"aggregateID": "f187760f4d8c4d1c9d9cf17b66766abd",
				"globalSequenceNumber": 2,
				"streamSequenceNumber": 2,
				"insertTimestamp": 1,
				"eventID": "99cbd88bbcaf482ba1cc96ed12541707",
				"eventType": "ThingWasDone",
				"data":{
					"id": "f187760f4d8c4d1c9d9cf17b66766abd",
					"number": 200
				},
				"metadata":null
			},
			{
				"aggregateType": "thing",
				"aggregateID": "5b36ae984b724685917b69ae47968be1",
				"globalSequenceNumber": 3,
				"streamSequenceNumber": 1,
				"insertTimestamp": 2,
				"eventID": "2e9e6918af10498cb7349c89a351fdb7",
				"eventType": "ThingWasDone",
				"data":{
					"id": "5b36ae984b724685917b69ae47968be1",
					"number": 300
				},
				"metadata":null
			},
			{
				"aggregateType": "another",
				"aggregateID": "9bc181144cef4fd19da1f32a17363997",
				"globalSequenceNumber": 4,
				"streamSequenceNumber": 1,
				"insertTimestamp": 3,
				"eventID": "5042958739514c948f776fc9f820bca0",
				"eventType": "AnotherWasComplete",
				"data":{
					"id": "9bc181144cef4fd19da1f32a17363997"
				},
				"metadata":null
			}
		]`
		assertJsonEqual(t, expectedJson, response.Body.String())
	})

	t.Run("get events by stream as ndjson", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/events/thing/f187760f4d8c4d1c9d9cf17b66766abd.ndjson", nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "application/json; boundary=LF", response.Header().Get("Content-Type"))
		expectedJson := `{
			"aggregateType": "thing",
			"aggregateID": "f187760f4d8c4d1c9d9cf17b66766abd",
			"globalSequenceNumber":0,
			"streamSequenceNumber":0,
			"insertTimestamp":0,
			"eventID": "27e9965ce0ce4b65a38d1e0b7768ba27",
			"eventType": "ThingWasDone",
			"data":{
				"id": "f187760f4d8c4d1c9d9cf17b66766abd",
				"number": 100
			},
			"metadata":null
		}` + "\n" + `{
			"aggregateType": "thing",
			"aggregateID": "f187760f4d8c4d1c9d9cf17b66766abd",
			"globalSequenceNumber": 1,
			"streamSequenceNumber": 1,
			"insertTimestamp": 1,
			"eventID": "27e9965ce0ce4b65a38d1e0b7768ba27",
			"eventType": "ThingWasDone",
			"data":{
				"id": "f187760f4d8c4d1c9d9cf17b66766abd",
				"number": 200
			},
			"metadata":null
		}`
		assertJsonEqual(t, expectedJson, response.Body.String())
	})

	t.Run("get events by stream as msgpack", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/events/thing/f187760f4d8c4d1c9d9cf17b66766abd.msgpack", nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "application/msgpack", response.Header().Get("Content-Type"))
		expectedRecord1 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			InsertTimestamp:      0,
			EventID:              "d2ba8e70072943388203c438d4e94bf3",
			EventType:            "ThingWasDone",
			Data: map[string]interface{}{
				"id":     aggregateID1,
				"number": "100",
			},
			Metadata: nil,
		}
		expectedRecord2 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 2,
			StreamSequenceNumber: 2,
			InsertTimestamp:      1,
			EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
			EventType:            "ThingWasDone",
			Data: map[string]interface{}{
				"id":     aggregateID1,
				"number": "200",
			},
			Metadata: nil,
		}
		ioStream := msgpackrecordiostream.New()
		recordIterator := ioStream.Read(base64.NewDecoder(base64.RawStdEncoding, response.Body))
		rangedbtest.AssertRecordsInIterator(t, recordIterator,
			expectedRecord1,
			expectedRecord2,
		)
	})

	t.Run("get events by aggregate type", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/events/thing.json", nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		expectedJson := `[
			{
				"aggregateType": "thing",
				"aggregateID": "f187760f4d8c4d1c9d9cf17b66766abd",
				"globalSequenceNumber":1,
				"streamSequenceNumber":1,
				"insertTimestamp":0,
				"eventID": "d2ba8e70072943388203c438d4e94bf3",
				"eventType": "ThingWasDone",
				"data":{
					"id": "f187760f4d8c4d1c9d9cf17b66766abd",
					"number": 100
				},
				"metadata":null
			},
			{
				"aggregateType": "thing",
				"aggregateID": "f187760f4d8c4d1c9d9cf17b66766abd",
				"globalSequenceNumber":2,
				"streamSequenceNumber":2,
				"insertTimestamp":1,
				"eventID": "99cbd88bbcaf482ba1cc96ed12541707",
				"eventType": "ThingWasDone",
				"data":{
					"id": "f187760f4d8c4d1c9d9cf17b66766abd",
					"number": 200
				},
				"metadata":null
			},
			{
				"aggregateType": "thing",
				"aggregateID": "5b36ae984b724685917b69ae47968be1",
				"globalSequenceNumber": 3,
				"streamSequenceNumber": 1,
				"insertTimestamp": 2,
				"eventID": "2e9e6918af10498cb7349c89a351fdb7",
				"eventType": "ThingWasDone",
				"data":{
					"id": "5b36ae984b724685917b69ae47968be1",
					"number": 300
				},
				"metadata":null
			}
		]`
		assertJsonEqual(t, expectedJson, response.Body.String())
	})

	t.Run("get events by aggregate types", func(t *testing.T) {
		// Given
		request := httptest.NewRequest(http.MethodGet, "/events/thing,another.json", nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		expectedJson := `[
			{
				"aggregateType": "thing",
				"aggregateID": "f187760f4d8c4d1c9d9cf17b66766abd",
				"globalSequenceNumber":1,
				"streamSequenceNumber":1,
				"insertTimestamp":0,
				"eventID": "d2ba8e70072943388203c438d4e94bf3",
				"eventType": "ThingWasDone",
				"data":{
					"id": "f187760f4d8c4d1c9d9cf17b66766abd",
					"number": 100
				},
				"metadata":null
			},
			{
				"aggregateType": "thing",
				"aggregateID": "f187760f4d8c4d1c9d9cf17b66766abd",
				"globalSequenceNumber":2,
				"streamSequenceNumber":2,
				"insertTimestamp":1,
				"eventID": "99cbd88bbcaf482ba1cc96ed12541707",
				"eventType": "ThingWasDone",
				"data":{
					"id": "f187760f4d8c4d1c9d9cf17b66766abd",
					"number": 200
				},
				"metadata":null
			},
			{
				"aggregateType": "thing",
				"aggregateID": "5b36ae984b724685917b69ae47968be1",
				"globalSequenceNumber": 3,
				"streamSequenceNumber": 1,
				"insertTimestamp": 2,
				"eventID": "2e9e6918af10498cb7349c89a351fdb7",
				"eventType": "ThingWasDone",
				"data":{
					"id": "5b36ae984b724685917b69ae47968be1",
					"number": 300
				},
				"metadata":null
			},
			{
				"aggregateType": "another",
				"aggregateID": "9bc181144cef4fd19da1f32a17363997",
				"globalSequenceNumber": 4,
				"streamSequenceNumber": 1,
				"insertTimestamp": 3,
				"eventID": "5042958739514c948f776fc9f820bca0",
				"eventType": "AnotherWasComplete",
				"data":{
					"id": "9bc181144cef4fd19da1f32a17363997"
				},
				"metadata":null
			}
		]`
		assertJsonEqual(t, expectedJson, response.Body.String())
	})
}

func TestApi_ListAggregates(t *testing.T) {
	// Given
	store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
	event1 := rangedbtest.ThingWasDone{ID: "A", Number: 1}
	event2 := rangedbtest.ThingWasDone{ID: "A", Number: 2}
	event3 := rangedbtest.AnotherWasComplete{ID: "B"}
	rangedbtest.BlockingSaveEvents(t, store,
		&rangedb.EventRecord{Event: event1},
		&rangedb.EventRecord{Event: event2},
	)
	rangedbtest.BlockingSaveEvents(t, store,
		&rangedb.EventRecord{Event: event3},
	)
	api, err := rangedbapi.New(
		rangedbapi.WithStore(store),
		rangedbapi.WithBaseUri("http://0.0.0.0:8080"),
	)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodGet, "/list-aggregate-types", nil)
	response := httptest.NewRecorder()

	// When
	api.ServeHTTP(response, request)

	// Then
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	expectedJson := `{
		"data":[
			{
				"links": {
					"self": "http://0.0.0.0:8080/events/another.json"
				},
				"name": "another",
				"totalEvents": 1
			},
			{
				"links": {
					"self": "http://0.0.0.0:8080/events/thing.json"
				},
				"name": "thing",
				"totalEvents": 2
			}
		],
		"totalEvents": 3,
		"links": {
			"allEvents": "http://0.0.0.0:8080/events.json",
			"self": "http://0.0.0.0:8080/list-aggregate-types"
		}
	}`
	assertJsonEqual(t, expectedJson, response.Body.String())
}

func TestApi_AggregateTypeStatsProjection(t *testing.T) {
	t.Run("contains projection", func(t *testing.T) {
		// Given
		api, err := rangedbapi.New()
		require.NoError(t, err)

		// When
		aggregateTypeStats := api.AggregateTypeStatsProjection()

		// Then
		assert.IsType(t, &projection.AggregateTypeStats{}, aggregateTypeStats)
	})

	t.Run("logs error from failing snapshot store", func(t *testing.T) {
		// Given
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		api, err := rangedbapi.New(
			rangedbapi.WithSnapshotStore(newFailingSnapshotStore()),
			rangedbapi.WithLogger(logger),
		)
		require.NoError(t, err)

		// When
		stats := api.AggregateTypeStatsProjection()

		// Then
		assert.Equal(t, uint64(0), stats.TotalEvents())
		assert.Equal(t, "unable to load from snapshot store: failingSnapshotStore.Load\nfailingSnapshotStore.Save\n", logBuffer.String())
	})

	t.Run("loads projection from snapshot store", func(t *testing.T) {
		// Given
		aggregateTypeStats := projection.NewAggregateTypeStats()
		aggregateTypeStats.Accept(rangedbtest.DummyRecord())
		inMemorySnapshotStore := newInmemorySnapshotStore()
		require.NoError(t, inMemorySnapshotStore.Save(aggregateTypeStats))
		api, err := rangedbapi.New(
			rangedbapi.WithSnapshotStore(inMemorySnapshotStore),
		)
		require.NoError(t, err)

		// When
		stats := api.AggregateTypeStatsProjection()

		// Then
		assert.Equal(t, uint64(1), stats.TotalEvents())
	})
}

func assertJsonEqual(t *testing.T, expectedJson, actualJson string) {
	t.Helper()
	assert.Equal(t, jsontools.PrettyJSONString(expectedJson), jsontools.PrettyJSONString(actualJson))
}

func saveEvents(t *testing.T, api http.Handler, aggregateType, aggregateID string, requests ...SaveEventRequest) {
	saveJson, err := json.Marshal(requests)
	require.NoError(t, err)

	saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
	saveRequest := httptest.NewRequest(http.MethodPost, saveUri, bytes.NewReader(saveJson))
	saveRequest.Header.Set("Content-Type", "application/json")
	saveResponse := httptest.NewRecorder()

	api.ServeHTTP(saveResponse, saveRequest)
	require.Equal(t, http.StatusCreated, saveResponse.Code)
}

func readGzippedBody(t *testing.T, body io.Reader) string {
	t.Helper()
	bodyReader, err := gzip.NewReader(body)
	require.NoError(t, err)
	actualBody, err := ioutil.ReadAll(bodyReader)
	require.NoError(t, err)
	return string(actualBody)
}

type SaveEventRequest struct {
	EventType string      `msgpack:"t" json:"eventType"`
	Data      interface{} `msgpack:"d" json:"data"`
	Metadata  interface{} `msgpack:"m" json:"metadata"`
}

type failingSnapshotStore struct{}

func newFailingSnapshotStore() *failingSnapshotStore {
	return &failingSnapshotStore{}
}

func (s failingSnapshotStore) Load(_ projection.SnapshotProjection) error {
	return fmt.Errorf("failingSnapshotStore.Load")
}

func (s failingSnapshotStore) Save(_ projection.SnapshotProjection) error {
	return fmt.Errorf("failingSnapshotStore.Save")
}

type inmemorySnapshotStore struct {
	bytes []byte
}

func newInmemorySnapshotStore() *inmemorySnapshotStore {
	return &inmemorySnapshotStore{}
}

func (s *inmemorySnapshotStore) Load(p projection.SnapshotProjection) error {
	return p.LoadFromSnapshot(bytes.NewReader(s.bytes))
}

func (s *inmemorySnapshotStore) Save(p projection.SnapshotProjection) error {
	buff := &bytes.Buffer{}
	err := p.SaveSnapshot(buff)

	s.bytes = buff.Bytes()

	return err
}
