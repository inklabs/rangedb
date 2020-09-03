package rangedbapi_test

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/provider/msgpackrecordiostream"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestApi_HealthCheck(t *testing.T) {
	// Given
	api := rangedbapi.New()
	request := httptest.NewRequest("GET", "/health-check", nil)

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
			"eventID": "b93bd54592394c999fad7095e2b4840e",
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
		api := rangedbapi.New()
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest("POST", saveUri, strings.NewReader(singleJsonEvent))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusCreated, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"OK"}`, response.Body.String())
	})

	t.Run("fails when content type not set", func(t *testing.T) {
		// Given
		const aggregateID = "2c12be033de7402d9fb28d9b635b3330"
		const aggregateType = "thing"
		api := rangedbapi.New()
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest("POST", saveUri, strings.NewReader(singleJsonEvent))
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "invalid content type\n", response.Body.String())
	})

	t.Run("fails when store save fails", func(t *testing.T) {
		// Given
		const aggregateID = "cbba5f386b2d4924ac34d1b9e9217d67"
		const aggregateType = "thing"
		api := rangedbapi.New(rangedbapi.WithStore(rangedbtest.NewFailingEventStore()))
		expectedJson := `[
		{
			"eventID": "b93bd54592394c999fad7095e2b4840e",
			"eventType": "ThingWasDone",
			"data":{
				"Name": "Thing Test",
				"Timestamp": 1546302589
			},
			"metadata":null
		}
	]`
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest("POST", saveUri, strings.NewReader(expectedJson))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusInternalServerError, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed"}`, response.Body.String())
	})

	t.Run("fails when input json is invalid", func(t *testing.T) {
		// Given
		const aggregateID = "cbba5f386b2d4924ac34d1b9e9217d67"
		const aggregateType = "thing"
		api := rangedbapi.New(rangedbapi.WithStore(rangedbtest.NewFailingEventStore()))
		invalidJson := `x`
		saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
		request := httptest.NewRequest("POST", saveUri, strings.NewReader(invalidJson))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
		assert.Equal(t, `{"status":"Failed"}`, response.Body.String())
	})
}

func TestApi_WithFourEventsSaved(t *testing.T) {
	// Given
	store := inmemorystore.New(inmemorystore.WithClock(sequentialclock.New()))
	api := rangedbapi.New(rangedbapi.WithStore(store))
	const aggregateID1 = "f187760f4d8c4d1c9d9cf17b66766abd"
	const aggregateID2 = "5b36ae984b724685917b69ae47968be1"
	const aggregateID3 = "9bc181144cef4fd19da1f32a17363997"

	saveEvents(t, api, "thing", aggregateID1,
		SaveEventsRequest{
			EventId:   "27e9965ce0ce4b65a38d1e0b7768ba27",
			EventType: "ThingWasDone",
			Data: rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 100,
			},
			Metadata: nil,
		},
		SaveEventsRequest{
			EventId:   "27e9965ce0ce4b65a38d1e0b7768ba27",
			EventType: "ThingWasDone",
			Data: rangedbtest.ThingWasDone{
				ID:     aggregateID1,
				Number: 200,
			},
			Metadata: nil,
		},
	)
	saveEvents(t, api, "thing", aggregateID2,
		SaveEventsRequest{
			EventId:   "ac376375a0834b0bae47b9246ed570c8",
			EventType: "ThingWasDone",
			Data: rangedbtest.ThingWasDone{
				ID:     aggregateID2,
				Number: 300,
			},
			Metadata: nil,
		},
	)
	saveEvents(t, api, "another", aggregateID3,
		SaveEventsRequest{
			EventId:   "d3d25ad1340e42ce89b809ef77ee67c7",
			EventType: "AnotherWasComplete",
			Data: rangedbtest.AnotherWasComplete{
				ID: aggregateID3,
			},
			Metadata: nil,
		},
	)

	t.Run("get all events as json", func(t *testing.T) {
		// Given
		request := httptest.NewRequest("GET", "/events.json", nil)
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
				"globalSequenceNumber": 0,
				"sequenceNumber": 0,
				"insertTimestamp": 0,
				"eventID": "27e9965ce0ce4b65a38d1e0b7768ba27",
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
				"globalSequenceNumber": 1,
				"sequenceNumber": 1,
				"insertTimestamp": 1,
				"eventID": "27e9965ce0ce4b65a38d1e0b7768ba27",
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
				"globalSequenceNumber": 2,
				"sequenceNumber": 0,
				"insertTimestamp": 2,
				"eventID": "ac376375a0834b0bae47b9246ed570c8",
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
				"globalSequenceNumber": 3,
				"sequenceNumber": 0,
				"insertTimestamp": 3,
				"eventID": "d3d25ad1340e42ce89b809ef77ee67c7",
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
		request := httptest.NewRequest("GET", "/events/thing/f187760f4d8c4d1c9d9cf17b66766abd.ndjson", nil)
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
			"sequenceNumber":0,
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
			"sequenceNumber": 1,
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
		request := httptest.NewRequest("GET", "/events/thing/f187760f4d8c4d1c9d9cf17b66766abd.msgpack", nil)
		response := httptest.NewRecorder()

		// When
		api.ServeHTTP(response, request)

		// Then
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, "application/msgpack", response.Header().Get("Content-Type"))
		expectedRecord1 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID1,
			GlobalSequenceNumber: 0,
			StreamSequenceNumber: 0,
			InsertTimestamp:      0,
			EventID:              "27e9965ce0ce4b65a38d1e0b7768ba27",
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
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			InsertTimestamp:      1,
			EventID:              "27e9965ce0ce4b65a38d1e0b7768ba27",
			EventType:            "ThingWasDone",
			Data: map[string]interface{}{
				"id":     aggregateID1,
				"number": "200",
			},
			Metadata: nil,
		}
		ioStream := msgpackrecordiostream.New()
		records, errors := ioStream.Read(base64.NewDecoder(base64.RawStdEncoding, response.Body))
		assert.Equal(t, expectedRecord1, <-records)
		assert.Equal(t, expectedRecord2, <-records)
		assert.Nil(t, <-records)
		require.NoError(t, <-errors)
	})

	t.Run("get events by aggregate type", func(t *testing.T) {
		// Given
		request := httptest.NewRequest("GET", "/events/thing.json", nil)
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
				"globalSequenceNumber":0,
				"sequenceNumber":0,
				"insertTimestamp":0,
				"eventID": "27e9965ce0ce4b65a38d1e0b7768ba27",
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
				"globalSequenceNumber":1,
				"sequenceNumber":1,
				"insertTimestamp":1,
				"eventID": "27e9965ce0ce4b65a38d1e0b7768ba27",
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
				"globalSequenceNumber": 2,
				"sequenceNumber": 0,
				"insertTimestamp": 2,
				"eventID": "ac376375a0834b0bae47b9246ed570c8",
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
		request := httptest.NewRequest("GET", "/events/thing,another.json", nil)
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
				"globalSequenceNumber":0,
				"sequenceNumber":0,
				"insertTimestamp":0,
				"eventID": "27e9965ce0ce4b65a38d1e0b7768ba27",
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
				"globalSequenceNumber":1,
				"sequenceNumber":1,
				"insertTimestamp":1,
				"eventID": "27e9965ce0ce4b65a38d1e0b7768ba27",
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
				"globalSequenceNumber": 2,
				"sequenceNumber": 0,
				"insertTimestamp": 2,
				"eventID": "ac376375a0834b0bae47b9246ed570c8",
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
				"globalSequenceNumber": 3,
				"sequenceNumber": 0,
				"insertTimestamp": 3,
				"eventID": "d3d25ad1340e42ce89b809ef77ee67c7",
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
	require.NoError(t, store.Save(event1, nil))
	require.NoError(t, store.Save(event2, nil))
	require.NoError(t, store.Save(event3, nil))
	api := rangedbapi.New(
		rangedbapi.WithStore(store),
		rangedbapi.WithBaseUri("http://0.0.0.0:8080"),
	)
	request := httptest.NewRequest("GET", "/list-aggregate-types", nil)
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
	// Given
	api := rangedbapi.New()

	// When
	aggregateTypeStats := api.AggregateTypeStatsProjection()

	// Then
	assert.IsType(t, &projection.AggregateTypeStats{}, aggregateTypeStats)
}

func assertJsonEqual(t *testing.T, expectedJson, actualJson string) {
	t.Helper()
	assert.Equal(t, jsontools.PrettyJSONString(expectedJson), jsontools.PrettyJSONString(actualJson))
}

func saveEvents(t *testing.T, api http.Handler, aggregateType, aggregateID string, requests ...SaveEventsRequest) {
	saveJson, err := json.Marshal(requests)
	require.NoError(t, err)

	saveUri := fmt.Sprintf("/save-events/%s/%s", aggregateType, aggregateID)
	saveRequest := httptest.NewRequest("POST", saveUri, bytes.NewReader(saveJson))
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

type SaveEventsRequest struct {
	EventId   string      `msgpack:"e" json:"eventID"`
	EventType string      `msgpack:"t" json:"eventType"`
	Data      interface{} `msgpack:"d" json:"data"`
	Metadata  interface{} `msgpack:"m" json:"metadata"`
}
