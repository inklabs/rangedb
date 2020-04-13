package rangedbapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/provider/jsonrecordiostream"
	"github.com/inklabs/rangedb/provider/msgpackrecordiostream"
	"github.com/inklabs/rangedb/provider/ndjsonrecordiostream"
)

type api struct {
	jsonRecordIoStream    rangedb.RecordIoStream
	ndJSONRecordIoStream  rangedb.RecordIoStream
	msgpackRecordIoStream rangedb.RecordIoStream
	store                 rangedb.Store
	handler               http.Handler
	baseUri               string
	projections           struct {
		aggregateTypeStats *projection.AggregateTypeStats
	}
}

// Option defines functional option parameters for api.
type Option func(*api)

// WithStore is a functional option to inject a Store.
func WithStore(store rangedb.Store) Option {
	return func(api *api) {
		api.store = store
	}
}

// WithBaseUri is a functional option to inject the base URI for use in API links.
func WithBaseUri(baseUri string) Option {
	return func(api *api) {
		api.baseUri = baseUri
	}
}

// New constructs an api.
func New(options ...Option) *api {
	api := &api{
		jsonRecordIoStream:    jsonrecordiostream.New(),
		ndJSONRecordIoStream:  ndjsonrecordiostream.New(),
		msgpackRecordIoStream: msgpackrecordiostream.New(),
		store:                 inmemorystore.New(),
		baseUri:               "http://127.0.0.1",
	}

	for _, option := range options {
		option(api)
	}

	api.initRoutes()
	api.initProjections()

	return api
}

func (a *api) initRoutes() {
	const stream = "{aggregateType:[a-zA-Z-]+}/{aggregateID:[0-9a-f]{32}}"
	const extension = ".{extension:json|ndjson|msgpack}"
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/health-check", a.healthCheck)
	router.HandleFunc("/save-events/"+stream, a.saveEvents)
	router.HandleFunc("/events"+extension, a.allEvents)
	router.HandleFunc("/events/"+stream+extension, a.eventsByStream)
	router.HandleFunc("/events/{aggregateType:[a-zA-Z-,]+}"+extension, a.eventsByAggregateType)
	router.HandleFunc("/list-aggregate-types", a.listAggregateTypes)
	a.handler = handlers.CompressHandler(router)
}

func (a *api) initProjections() {
	a.projections.aggregateTypeStats = projection.NewAggregateTypeStats()
	a.store.SubscribeAndReplay(a.projections.aggregateTypeStats)
}

func (a *api) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

func (a *api) healthCheck(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set(`Content-Type`, `application/json`)
	_, _ = fmt.Fprintf(w, `{"status":"OK"}`)
}

func (a *api) allEvents(w http.ResponseWriter, r *http.Request) {
	extension := mux.Vars(r)["extension"]

	events := a.store.AllEvents()
	a.writeEvents(w, events, extension)
}

func (a *api) eventsByStream(w http.ResponseWriter, r *http.Request) {
	aggregateType := mux.Vars(r)["aggregateType"]
	aggregateID := mux.Vars(r)["aggregateID"]
	extension := mux.Vars(r)["extension"]

	streamName := rangedb.GetStream(aggregateType, aggregateID)
	events := a.store.AllEventsByStream(streamName)
	a.writeEvents(w, events, extension)
}

func (a *api) eventsByAggregateType(w http.ResponseWriter, r *http.Request) {
	aggregateTypeInput := mux.Vars(r)["aggregateType"]
	aggregateTypes := strings.Split(aggregateTypeInput, ",")
	extension := mux.Vars(r)["extension"]

	var events <-chan *rangedb.Record
	if len(aggregateTypes) > 1 {
		events = a.store.AllEventsByAggregateTypes(aggregateTypes...)
	} else {
		events = a.store.AllEventsByAggregateType(aggregateTypes[0])
	}

	a.writeEvents(w, events, extension)
}

func (a *api) saveEvents(w http.ResponseWriter, r *http.Request) {
	aggregateType := mux.Vars(r)["aggregateType"]
	aggregateID := mux.Vars(r)["aggregateID"]

	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "invalid content type", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	records, errors := a.jsonRecordIoStream.Read(r.Body)

	err := a.saveRecords(aggregateType, aggregateID, records, errors)
	if err != nil {
		if _, ok := err.(*invalidInput); ok {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, `{"status":"Failed"}`)
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, `{"status":"Failed"}`)
		return
	}

	w.WriteHeader(http.StatusCreated)
	_, _ = fmt.Fprintf(w, `{"status":"OK"}`)
}

func (a *api) AggregateTypeStatsProjection() *projection.AggregateTypeStats {
	return a.projections.aggregateTypeStats
}

func (a *api) saveRecords(aggregateType, aggregateID string, records <-chan *rangedb.Record, errors <-chan error) error {
	for {
		select {
		case record, ok := <-records:
			if !ok {
				return nil
			}

			err := a.store.SaveEvent(
				aggregateType,
				aggregateID,
				record.EventType,
				record.EventID,
				record.Data,
				record.Metadata,
			)
			if err != nil {
				return fmt.Errorf("unable to save event: %v", err)
			}

		case err, ok := <-errors:
			if !ok {
				return nil
			}

			return newInvalidInput(err)
		}
	}
}

func (a *api) listAggregateTypes(w http.ResponseWriter, _ *http.Request) {
	var data []map[string]interface{}
	for _, aggregateType := range a.projections.aggregateTypeStats.SortedAggregateTypes() {
		data = append(data, map[string]interface{}{
			"name":        aggregateType,
			"totalEvents": a.projections.aggregateTypeStats.TotalEventsByAggregateType[aggregateType],
			"links": map[string]interface{}{
				"self": fmt.Sprintf("%s/events/%s.json", a.baseUri, aggregateType),
			},
		})
	}

	listResponse := struct {
		Data  interface{}       `json:"data"`
		Links map[string]string `json:"links"`
	}{
		Data: data,
		Links: map[string]string{
			"allEvents": fmt.Sprintf("%s/events.json", a.baseUri),
			"self":      fmt.Sprintf("%s/list-aggregate-types", a.baseUri),
		},
	}

	w.Header().Set(`Content-Type`, `application/json`)
	encoder := json.NewEncoder(w)
	_ = encoder.Encode(listResponse)
}

func (a *api) writeEvents(w http.ResponseWriter, events <-chan *rangedb.Record, extension string) {
	switch extension {
	case "json":
		w.Header().Set(`Content-Type`, `application/json`)
		errors := a.jsonRecordIoStream.Write(w, events)
		<-errors

	case "ndjson":
		w.Header().Set(`Content-Type`, `application/json; boundary=LF`)
		errors := a.ndJSONRecordIoStream.Write(w, events)
		<-errors

	case "msgpack":
		w.Header().Set(`Content-Type`, `application/msgpack`)
		base64Writer := base64.NewEncoder(base64.RawStdEncoding, w)
		errors := a.msgpackRecordIoStream.Write(base64Writer, events)
		<-errors
		_ = base64Writer.Close()

	}
}

type invalidInput struct {
	err error
}

func newInvalidInput(err error) *invalidInput {
	return &invalidInput{err: err}
}

func (i invalidInput) Error() string {
	return fmt.Sprintf("invalid input: %v", i.err)
}
