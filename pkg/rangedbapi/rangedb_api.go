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
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/provider/jsonrecordiostream"
	"github.com/inklabs/rangedb/provider/msgpackrecordiostream"
	"github.com/inklabs/rangedb/provider/ndjsonrecordiostream"
)

type api struct {
	jsonRecordIoStream    rangedb.RecordIoStream
	ndJsonRecordIoStream  rangedb.RecordIoStream
	msgpackRecordIoStream rangedb.RecordIoStream
	store                 rangedb.Store
	handler               http.Handler
	projections           *Projections
}

type Option func(*api)

func WithStore(store rangedb.Store) Option {
	return func(api *api) {
		api.store = store
	}
}

func New(options ...Option) *api {
	api := &api{
		jsonRecordIoStream:    jsonrecordiostream.New(),
		ndJsonRecordIoStream:  ndjsonrecordiostream.New(),
		msgpackRecordIoStream: msgpackrecordiostream.New(),
		store:                 inmemorystore.New(),
	}

	for _, option := range options {
		option(api)
	}

	api.initRoutes()
	api.initProjections()

	return api
}

func (a *api) initRoutes() {
	const stream = "{aggregateType:[a-zA-Z-]+}/{aggregateId:[0-9a-f]{32}}"
	const extension = ".{extension:json|ndjson|msgpack}"
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/health-check", a.HealthCheck)
	router.HandleFunc("/save-events/"+stream, a.SaveEvents)
	router.HandleFunc("/events"+extension, a.AllEvents)
	router.HandleFunc("/events/"+stream+extension, a.EventsByStream)
	router.HandleFunc("/events/{aggregateType:[a-zA-Z-,]+}"+extension, a.EventsByAggregateType)
	router.HandleFunc("/list-aggregate-types", a.ListAggregateTypes)
	a.handler = handlers.CompressHandler(router)
}

func (a *api) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

func (a *api) HealthCheck(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set(`Content-Type`, `application/json`)
	_, _ = fmt.Fprintf(w, `{"status":"OK"}`)
}

func (a *api) AllEvents(w http.ResponseWriter, r *http.Request) {
	extension := mux.Vars(r)["extension"]

	events := a.store.AllEvents()
	a.writeEvents(w, events, extension)
}

func (a *api) EventsByStream(w http.ResponseWriter, r *http.Request) {
	aggregateType := mux.Vars(r)["aggregateType"]
	aggregateId := mux.Vars(r)["aggregateId"]
	extension := mux.Vars(r)["extension"]

	streamName := rangedb.GetStream(aggregateType, aggregateId)
	events := a.store.EventsByStream(streamName)
	a.writeEvents(w, events, extension)
}

func (a *api) EventsByAggregateType(w http.ResponseWriter, r *http.Request) {
	aggregateTypeInput := mux.Vars(r)["aggregateType"]
	aggregateTypes := strings.Split(aggregateTypeInput, ",")
	extension := mux.Vars(r)["extension"]

	var events <-chan *rangedb.Record
	if len(aggregateTypes) > 1 {
		events = a.store.EventsByAggregateTypes(aggregateTypes...)
	} else {
		events = a.store.EventsByAggregateType(aggregateTypes[0])
	}

	a.writeEvents(w, events, extension)
}

func (a *api) SaveEvents(w http.ResponseWriter, r *http.Request) {
	aggregateType := mux.Vars(r)["aggregateType"]
	aggregateId := mux.Vars(r)["aggregateId"]

	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "invalid content type", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	records, errors := a.jsonRecordIoStream.Read(r.Body)

	err := a.saveRecords(aggregateType, aggregateId, records, errors)
	if err != nil {
		if _, ok := err.(*InvalidInput); ok {
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

func (a *api) saveRecords(aggregateType, aggregateId string, records <-chan *rangedb.Record, errors <-chan error) error {
	for {
		select {
		case record, ok := <-records:
			if !ok {
				return nil
			}

			err := a.store.SaveEvent(
				aggregateType,
				aggregateId,
				record.EventType,
				record.EventId,
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

			return NewInvalidInput(err)
		}
	}
}

func (a *api) ListAggregateTypes(w http.ResponseWriter, _ *http.Request) {
	var data []map[string]interface{}
	for _, aggregateType := range a.projections.aggregateTypeInfo.SortedAggregateTypes() {
		data = append(data, map[string]interface{}{
			"name":        aggregateType,
			"totalEvents": a.projections.aggregateTypeInfo.TotalEventsByAggregateType[aggregateType],
			"links": map[string]interface{}{
				"self": fmt.Sprintf("http://127.0.0.1:8080/events/%s.json", aggregateType),
			},
		})
	}

	listResponse := struct {
		Data  interface{}       `json:"data"`
		Links map[string]string `json:"links"`
	}{
		Data: data,
		Links: map[string]string{
			"self": "http://127.0.0.1:8080/list-aggregate-types",
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
		_ = <-errors

	case "ndjson":
		w.Header().Set(`Content-Type`, `application/json; boundary=LF`)
		errors := a.ndJsonRecordIoStream.Write(w, events)
		_ = <-errors

	case "msgpack":
		w.Header().Set(`Content-Type`, `application/msgpack`)
		base64Writer := base64.NewEncoder(base64.RawStdEncoding, w)
		errors := a.msgpackRecordIoStream.Write(base64Writer, events)
		_ = <-errors
		_ = base64Writer.Close()

	}
}

func (a *api) initProjections() {
	a.projections = &Projections{
		aggregateTypeInfo: NewAggregateTypeInfo(),
	}
	a.store.SubscribeAndReplay(a.projections.aggregateTypeInfo)
}

type InvalidInput struct {
	err error
}

func NewInvalidInput(err error) *InvalidInput {
	return &InvalidInput{err: err}
}

func (i InvalidInput) Error() string {
	return fmt.Sprintf("invalid input: %v", i.err)
}
