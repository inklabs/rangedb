package rangedbapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/pkg/rangedberror"
	"github.com/inklabs/rangedb/pkg/recordsubscriber"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/provider/jsonrecordiostream"
	"github.com/inklabs/rangedb/provider/msgpackrecordiostream"
	"github.com/inklabs/rangedb/provider/ndjsonrecordiostream"
)

const (
	broadcastRecordBuffSize  = 100
	subscriberRecordBuffSize = 20
)

type api struct {
	jsonRecordIoStream    rangedb.RecordIoStream
	ndJSONRecordIoStream  rangedb.RecordIoStream
	msgpackRecordIoStream rangedb.RecordIoStream
	store                 rangedb.Store
	snapshotStore         projection.SnapshotStore
	handler               http.Handler
	logger                *log.Logger
	broadcaster           broadcast.Broadcaster
	baseUri               string
	projections           struct {
		aggregateTypeStats *projection.AggregateTypeStats
	}
}

// Option defines functional option parameters for api.
type Option func(*api)

// WithBaseUri is a functional option to inject the base URI for use in API links.
func WithBaseUri(baseUri string) Option {
	return func(api *api) {
		api.baseUri = baseUri
	}
}

// WithLogger is a functional option to inject a Logger.
func WithLogger(logger *log.Logger) Option {
	return func(api *api) {
		api.logger = logger
	}
}

// WithSnapshotStore is a functional option to inject a SnapshotStore.
func WithSnapshotStore(snapshotStore projection.SnapshotStore) Option {
	return func(api *api) {
		api.snapshotStore = snapshotStore
	}
}

// WithStore is a functional option to inject a Store.
func WithStore(store rangedb.Store) Option {
	return func(api *api) {
		api.store = store
	}
}

// New constructs an api.
func New(options ...Option) (*api, error) {
	api := &api{
		jsonRecordIoStream:    jsonrecordiostream.New(),
		ndJSONRecordIoStream:  ndjsonrecordiostream.New(),
		msgpackRecordIoStream: msgpackrecordiostream.New(),
		store:                 inmemorystore.New(),
		logger:                log.New(ioutil.Discard, "", 0),
		broadcaster:           broadcast.New(broadcastRecordBuffSize, broadcast.DefaultTimeout),
		baseUri:               "http://127.0.0.1",
	}

	for _, option := range options {
		option(api)
	}

	api.initRoutes()
	err := api.initProjections()
	if err != nil {
		return nil, err
	}

	return api, nil
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

func (a *api) initProjections() error {
	a.projections.aggregateTypeStats = projection.NewAggregateTypeStats()
	globalSequenceNumber := uint64(0)

	if a.snapshotStore != nil {
		err := a.snapshotStore.Load(a.projections.aggregateTypeStats)
		if err != nil {
			a.logger.Printf("unable to load from snapshot store: %v", err)
		}

		if a.projections.aggregateTypeStats.TotalEvents() > 0 {
			globalSequenceNumber = a.projections.aggregateTypeStats.LatestGlobalSequenceNumber() + 1
		}
	}

	ctx := context.Background()
	err := a.store.Subscribe(ctx,
		rangedb.RecordSubscriberFunc(a.broadcaster.Accept),
	)
	if err != nil {
		return err
	}

	config := recordsubscriber.AllEventsConfig(ctx,
		a.store,
		a.broadcaster,
		subscriberRecordBuffSize,
		func(record *rangedb.Record) error {
			a.projections.aggregateTypeStats.Accept(record)
			return nil
		},
	)
	subscriber := recordsubscriber.New(config)
	err = subscriber.StartFrom(globalSequenceNumber)
	if err != nil {
		return err
	}

	if a.snapshotStore != nil {
		err := a.snapshotStore.Save(a.projections.aggregateTypeStats)
		if err != nil {
			a.logger.Print(err)
		}
	}

	return nil
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

	recordIterator := a.store.Events(r.Context(), 0)
	a.writeEvents(w, recordIterator, extension)
}

func (a *api) eventsByStream(w http.ResponseWriter, r *http.Request) {
	aggregateType := mux.Vars(r)["aggregateType"]
	aggregateID := mux.Vars(r)["aggregateID"]
	extension := mux.Vars(r)["extension"]

	streamName := rangedb.GetStream(aggregateType, aggregateID)
	events := a.store.EventsByStream(r.Context(), 0, streamName)
	a.writeEvents(w, events, extension)
}

func (a *api) eventsByAggregateType(w http.ResponseWriter, r *http.Request) {
	aggregateTypeInput := mux.Vars(r)["aggregateType"]
	aggregateTypes := strings.Split(aggregateTypeInput, ",")
	extension := mux.Vars(r)["extension"]

	events := a.store.EventsByAggregateTypes(r.Context(), 0, aggregateTypes...)

	a.writeEvents(w, events, extension)
}

func (a *api) saveEvents(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "invalid content type", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	eventRecords, err := getEventRecords(r)
	if err != nil {
		writeBadRequest(w, "invalid json request body")
		return
	}

	var lastStreamSequenceNumber uint64
	var saveErr error
	expectedStreamSequenceNumberInput := r.Header.Get("ExpectedStreamSequenceNumber")
	if expectedStreamSequenceNumberInput != "" {
		expectedStreamSequenceNumber, err := strconv.ParseUint(expectedStreamSequenceNumberInput, 10, 64)
		if err != nil {
			writeBadRequest(w, "invalid ExpectedStreamSequenceNumber")
			return
		}
		lastStreamSequenceNumber, saveErr = a.store.OptimisticSave(r.Context(), expectedStreamSequenceNumber, eventRecords...)
	} else {
		lastStreamSequenceNumber, saveErr = a.store.Save(r.Context(), eventRecords...)
	}

	if saveErr != nil {
		if unexpectedErr, ok := saveErr.(*rangedberror.UnexpectedSequenceNumber); ok {
			writeBadRequest(w, unexpectedErr.Error())
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, `{"status":"Failed"}`)
		return
	}

	w.WriteHeader(http.StatusCreated)
	_, _ = fmt.Fprintf(w, `{"status":"OK","lastStreamSequenceNumber":%d}`, lastStreamSequenceNumber)
}

func writeBadRequest(w http.ResponseWriter, message string) {
	w.WriteHeader(http.StatusBadRequest)
	_, _ = fmt.Fprintf(w, `{"status":"Failed", "message": "%s"}`, message)
}

func getEventRecords(r *http.Request) ([]*rangedb.EventRecord, error) {
	aggregateType := mux.Vars(r)["aggregateType"]
	aggregateID := mux.Vars(r)["aggregateID"]

	var events []struct {
		EventType string      `json:"eventType"`
		Data      interface{} `json:"data"`
		Metadata  interface{} `json:"metadata"`
	}
	err := json.NewDecoder(r.Body).Decode(&events)
	if err != nil {
		return nil, fmt.Errorf("invalid json request body: %v", err)
	}

	var eventRecords []*rangedb.EventRecord
	for _, event := range events {
		eventRecords = append(eventRecords, &rangedb.EventRecord{
			Event:    rangedb.NewRawEvent(aggregateType, aggregateID, event.EventType, event.Data),
			Metadata: event.Metadata,
		})
	}

	return eventRecords, nil
}

func (a *api) AggregateTypeStatsProjection() *projection.AggregateTypeStats {
	return a.projections.aggregateTypeStats
}

func (a *api) listAggregateTypes(w http.ResponseWriter, _ *http.Request) {
	var data []map[string]interface{}
	for _, aggregateType := range a.projections.aggregateTypeStats.SortedAggregateTypes() {
		data = append(data, map[string]interface{}{
			"name":        aggregateType,
			"totalEvents": a.projections.aggregateTypeStats.TotalEventsByAggregateType(aggregateType),
			"links": map[string]interface{}{
				"self": fmt.Sprintf("%s/events/%s.json", a.baseUri, aggregateType),
			},
		})
	}

	listResponse := struct {
		Data        interface{}       `json:"data"`
		TotalEvents uint64            `json:"totalEvents"`
		Links       map[string]string `json:"links"`
	}{
		Data:        data,
		TotalEvents: a.projections.aggregateTypeStats.TotalEvents(),
		Links: map[string]string{
			"allEvents": fmt.Sprintf("%s/events.json", a.baseUri),
			"self":      fmt.Sprintf("%s/list-aggregate-types", a.baseUri),
		},
	}

	w.Header().Set(`Content-Type`, `application/json`)
	encoder := json.NewEncoder(w)
	_ = encoder.Encode(listResponse)
}

func (a *api) writeEvents(w http.ResponseWriter, recordIterator rangedb.RecordIterator, extension string) {
	switch extension {
	case "json":
		w.Header().Set(`Content-Type`, `application/json`)
		errors := a.jsonRecordIoStream.Write(w, recordIterator)
		<-errors

	case "ndjson":
		w.Header().Set(`Content-Type`, `application/json; boundary=LF`)
		errors := a.ndJSONRecordIoStream.Write(w, recordIterator)
		<-errors

	case "msgpack":
		w.Header().Set(`Content-Type`, `application/msgpack`)
		base64Writer := base64.NewEncoder(base64.RawStdEncoding, w)
		errors := a.msgpackRecordIoStream.Write(base64Writer, recordIterator)
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
