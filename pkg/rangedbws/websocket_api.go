package rangedbws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/pkg/recordsubscriber"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

const recordBuffSize = 100

type void struct{}

type websocketAPI struct {
	store           rangedb.Store
	handler         http.Handler
	upgrader        *websocket.Upgrader
	logger          *log.Logger
	bufferedRecords chan *rangedb.Record
	broadcaster     broadcast.Broadcaster
	stopChan        chan void
}

// Option defines functional option parameters for websocketAPI.
type Option func(*websocketAPI)

// WithLogger is a functional option to inject a Logger.
func WithLogger(logger *log.Logger) Option {
	return func(api *websocketAPI) {
		api.logger = logger
	}
}

// WithStore is a functional option to inject a Store.
func WithStore(store rangedb.Store) Option {
	return func(api *websocketAPI) {
		api.store = store
	}
}

// New constructs a websocketAPI.
func New(options ...Option) (*websocketAPI, error) {
	api := &websocketAPI{
		store: inmemorystore.New(),
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		logger:          log.New(ioutil.Discard, "", 0),
		bufferedRecords: make(chan *rangedb.Record, recordBuffSize),
		broadcaster:     broadcast.New(recordBuffSize),
		stopChan:        make(chan void),
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

func (a *websocketAPI) initRoutes() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/events", a.SubscribeToAllEvents)
	router.HandleFunc("/events/{aggregateType:[a-zA-Z-,]+}", a.SubscribeToEventsByAggregateTypes)
	a.handler = router
}

func (a *websocketAPI) initProjections() error {
	ctx := context.Background()
	return a.store.Subscribe(ctx,
		rangedb.RecordSubscriberFunc(a.broadcaster.Accept),
	)
}

func (a *websocketAPI) Stop() error {
	err := a.broadcaster.Close()
	if err != nil {
		return err
	}

	close(a.stopChan)
	return nil
}

func (a *websocketAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

func (a *websocketAPI) SubscribeToAllEvents(w http.ResponseWriter, r *http.Request) {
	globalSequenceNumber, err := globalSequenceNumberFromRequest(r)

	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "unable to upgrade websocket connection", http.StatusBadRequest)
		return
	}
	defer ignoreClose(conn)

	config := recordsubscriber.Config{
		BufLen:   100,
		DoneChan: r.Context().Done(),
		Subscribe: func(subscriber broadcast.RecordSubscriber) {
			a.broadcaster.SubscribeAllEvents(subscriber)
		},
		Unsubscribe: func(subscriber broadcast.RecordSubscriber) {
			a.broadcaster.UnsubscribeAllEvents(subscriber)
			_ = conn.Close()
		},
		GetRecords: func(globalSequenceNumber uint64) rangedb.RecordIterator {
			return a.store.EventsStartingWith(r.Context(), globalSequenceNumber)
		},
		ConsumeRecord: func(record *rangedb.Record) error {
			return a.broadcastRecord(conn, record)
		},
	}
	subscriber := recordsubscriber.New(config)
	err = subscriber.StartFrom(globalSequenceNumber)
	if err != nil {
		return
	}

	_, _, _ = conn.ReadMessage()
}

func globalSequenceNumberFromRequest(r *http.Request) (uint64, error) {
	globalSequenceNumberInput := r.URL.Query().Get("global-sequence-number")
	if globalSequenceNumberInput == "" {
		return 0, nil
	}

	globalSequenceNumber, err := strconv.ParseUint(globalSequenceNumberInput, 10, 64)
	if err != nil {
		return 0, err
	}

	return globalSequenceNumber, nil
}

func (a *websocketAPI) SubscribeToEventsByAggregateTypes(w http.ResponseWriter, r *http.Request) {
	globalSequenceNumber, err := globalSequenceNumberFromRequest(r)

	aggregateTypeInput := mux.Vars(r)["aggregateType"]
	aggregateTypes := strings.Split(aggregateTypeInput, ",")

	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "unable to upgrade websocket connection", http.StatusBadRequest)
		return
	}
	defer ignoreClose(conn)

	config := recordsubscriber.Config{
		BufLen:   100,
		DoneChan: r.Context().Done(),
		Subscribe: func(subscriber broadcast.RecordSubscriber) {
			a.broadcaster.SubscribeAggregateTypes(subscriber, aggregateTypes...)
		},
		Unsubscribe: func(subscriber broadcast.RecordSubscriber) {
			a.broadcaster.UnsubscribeAggregateTypes(subscriber, aggregateTypes...)
			_ = conn.Close()
		},
		GetRecords: func(globalSequenceNumber uint64) rangedb.RecordIterator {
			return a.store.EventsByAggregateTypesStartingWith(r.Context(), globalSequenceNumber, aggregateTypes...)
		},
		ConsumeRecord: func(record *rangedb.Record) error {
			return a.broadcastRecord(conn, record)
		},
	}
	subscriber := recordsubscriber.New(config)
	err = subscriber.StartFrom(globalSequenceNumber)
	if err != nil {
		return
	}

	_, _, _ = conn.ReadMessage()
}

func (a *websocketAPI) broadcastRecord(conn *websocket.Conn, record *rangedb.Record) error {
	jsonEvent, err := json.Marshal(record)
	if err != nil {
		a.logger.Printf("unable to marshal record: %v", err)
		return err
	}

	err = a.sendMessage(conn, jsonEvent)
	if err != nil {
		a.logger.Printf("unable to send record to WebSocket client: %v", err)
		return err
	}

	return nil
}

// MessageWriter is the interface for writing a message to a connection
type MessageWriter interface {
	WriteMessage(messageType int, data []byte) error
}

func (a *websocketAPI) sendMessage(conn MessageWriter, message []byte) error {
	err := conn.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		err := fmt.Errorf("unable to send record to client: %v", err)
		a.logger.Printf("%v", err)
		return err
	}

	return nil
}

func ignoreClose(c io.Closer) {
	_ = c.Close()
}
