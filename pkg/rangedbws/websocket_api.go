package rangedbws

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

type websocketAPI struct {
	store    rangedb.Store
	handler  http.Handler
	upgrader *websocket.Upgrader
	logger   *log.Logger

	sync                     sync.RWMutex
	allEventConnections      map[*websocket.Conn]struct{}
	aggregateTypeConnections map[string]map[*websocket.Conn]struct{}

	broadcastMutex sync.Mutex
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
func New(options ...Option) *websocketAPI {
	api := &websocketAPI{
		store: inmemorystore.New(),
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		logger:                   log.New(ioutil.Discard, "", 0),
		allEventConnections:      make(map[*websocket.Conn]struct{}),
		aggregateTypeConnections: make(map[string]map[*websocket.Conn]struct{}),
	}

	for _, option := range options {
		option(api)
	}

	api.initRoutes()
	api.initProjections()

	return api
}

func (a *websocketAPI) initRoutes() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/events", a.SubscribeToAllEvents)
	router.HandleFunc("/events/{aggregateType:[a-zA-Z-,]+}", a.SubscribeToEventsByAggregateTypes)
	a.handler = router
}

func (a *websocketAPI) initProjections() {
	a.store.Subscribe(NewRecordBroadcaster(a.broadcastRecord))
}

func (a *websocketAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

func (a *websocketAPI) SubscribeToAllEvents(w http.ResponseWriter, r *http.Request) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "unable to upgrade websocket connection", http.StatusBadRequest)
		return
	}
	defer ignoreClose(conn)

	a.writeEventsToConnection(conn, a.store.AllEvents())

	a.subscribeToAllEvents(conn)

	_, _, _ = conn.ReadMessage()
	a.unRegisterFromAllEvents(conn)
}

func (a *websocketAPI) writeEventsToConnection(conn MessageWriter, events <-chan *rangedb.Record) {
	for event := range events {
		jsonEvent, err := json.Marshal(event)
		if err != nil {
			a.logger.Printf("unable to marshal record: %v", err)
			return
		}

		a.sendMessage(conn, jsonEvent)
	}
}

func (a *websocketAPI) SubscribeToEventsByAggregateTypes(w http.ResponseWriter, r *http.Request) {
	aggregateTypeInput := mux.Vars(r)["aggregateType"]
	aggregateTypes := strings.Split(aggregateTypeInput, ",")

	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "unable to upgrade websocket connection", http.StatusBadRequest)
		return
	}
	defer ignoreClose(conn)

	a.writeEventsToConnection(conn, a.store.AllEventsByAggregateTypes(aggregateTypes...))

	a.subscribeToAggregateTypes(conn, aggregateTypes...)

	_, _, _ = conn.ReadMessage()
	a.unRegisterFromAggregateTypes(conn, aggregateTypes...)
}

func (a *websocketAPI) subscribeToAllEvents(conn *websocket.Conn) {
	a.sync.Lock()
	defer a.sync.Unlock()

	a.allEventConnections[conn] = struct{}{}
	a.logger.Printf("client registered (all)\n")
}

func (a *websocketAPI) subscribeToAggregateTypes(conn *websocket.Conn, aggregateTypes ...string) {
	a.sync.Lock()
	defer a.sync.Unlock()

	for _, aggregateType := range aggregateTypes {
		if _, ok := a.aggregateTypeConnections[aggregateType]; !ok {
			a.aggregateTypeConnections[aggregateType] = make(map[*websocket.Conn]struct{})
		}

		a.aggregateTypeConnections[aggregateType][conn] = struct{}{}
	}

	a.logger.Printf("client registered (%v)\n", aggregateTypes)
}

func (a *websocketAPI) broadcastRecord(record *rangedb.Record) {
	a.broadcastMutex.Lock()
	go func() {
		defer a.broadcastMutex.Unlock()

		jsonEvent, err := json.Marshal(record)
		if err != nil {
			a.logger.Printf("unable to marshal record: %v", err)
			return
		}

		a.sync.RLock()
		defer a.sync.RUnlock()

		for connection := range a.allEventConnections {
			a.sendMessage(connection, jsonEvent)
		}

		for aggregateType, connections := range a.aggregateTypeConnections {
			if record.AggregateType != aggregateType {
				continue
			}

			for connection := range connections {
				a.sendMessage(connection, jsonEvent)
			}
		}
	}()
}

func (a *websocketAPI) unRegisterFromAllEvents(conn *websocket.Conn) {
	a.sync.Lock()
	defer a.sync.Unlock()

	delete(a.allEventConnections, conn)
	_ = conn.Close()
	a.logger.Printf("client unregistered (all)")
}

func (a *websocketAPI) unRegisterFromAggregateTypes(conn *websocket.Conn, aggregateTypes ...string) {
	a.sync.Lock()
	defer a.sync.Unlock()

	for _, aggregateType := range aggregateTypes {
		delete(a.aggregateTypeConnections[aggregateType], conn)
	}

	_ = conn.Close()
	a.logger.Printf("client unregistered (%v)\n", aggregateTypes)
}

// MessageWriter is the interface for writing a message to a connection
type MessageWriter interface {
	WriteMessage(messageType int, data []byte) error
}

func (a *websocketAPI) sendMessage(conn MessageWriter, message []byte) {
	if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
		a.logger.Printf("unable to send record to client: %v", err)
	}
}

func ignoreClose(c io.Closer) {
	_ = c.Close()
}
