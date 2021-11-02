package rangedbui

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/paging"
	"github.com/inklabs/rangedb/pkg/projection"
)

//go:embed static
var StaticAssets embed.FS

//go:embed templates
var Templates embed.FS

const (
	subscriberRecordBuffSize = 20
	defaultHost              = "0.0.0.0:8080"
)

type webUI struct {
	handler            http.Handler
	aggregateTypeStats *projection.AggregateTypeStats
	store              rangedb.Store
	templateFS         fs.FS
	upgrader           *websocket.Upgrader
	host               string
}

// Option defines functional option parameters for webUI.
type Option func(*webUI)

// WithTemplateFS is a functional option to inject a template filesystem
func WithTemplateFS(f fs.FS) Option {
	return func(webUI *webUI) {
		webUI.templateFS = f
	}
}

// WithHost is a functional option to inject a tcp4 host.
func WithHost(host string) Option {
	return func(app *webUI) {
		app.host = host
	}
}

// New constructs a webUI web application.
func New(
	aggregateTypeStats *projection.AggregateTypeStats,
	store rangedb.Store,
	options ...Option,
) *webUI {
	webUI := &webUI{
		aggregateTypeStats: aggregateTypeStats,
		store:              store,
		templateFS:         Templates,
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		host: defaultHost,
	}

	for _, option := range options {
		option(webUI)
	}

	webUI.initRoutes()

	return webUI
}

func (a *webUI) initRoutes() {
	const aggregateType = "{aggregateType:[a-zA-Z-]+}"
	const stream = aggregateType + "/{aggregateID:[0-9a-f]{32}}"
	router := mux.NewRouter().StrictSlash(true)

	main := router.PathPrefix("/").Subrouter()
	main.HandleFunc("/", a.index)
	main.HandleFunc("/aggregate-types", a.aggregateTypes)
	main.HandleFunc("/aggregate-types/live", a.aggregateTypesLive)
	main.HandleFunc("/e/"+aggregateType, a.aggregateType)
	main.HandleFunc("/e/"+aggregateType+"/live", a.aggregateTypeLive)
	main.HandleFunc("/e/"+stream, a.stream)
	main.PathPrefix("/static/").Handler(http.FileServer(http.FS(StaticAssets)))
	main.Use(handlers.CompressHandler)

	websocketRouter := router.PathPrefix("/live").Subrouter()
	websocketRouter.HandleFunc("/e/"+aggregateType, a.realtimeEventsByAggregateType)
	websocketRouter.HandleFunc("/aggregate-types", a.realtimeAggregateTypes)

	a.handler = router
}

func (a *webUI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

func (a *webUI) index(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/aggregate-types", http.StatusFound)
}

type aggregateTypesTemplateVars struct {
	AggregateTypes []AggregateTypeInfo
	TotalEvents    uint64
	UIHost         string
}

func (a *webUI) aggregateTypes(w http.ResponseWriter, _ *http.Request) {
	var aggregateTypes []AggregateTypeInfo

	for _, aggregateType := range a.aggregateTypeStats.SortedAggregateTypes() {
		aggregateTypes = append(aggregateTypes, AggregateTypeInfo{
			Name:        aggregateType,
			TotalEvents: a.aggregateTypeStats.TotalEventsByAggregateType(aggregateType),
		})
	}

	a.renderWithValues(w, "aggregate-types.html", aggregateTypesTemplateVars{
		AggregateTypes: aggregateTypes,
		TotalEvents:    a.aggregateTypeStats.TotalEvents(),
	})
}

func (a *webUI) aggregateTypesLive(w http.ResponseWriter, _ *http.Request) {
	var aggregateTypes []AggregateTypeInfo

	for _, aggregateType := range a.aggregateTypeStats.SortedAggregateTypes() {
		aggregateTypes = append(aggregateTypes, AggregateTypeInfo{
			Name:        aggregateType,
			TotalEvents: a.aggregateTypeStats.TotalEventsByAggregateType(aggregateType),
		})
	}

	a.renderWithValues(w, "aggregate-types-live.html", aggregateTypesTemplateVars{
		AggregateTypes: aggregateTypes,
		TotalEvents:    a.aggregateTypeStats.TotalEvents(),
		UIHost:         a.host,
	})
}

type aggregateTypeTemplateVars struct {
	AggregateTypeInfo AggregateTypeInfo
	PaginationLinks   paging.Links
	Records           []*rangedb.Record
	UIHost            string
}

func (a *webUI) aggregateType(w http.ResponseWriter, r *http.Request) {
	aggregateTypeName := mux.Vars(r)["aggregateType"]
	pagination := paging.NewPaginationFromQuery(r.URL.Query())

	globalSequenceNumber := uint64(pagination.FirstRecordPosition())
	records := rangedb.ReadNRecords(
		uint64(pagination.ItemsPerPage),
		func() (rangedb.RecordIterator, context.CancelFunc) {
			ctx, done := context.WithCancel(r.Context())
			return a.store.EventsByAggregateTypes(ctx, globalSequenceNumber, aggregateTypeName), done
		},
	)

	baseURI := fmt.Sprintf("/e/%s", aggregateTypeName)
	totalRecords := a.aggregateTypeStats.TotalEventsByAggregateType(aggregateTypeName)

	a.renderWithValues(w, "aggregate-type.html", aggregateTypeTemplateVars{
		AggregateTypeInfo: AggregateTypeInfo{
			Name:        aggregateTypeName,
			TotalEvents: totalRecords,
		},
		PaginationLinks: pagination.Links(baseURI, totalRecords),
		Records:         records,
	})
}

type streamTemplateVars struct {
	PaginationLinks  paging.Links
	Records          []*rangedb.Record
	StreamInfo       StreamInfo
	AggregateType    string
	AggregateTypeURL string
}

func (a *webUI) stream(w http.ResponseWriter, r *http.Request) {
	aggregateTypeName := mux.Vars(r)["aggregateType"]
	aggregateID := mux.Vars(r)["aggregateID"]
	pagination := paging.NewPaginationFromQuery(r.URL.Query())

	streamName := rangedb.GetStream(aggregateTypeName, aggregateID)
	streamSequenceNumber := uint64(pagination.FirstRecordPosition())
	records := rangedb.ReadNRecords(
		uint64(pagination.ItemsPerPage),
		func() (rangedb.RecordIterator, context.CancelFunc) {
			ctx, done := context.WithCancel(r.Context())
			return a.store.EventsByStream(ctx, streamSequenceNumber, streamName), done
		},
	)

	baseURI := fmt.Sprintf("/e/%s/%s", aggregateTypeName, aggregateID)
	totalRecords, err := a.store.TotalEventsInStream(r.Context(), streamName)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			http.Error(w, "Request Timeout", http.StatusRequestTimeout)
			return
		}

		http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
		return
	}

	aggregateURL := fmt.Sprintf("/e/%s", aggregateTypeName)

	a.renderWithValues(w, "stream.html", streamTemplateVars{
		PaginationLinks: pagination.Links(baseURI, totalRecords),
		Records:         records,
		StreamInfo: StreamInfo{
			Name:        streamName,
			TotalEvents: totalRecords,
		},
		AggregateType:    aggregateTypeName,
		AggregateTypeURL: aggregateURL,
	})
}

func (a *webUI) aggregateTypeLive(w http.ResponseWriter, r *http.Request) {
	aggregateTypeName := mux.Vars(r)["aggregateType"]

	totalRecords := a.aggregateTypeStats.TotalEventsByAggregateType(aggregateTypeName)

	a.renderWithValues(w, "aggregate-type-live.html", aggregateTypeTemplateVars{
		AggregateTypeInfo: AggregateTypeInfo{
			Name:        aggregateTypeName,
			TotalEvents: totalRecords,
		},
		UIHost: a.host,
	})
}

func (a *webUI) renderWithValues(w http.ResponseWriter, tpl string, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	err := a.RenderTemplate(w, tpl, data)
	if err != nil {
		log.Printf("unable to render template %s: %v", tpl, err)
		http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func (a *webUI) RenderTemplate(w http.ResponseWriter, tpl string, data interface{}) error {
	tmpl, err := template.New(tpl).Funcs(FuncMap).ParseFS(a.templateFS, "templates/layout/*.html", "templates/*"+tpl)
	if err != nil {
		return fmt.Errorf("unable to parse template: %w", err)
	}

	return tmpl.Execute(w, data)
}

type RecordEnvelope struct {
	Record      rangedb.Record
	TotalEvents uint64
}

func (a *webUI) realtimeEventsByAggregateType(w http.ResponseWriter, r *http.Request) {
	aggregateTypeName := mux.Vars(r)["aggregateType"]

	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "unable to upgrade websocket connection", http.StatusBadRequest)
		return
	}
	defer conn.Close()

	keepAlive(conn, 1*time.Minute)

	var startingGlobalSequenceNumber, runningTotalRecords uint64
	totalRecords := a.aggregateTypeStats.TotalEventsByAggregateType(aggregateTypeName)
	if totalRecords > 10 {
		// TODO: optimize: obtain last 10th aggregate type records GSN then rangedb.ReadNRecords using
		//  store.EventsByAggregateTypes from that position.
		startingGlobalSequenceNumber = totalRecords - 10
		runningTotalRecords = startingGlobalSequenceNumber - 1
	}

	ctx, cancel := context.WithCancel(r.Context())

	recordSubscriber := rangedb.RecordSubscriberFunc(func(record *rangedb.Record) {
		runningTotalRecords++

		envelope := RecordEnvelope{
			Record:      *record,
			TotalEvents: runningTotalRecords,
		}

		message, err := json.Marshal(envelope)
		if err != nil {
			log.Printf("unable to marshal record: %v", err)
			return
		}

		writeErr := conn.WriteMessage(websocket.TextMessage, message)
		if writeErr != nil {
			log.Printf("unable to write to ws client: %v", writeErr)
			cancel()
		}
	})

	subscription := a.store.AggregateTypesSubscription(ctx, subscriberRecordBuffSize, recordSubscriber, aggregateTypeName)
	defer subscription.Stop()
	err = subscription.StartFrom(startingGlobalSequenceNumber)
	if err != nil {
		log.Printf("unable to start subscription: %v", err)
		return
	}

	_, _, _ = conn.ReadMessage()
}

func (a *webUI) realtimeAggregateTypes(w http.ResponseWriter, r *http.Request) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "unable to upgrade websocket connection", http.StatusBadRequest)
		return
	}
	defer conn.Close()

	keepAlive(conn, 1*time.Minute)

	latestGlobalSequenceNumber := uint64(0)

	for {
		time.Sleep(100 * time.Millisecond)

		if a.aggregateTypeStats.LatestGlobalSequenceNumber() <= latestGlobalSequenceNumber {
			continue
		}

		latestGlobalSequenceNumber = a.aggregateTypeStats.LatestGlobalSequenceNumber()

		var aggregateTypes []AggregateTypeInfo
		for _, aggregateType := range a.aggregateTypeStats.SortedAggregateTypes() {
			aggregateTypes = append(aggregateTypes, AggregateTypeInfo{
				Name:        aggregateType,
				TotalEvents: a.aggregateTypeStats.TotalEventsByAggregateType(aggregateType),
			})
		}

		envelope := aggregateTypesTemplateVars{
			AggregateTypes: aggregateTypes,
			TotalEvents:    a.aggregateTypeStats.TotalEvents(),
		}

		message, err := json.Marshal(envelope)
		if err != nil {
			log.Printf("unable to marshal record: %v", err)
			return
		}

		writeErr := conn.WriteMessage(websocket.TextMessage, message)
		if writeErr != nil {
			log.Printf("unable to write to ws client: %v", writeErr)
			return
		}
	}
}

func keepAlive(c *websocket.Conn, timeout time.Duration) {
	lastResponse := time.Now()
	c.SetPongHandler(func(_ string) error {
		lastResponse = time.Now()
		return nil
	})

	go func() {
		for {
			err := c.WriteMessage(websocket.PingMessage, []byte("keepalive"))
			if err != nil {
				return
			}
			time.Sleep(timeout / 2)
			if time.Since(lastResponse) > timeout {
				_ = c.Close()
				return
			}
		}
	}()
}

// AggregateTypeInfo contains the aggregate type data available to templates.
type AggregateTypeInfo struct {
	Name        string
	TotalEvents uint64
}

// StreamInfo contains the stream data available to templates.
type StreamInfo struct {
	Name        string
	TotalEvents uint64
}
