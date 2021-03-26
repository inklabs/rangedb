package rangedbui

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"

	"github.com/inklabs/rangedb/pkg/paging"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/projection"
)

//go:embed static
var StaticAssets embed.FS

//go:embed templates
var Templates embed.FS

type webUI struct {
	handler            http.Handler
	aggregateTypeStats *projection.AggregateTypeStats
	store              rangedb.Store
}

// New constructs a webUI web application.
func New(
	aggregateTypeStats *projection.AggregateTypeStats,
	store rangedb.Store,
) *webUI {
	webUI := &webUI{
		aggregateTypeStats: aggregateTypeStats,
		store:              store,
	}

	webUI.initRoutes()

	return webUI
}

func (a *webUI) initRoutes() {
	const aggregateType = "{aggregateType:[a-zA-Z-]+}"
	const stream = aggregateType + "/{aggregateID:[0-9a-f]{32}}"
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", a.index)
	router.HandleFunc("/aggregate-types", a.aggregateTypes)
	router.HandleFunc("/e/"+aggregateType, a.aggregateType)
	router.HandleFunc("/e/"+stream, a.stream)
	router.PathPrefix("/static/").Handler(http.FileServer(http.FS(StaticAssets)))

	a.handler = handlers.CompressHandler(router)
}

func (a *webUI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

func (a *webUI) index(w http.ResponseWriter, _ *http.Request) {
	a.renderWithValues(w, "index.html", nil)
}

type aggregateTypesTemplateVars struct {
	AggregateTypes []AggregateTypeInfo
	TotalEvents    uint64
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

type aggregateTypeTemplateVars struct {
	AggregateTypeInfo AggregateTypeInfo
	PaginationLinks   paging.Links
	Records           []*rangedb.Record
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
	tmpl, err := template.New(tpl).Funcs(FuncMap).ParseFS(Templates, "templates/layout/*.html", "templates/"+tpl)
	if err != nil {
		return fmt.Errorf("unable to parse template: %w", err)
	}

	return tmpl.Execute(w, data)
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
