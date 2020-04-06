package rangedbui

import (
	"fmt"
	"log"
	"net/http"

	"github.com/inklabs/rangedb/pkg/paging"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager"
)

//go:generate go run github.com/shurcooL/vfsgen/cmd/vfsgendev -source="github.com/inklabs/rangedb/pkg/rangedbui".StaticAssets
//go:generate go run ./gen/pack-templates -path ./templates -package rangedbui

type webUI struct {
	handler            http.Handler
	templateManager    templatemanager.TemplateManager
	aggregateTypeStats *projection.AggregateTypeStats
	store              rangedb.Store
}

func New(
	templateManager templatemanager.TemplateManager,
	aggregateTypeStats *projection.AggregateTypeStats,
	store rangedb.Store,
) *webUI {
	webUI := &webUI{
		aggregateTypeStats: aggregateTypeStats,
		templateManager:    templateManager,
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
	router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(StaticAssets)))

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
}

func (a *webUI) aggregateTypes(w http.ResponseWriter, _ *http.Request) {
	var aggregateTypes []AggregateTypeInfo

	for _, aggregateType := range a.aggregateTypeStats.SortedAggregateTypes() {
		aggregateTypes = append(aggregateTypes, AggregateTypeInfo{
			Name:        aggregateType,
			TotalEvents: a.aggregateTypeStats.TotalEventsByAggregateType[aggregateType],
		})
	}

	a.renderWithValues(w, "aggregate-types.html", aggregateTypesTemplateVars{
		AggregateTypes: aggregateTypes,
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

	records := rangedb.RecordChannelToSlice(a.store.EventsByAggregateType(pagination, aggregateTypeName))

	baseURI := fmt.Sprintf("/e/%s", aggregateTypeName)
	totalRecords := a.aggregateTypeStats.TotalEventsByAggregateType[aggregateTypeName]

	a.renderWithValues(w, "aggregate-type.html", aggregateTypeTemplateVars{
		AggregateTypeInfo: AggregateTypeInfo{
			Name:        aggregateTypeName,
			TotalEvents: a.aggregateTypeStats.TotalEventsByAggregateType[aggregateTypeName],
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
	records := rangedb.RecordChannelToSlice(a.store.EventsByStream(pagination, streamName))

	baseURI := fmt.Sprintf("/e/%s/%s", aggregateTypeName, aggregateID)
	totalRecords := a.store.TotalEventsInStream(streamName)
	aggregateURL := fmt.Sprintf("/e/%s", aggregateTypeName)

	a.renderWithValues(w, "stream.html", streamTemplateVars{
		PaginationLinks: pagination.Links(baseURI, totalRecords),
		Records:         records,
		StreamInfo: StreamInfo{
			Name:        streamName,
			TotalEvents: a.store.TotalEventsInStream(streamName),
		},
		AggregateType:    aggregateTypeName,
		AggregateTypeURL: aggregateURL,
	})
}

func (a *webUI) renderWithValues(w http.ResponseWriter, tpl string, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	err := a.templateManager.RenderTemplate(w, tpl, data)
	if err != nil {
		if err == templatemanager.TemplateNotFound {
			http.Error(w, "404 Not Found", http.StatusNotFound)
			return
		}

		log.Printf("unable to render template %s: %v", tpl, err)
		http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
		return
	}
}

type AggregateTypeInfo struct {
	Name        string
	TotalEvents uint64
}

type StreamInfo struct {
	Name        string
	TotalEvents uint64
}
