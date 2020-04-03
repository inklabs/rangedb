package rangedbui

import (
	"fmt"
	"github.com/inklabs/rangedb/pkg/paging"
	"log"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rakyll/statik/fs"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager"
	_ "github.com/inklabs/rangedb/pkg/rangedbui/statik"
)

//go:generate go run github.com/rakyll/statik -src ./static
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
	statikFS, _ := fs.New()
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", a.index)
	router.HandleFunc("/aggregate-types", a.aggregateTypes)
	router.HandleFunc("/e/"+aggregateType, a.aggregateType)
	router.HandleFunc("/e/"+stream, a.stream)
	router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(statikFS)))

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

	itemsPerPage := r.URL.Query().Get("itemsPerPage")
	page := r.URL.Query().Get("page")
	pagination := paging.NewPaginationFromString(itemsPerPage, page)

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
	StreamName string
	Records    []*rangedb.Record
}

func (a *webUI) stream(w http.ResponseWriter, r *http.Request) {
	aggregateTypeName := mux.Vars(r)["aggregateType"]
	aggregateID := mux.Vars(r)["aggregateID"]

	stream := rangedb.GetStream(aggregateTypeName, aggregateID)
	records := rangedb.RecordChannelToSlice(a.store.AllEventsByStream(stream))

	a.renderWithValues(w, "stream.html", streamTemplateVars{
		StreamName: stream,
		Records:    records,
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
