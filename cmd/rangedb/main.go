package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/pkg/rangedbui"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager/provider/filesystemtemplate"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager/provider/memorytemplate"
	"github.com/inklabs/rangedb/pkg/rangedbws"
	"github.com/inklabs/rangedb/provider/leveldbstore"
)

func main() {
	fmt.Println("RangeDB API")
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	port := flag.Int("port", 8080, "port")
	baseUri := flag.String("baseUri", "http://0.0.0.0:8080", "")
	dbPath := flag.String("dbPath", ".leveldb", "path to LevelDB directory")
	templatesPath := flag.String("templates", "", "optional templates path")
	flag.Parse()

	logger := log.New(os.Stderr, "", 0)
	leveldbStore, err := leveldbstore.New(*dbPath, leveldbstore.WithLogger(logger))
	if err != nil {
		log.Fatalf("Unable to load db (%s): %v", *dbPath, err)
	}

	api := rangedbapi.New(
		rangedbapi.WithStore(leveldbStore),
		rangedbapi.WithBaseUri(*baseUri+"/api"),
	)
	websocketAPI := rangedbws.New(
		rangedbws.WithStore(leveldbStore),
		rangedbws.WithLogger(logger),
	)

	var templateManager templatemanager.TemplateManager
	if *templatesPath != "" {
		if _, err := os.Stat(*templatesPath); os.IsNotExist(err) {
			log.Fatalf("templates path does not exist: %v", err)
		}

		templateManager = filesystemtemplate.New(*templatesPath)
	} else {
		templateManager, err = memorytemplate.New(rangedbui.GetTemplates())
		if err != nil {
			log.Fatalf("unable to load templates: %v", err)
		}
	}

	ui := rangedbui.New(templateManager, api.AggregateTypeStatsProjection(), leveldbStore)

	server := http.NewServeMux()
	server.Handle("/", ui)
	server.Handle("/api/", http.StripPrefix("/api", api))
	server.Handle("/ws/", http.StripPrefix("/ws", websocketAPI))

	fmt.Printf("Listening: http://0.0.0.0:%d/\n", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), server))
}
