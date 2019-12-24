package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/provider/leveldbstore"
)

func main() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	port := flag.Int("port", 8080, "port")
	dbPath := flag.String("dbPath", ".leveldb", "path to LevelDB directory")
	flag.Parse()

	fmt.Println("RangeDB API")
	fmt.Printf("Listening on http://0.0.0.0:%d\n", *port)

	logger := log.New(os.Stderr, "", 0)
	leveldbStore, err := leveldbstore.New(*dbPath, leveldbstore.WithLogger(logger))
	if err != nil {
		log.Fatalf("Unable to load db (%s): %v", *dbPath, err)
	}

	api := rangedbapi.New(rangedbapi.WithStore(leveldbStore))

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), api))
}
