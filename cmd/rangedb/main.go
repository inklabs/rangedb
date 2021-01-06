package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"google.golang.org/grpc"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbserver"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/pkg/rangedbui"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager/provider/filesystemtemplate"
	"github.com/inklabs/rangedb/pkg/rangedbui/pkg/templatemanager/provider/memorytemplate"
	"github.com/inklabs/rangedb/pkg/rangedbws"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/provider/leveldbstore"
	"github.com/inklabs/rangedb/provider/postgresstore"
)

const (
	httpTimeout = 10 * time.Second
)

func main() {
	fmt.Println("RangeDB API")
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	port := flag.Int("port", 8080, "port")
	baseURI := flag.String("baseUri", "http://0.0.0.0:8080", "")
	dbPath := flag.String("levelDBPath", "", "path to LevelDB directory")
	templatesPath := flag.String("templates", "", "optional templates path")
	gRPCPort := flag.Int("gRPCPort", 8081, "gRPC port")
	flag.Parse()

	httpAddress := fmt.Sprintf("0.0.0.0:%d", *port)

	logger := log.New(os.Stderr, "", 0)
	store, snapshotName, closeStore, err := getStore(*dbPath, logger)

	api := rangedbapi.New(
		rangedbapi.WithStore(store),
		rangedbapi.WithBaseUri(*baseURI+"/api"),
		rangedbapi.WithSnapshotStore(projection.NewDiskSnapshotStore(snapshotBasePath(snapshotName))),
	)

	websocketAPI, err := rangedbws.New(
		rangedbws.WithStore(store),
		rangedbws.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("unable to create WebSocket API: %v", err)
	}

	rangeDBServer, err := rangedbserver.New(
		rangedbserver.WithStore(store),
	)
	if err != nil {
		log.Fatalf("unable to create RangeDB Server: %v", err)
	}

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

	ui := rangedbui.New(templateManager, api.AggregateTypeStatsProjection(), store)

	muxServer := http.NewServeMux()
	muxServer.Handle("/", ui)
	muxServer.Handle("/api/", http.StripPrefix("/api", api))
	muxServer.Handle("/ws/", http.StripPrefix("/ws", websocketAPI))

	httpServer := &http.Server{
		Addr:         httpAddress,
		ReadTimeout:  httpTimeout + time.Second,
		WriteTimeout: httpTimeout + time.Second,
		Handler:      muxServer,
	}

	gRPCServer := grpc.NewServer()
	rangedbpb.RegisterRangeDBServer(gRPCServer, rangeDBServer)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	go serveGRPC(gRPCServer, *gRPCPort)
	go serveHTTP(httpServer, httpAddress)

	<-stop

	fmt.Println("Shutting down RangeDB gRPC server")
	rangeDBServer.Stop()

	fmt.Println("Shutting down gRPC server")
	gRPCServer.Stop()

	fmt.Println("Shutting down RangeDB WebSocket server")
	websocketAPI.Stop()

	fmt.Println("Shutting down HTTP server")
	err = httpServer.Shutdown(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Shutting down store")
	err = closeStore()
	if err != nil {
		log.Fatal(err)
	}
}

func getStore(levelDBPath string, logger *log.Logger) (rangedb.Store, string, func() error, error) {
	postgreSQLConfig, err := postgreSQLConfigFromEnvironment()
	if err == nil {
		postgresStore, err := postgresstore.New(*postgreSQLConfig)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Using PostgreSQL Store")
		return postgresStore, postgreSQLConfig.DataSourceName(), nilFunc, nil
	}

	if levelDBPath != "" {
		levelDBStore, err := leveldbstore.New(levelDBPath, leveldbstore.WithLogger(logger))
		if err != nil {
			log.Fatalf("Unable to load db (%s): %v", levelDBPath, err)
		}

		fmt.Println("Using LevelDB Store")
		return levelDBStore, levelDBPath, levelDBStore.Stop, nil
	}

	inMemoryStore := inmemorystore.New(inmemorystore.WithLogger(logger))
	fmt.Println("Using In Memory Store")
	return inMemoryStore, shortuuid.New().String(), nilFunc, nil
}

func nilFunc() error {
	return nil
}

func serveHTTP(srv *http.Server, addr string) {
	fmt.Printf("Listening: http://%s/\n", addr)
	err := srv.ListenAndServe()
	if err != nil {
		log.Fatal(err)
	}
}

func serveGRPC(srv *grpc.Server, gRPCPort int) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", gRPCPort))
	if err != nil {
		log.Fatalf("failed to bind to port: %v", err)
	}

	fmt.Printf("gRPC listening: 0.0.0.0:%d\n", gRPCPort)
	err = srv.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
}

func snapshotBasePath(uniqueName string) string {
	snapshotBasePath := fmt.Sprintf("%s%s/shapshots", os.TempDir(), uniqueName)
	err := os.MkdirAll(snapshotBasePath, 0700)
	if err != nil && os.IsNotExist(err) {
		log.Fatalf("unable to create snapshot directory: %v", err)
	}
	return snapshotBasePath
}

func postgreSQLConfigFromEnvironment() (*postgresstore.Config, error) {
	pgHost := os.Getenv("PG_HOST")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDBName := os.Getenv("PG_DBNAME")

	if pgHost+pgUser+pgPassword+pgDBName == "" {
		return nil, fmt.Errorf("postgreSQL DB has not been configured via environment variables to run integration tests")
	}

	return &postgresstore.Config{
		Host:     pgHost,
		Port:     5432,
		User:     pgUser,
		Password: pgPassword,
		DBName:   pgDBName,
	}, nil
}
